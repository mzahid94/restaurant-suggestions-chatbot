import json
import os
import random
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from json.decoder import JSONDecodeError
import requests
from requests.auth import HTTPBasicAuth
import urllib3
import logging

# Disable insecure request warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = 'us-east-1'
CLUSTER_HOST = 'search-restaurants-es-p7hxdvt2e6jyasb2i3uoglc2xe.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/546066487972/requests-queue'
SENDER_EMAIL = 'mz3327@nyu.edu'

def query_opensearch(cuisine):
    query = {
        "query": {
            "term": {
                "Cuisine.keyword": cuisine.lower()
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    url = "https://search-restaurants-es-p7hxdvt2e6jyasb2i3uoglc2xe.us-east-1.es.amazonaws.com/restaurants/_search"
    auth = HTTPBasicAuth('<usrname>', 'pass')
    try:
        response = requests.get(url, headers=headers, json=query, auth=auth)
        response.raise_for_status()  # Raise an exception for non-2xx response codes
        response_json = response.json()
        logger.info('Response from Elasticsearch: %s', json.dumps(response_json))
        return response_json
    except Exception as e:
        logger.error(f"Error occurred while querying Elasticsearch: {e}")
        raise


   
def get_restaurant_data(restaurantsToEmail):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    recommendations = []
    
    for BusinessID in restaurantsToEmail:
        dynamodb_response = table.query(KeyConditionExpression=Key('BusinessID').eq(BusinessID))
        recommendations.append({
            "name": dynamodb_response['Items'][0]["Name"],
            "address": dynamodb_response['Items'][0]["Address"]
        })
    
    return recommendations



def get_random_restaurants(cuisine):
    try:
        response = query_opensearch(cuisine)
        resp = response["hits"]["hits"]

        restaurants = []
        for hit in resp:
            source = hit["_source"]
            business_id = source.get("BusinessID")
            if business_id:
                restaurants.append(business_id)
            if len(restaurants) == 3:
                break
        return restaurants
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []


def send_email(recipient, body_text):
    ses_client = boto3.client('ses', region_name='us-east-1')
    try:
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': "UTF-8",
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': "UTF-8",
                    'Data': "Your Restaurant Suggestions!",
                },
            },
            Source=SENDER_EMAIL,
        )
        return True
    except ClientError as e:
        logger.error(f"An error occurred while sending email: {e.response['Error']['Message']}")
        return False

def lambda_handler(event, context):
    logger.info('Received event: %s', json.dumps(event))
    
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    try:
        sqs_response = sqs_client.receive_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/546066487972/requests-queue",
            AttributeNames=['SentTimestamp'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )
        logger.info('Received SQS response: %s', json.dumps(sqs_response))
        logger.info('Received SQS response Messages: %s', json.dumps(sqs_response.get('Messages')))
        
        if sqs_response.get('Messages'):
            message = sqs_response['Messages'][0]
            attributes = message.get('MessageAttributes', {})
            cuisine = attributes.get('CuisineType', {}).get('StringValue')
            email = attributes.get('Email', {}).get('StringValue')
            numPeople = attributes.get('NumberOfPeople', {}).get('StringValue')
            time = attributes.get('Time', {}).get('StringValue')
            location = attributes.get('Location', {}).get('StringValue')
            
            if cuisine and email:
                restaurantIDs = get_random_restaurants(cuisine)
                logger.info('Received restaurant IDs')
                if restaurantIDs:
                    restaurants = get_restaurant_data(restaurantIDs)
                    
                    body_text = f"Hi! These are my {cuisine} restaurant suggestions for {numPeople} people at {time} in {location}:\n"
                    for restaurant in restaurants:
                        body_text += f"Restaurant Name: {restaurant['name']}\n"
                        body_text += f"Restaurant Address: {restaurant['address']}\n\n"

                    success = send_email(email, body_text)
                    
                    if success:
                        sqs_client.delete_message(
                            QueueUrl=QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        return {
                            'statusCode': 200,
                            'body': json.dumps({'message': 'Email sent successfully.'})
                        }
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'No pending requests or email not sent.'})
    }

