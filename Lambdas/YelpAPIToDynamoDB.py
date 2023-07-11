from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import boto3
import datetime
import random
import time

from urllib.parse import quote
from decimal import *


API_KEY= 'mmLpySNza7q1ubTTSeE_UXgLySnhV0FFDrD9jQF6_cubLz1bvUYbqTnwXMEmo2icGKL064AqLNwv7Vei-u81OzlGzFPj_YTl7wJacNKmMQIsBtzk7ZkegX94EkyrZHYx'

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  
TOKEN_PATH = '/oauth2/token'
GRANT_TYPE = 'client_credentials'

# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Manhattan'
SEARCH_LIMIT = 50

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

restaurants = {}

def request(host, path, api_key, url_params=None):

    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))
    
    time.sleep(random.uniform(0.5, 1.0))
    
    response = requests.request('GET', url, headers=headers, params=url_params)
    
    return response.json()


def search(api_key, term, page_off):

    url_params = {
        'term': term,
        'location': DEFAULT_LOCATION,
        'offset': page_off,
        'limit': 50,
        'sort_by': 'rating'
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def empty_replace(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input

def addItems(data, cuisine):
    global restaurants
    for info in data:
        if info['id'] in restaurants:
            continue
        table.put_item(
            Item={
                'BusinessID': empty_replace(info['id']),
                'insertedAtTimestamp': empty_replace(str(datetime.datetime.now())),
                'Name': empty_replace(info['name']),
                'Cuisine': empty_replace(cuisine),
                'Rating': empty_replace(Decimal(info['rating'])),
                'Number of Reviews': empty_replace(Decimal(info['review_count'])),
                'Address': empty_replace(info['location']['address1']),
                'Zip Code': empty_replace(info['location']['zip_code']),
                'Latitude': empty_replace(Decimal(str(info['coordinates']['latitude']))),
                'Longitude': empty_replace(Decimal(str(info['coordinates']['longitude'])))
            }
        )

def query_api():

    cuisine = ['italian', 'chinese', 'indian', 'american', 'mexican']
    for c in cuisine:
        page_off = 0
        while page_off < SEARCH_LIMIT:
            # print(c+" restaurants")
            js = search(API_KEY, c+" restaurants", page_off)
            if js.get('businesses'):
                addItems(js['businesses'], c)
            page_off += 50

def lambda_handler(event, context):
    query_api()
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': API_HOST,
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps('Hello from Lambda!')
    }