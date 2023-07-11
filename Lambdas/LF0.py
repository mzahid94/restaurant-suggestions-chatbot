import boto3
import json

client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    print(event)
    body = json.loads(event['body'])
    msg_from_user = body['messages'][0]
    print(f"Message from frontend: {msg_from_user}")
    botMessage = "Please try again."
    if msg_from_user is None or len(msg_from_user) < 1:
        return {
            'statusCode': 200,
            'body': json.dumps(botMessage)
        }
    response = client.recognize_text(
        botId='S1LV2FFSUD',
        botAliasId='W4KDRINP2S',
        localeId='en_US',
        sessionId='testuser',
        text=msg_from_user["unstructured"]["text"]
    )

    if "messages" in response:
        messages = response['messages']
    else:
        default_message = "What can I help you with?"
        return {'statusCode': 200, 'body': default_message}

    if messages:
        print([message['content'] for message in messages])
        resp = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'messages': [{
                    "type": "unstructured",
                    "unstructured": {
                        "text": json.dumps([message['content'] for message in messages])
                    }
                }]
            })
        }
        return resp



"""
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

lex = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    # Extract the user input text from the input event
    user_input = event['messages'][0]['unstructured']['text']
    # logger.debug(event)
    
    # Invoke the Lex v2 bot with the user input
    bot_id = ''
    bot_alias_id = ''
    locale_id = 'en_US'

    response = lex.recognize_text(
        botId='S1LV2FFSUD',
        botAliasId='W4KDRINP2S',
        localeId=locale_id,
        sessionId='abc1234',
        text=user_input
    )

    # Return the bot response as the output of the Lambda function
    return response
  """     