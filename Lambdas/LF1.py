import boto3

sqs = boto3.resource('sqs')

def lambda_handler(event, context):
   return sendToSQS(event)

    
def close(session_attributes, fulfillment_state, message, intent_name):
    response = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close',
            },
            'intent': {
                'name': intent_name,
                'state': fulfillment_state
            },
            'messages': [
                message
            ]
        }
    }
    
    return response

def sendToSQS(event):
    client = boto3.client("sqs")

    # Initialize the state we'll return
    my_session_state = event['sessionState']
    my_session_state['dialogAction'] = {'type': 'Close'}
    
    session_attributes = event.get('sessionAttributes') if event.get(
        'sessionAttributes') is not None else {}

    # Get and validate the slots
    intent = event['sessionState']['intent']
    slots = intent['slots']

    if "CuisineType" not in slots or not slots["CuisineType"] or not slots["CuisineType"]["value"]["interpretedValue"]:
        return None
    if "NumberOfPeople" not in slots or not slots["NumberOfPeople"] or not slots["NumberOfPeople"]["value"]["interpretedValue"]:
        return None
    if "Email" not in slots or not slots["Email"] or not slots["Email"]["value"]["interpretedValue"]:
        return None
    if "Time" not in slots or not slots["Time"] or not slots["Time"]["value"]["interpretedValue"]:
        return None
    if "Location" not in slots or not slots["Location"] or not slots["Location"]["value"]["interpretedValue"]:
        return None

    # Publish message to SQS
    sqs_response = client.send_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/546066487972/requests-queue",
        MessageBody="abc12",
        MessageAttributes={
            'CuisineType': {
                'StringValue': slots["CuisineType"]["value"]["interpretedValue"],
                'DataType': 'String'
            },
            'Location': {
                'StringValue': slots["Location"]["value"]["interpretedValue"],
                'DataType': 'String'
            },
            'Email': {
                'StringValue': slots["Email"]["value"]["interpretedValue"],
                'DataType': 'String'
            },
            'NumberOfPeople': {
                'StringValue': slots["NumberOfPeople"]["value"]["interpretedValue"],
                'DataType': 'String'
            },
            'Time': {
                'StringValue': slots["Time"]["value"]["interpretedValue"],
                'DataType': 'String'
            },
        }
    )

    return close(
            session_attributes,
            "Fulfilled",
            {
                'contentType': 'PlainText',
                'content': "Thanks, you're all set! You should receive my suggestions via SMS in a few minutes!"
            },
            event['sessionState']['intent']['name'],
        )
