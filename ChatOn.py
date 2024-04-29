import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone, timedelta

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('userData')

    resources = event['resources']
    
    channel_arn = resources[0]
    channel_id_part = channel_arn.split(':')[-1]
    channel_id = channel_id_part.split('/')[-1]
    
    # chat-logging-arn
    log_arn = "arn:aws:ivschat:ap-northeast-1:891377305172:logging-configuration/7sfKBmIi8UTf"
    
    # Create chat-room
    client = boto3.client('ivschat', region_name='ap-northeast-1')

    # Define the input parameters
    input = {
        'name': channel_id,
        'loggingConfigurationIdentifiers' : [log_arn],
    }
    
    # Create the room
    response_chat = client.create_room(**input)
    
    chatroom_url = response_chat['arn']

    # index_value를 사용하여 글로벌 보조 인덱스를 통해 항목을 찾음
    response = table.query(
        IndexName='channelid-index',
        KeyConditionExpression=Key('channelid').eq(channel_id)
    )

    #찾은 항목의 isstream 값을 1로 업데이트하고 thumbnailurl 값을 업데이트
    for item in response['Items']:
        table.update_item(
			Key={'userid': item['userid']
			},  # YourPrimaryKey를 실제 테이블의 기본 키 이름으로 바꿔주세요.
            UpdateExpression='SET chaturl = :val1, streamstarttime = :val2',
            ExpressionAttributeValues={
                ':val1': chatroom_url,
                ':val2': event['time']
            }
        )
    return (response)
