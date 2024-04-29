import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone, timedelta

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('userData')

    # 이벤트 상세 정보를 파싱
    detail = event['detail']
    resources = event['resources']

    # arn 추출
    channel_arn = resources[0]
    channel_id_part = channel_arn.split(':')[-1]
    channel_id = channel_id_part.split('/')[-1]

    # index_value를 사용하여 글로벌 보조 인덱스를 통해 항목을 찾음
    response = table.query(
        IndexName='channelid-index',
        KeyConditionExpression=Key('channelid').eq(channel_id)
    )

    # 찾은 항목의 isstream 값을 0, 스트림 종료시간 업데이트 
    for item in response['Items']:
        table.update_item(
            Key={'userid': item['userid']},
            UpdateExpression='SET isstream = :val1, streamendtime = :val2',
            ExpressionAttributeValues={
                ':val1': int(0),
                ':val2': event['time']
            }
        )

    ## chat room 삭제
    client = boto3.client('ivschat', region_name='ap-northeast-1')

    # 가져온 chaturl
    chaturl = response['Items'][0]['chaturl']
    
    input = {
        'identifier' : chaturl,
    }
    
    response_chat = client.delete_room(**input)
    return {}