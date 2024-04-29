import json
import boto3
import time

SearchLabel = 'smoke'


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    image = event['Records'][0]['s3']['object']['key']
    timestamp = int(time.time())
    
    s3 = boto3.client('s3')
    rekognition_client = boto3.client('rekognition')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('userData')
    
    print(bucket)
    print(image)
    
    # smoke 90% 이상이면 iscensor = 1로 변경
    # censorlist에 타임스탬프 추가
    
    try:
        channel_id = image.split('/')[3]
        response_rekognition = rekognition_client.detect_labels(
            Image={
                'S3Object':{
                    'Bucket':bucket,
                    'Name':image
                }
            },
            MinConfidence = 90
        )
        print(response_rekognition)

        ddb_response = table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('channelid').eq(channel_id)
            )
        if ddb_response['Items']:
            userid = ddb_response['Items'][0]['userid']

        detect_labels=[]
        if response_rekognition['Labels']:
            for label in response_rekognition['Labels']:
                detect_labels.append(label['Name'].lower())
            print(detect_labels)
            if SearchLabel in detect_labels:
                result = table.update_item(
                    Key={
                        'userid': userid
                    },
                    # censorlist update
                    UpdateExpression='SET iscensor = :iscensor, censorlist = list_append(censorlist, :censorlist)',
                    ExpressionAttributeValues={
                        ':iscensor': 1,
                        ':censorlist': [timestamp]
                    }
                    # censorlist reset
                    # UpdateExpression='SET iscensor = :iscensor, censorlist = :censorlist',
                    # ExpressionAttributeValues={
                    #     ':iscensor': 0,
                    #     ':censorlist': []
                    # }
                )

                s3.copy({'Bucket':bucket,'Key':image},'rekognition-sesac-test',f'{channel_id}/{timestamp}.jpg')
                return '있음. 복사완료'
            else:
                return '없음'
    except Exception as error:
        print(error)
        return '에러'
