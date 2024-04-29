import json
import boto3
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('userData')

    # 이벤트 상세 정보를 파싱
    detail = event['detail']
    resources = event['resources']

    # S3 버킷 이름과 키 프리픽스 추출, arn
    bucket_name = detail['recording_s3_bucket_name']
    key_prefix = detail['recording_s3_key_prefix']
    channel_arn = resources[0]
    channel_id_part = channel_arn.split(':')[-1]
    channel_id = channel_id_part.split('/')[-1]



    # 썸네일 URL 생성
    thumbnail_url = f"https://{bucket_name}.s3.ap-northeast-1.amazonaws.com/{key_prefix}/media/latest_thumbnail/thumb.jpg"

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
            UpdateExpression='SET isstream = :val1, thumbnailurl = :val2, category = :val3',
            ExpressionAttributeValues={
                ':val1': int(1),
                ':val2': thumbnail_url,
                ':val3': "",
            }
        )
    return (response)

