import json
import pymysql
import boto3

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    dynamodb_client = boto3.client('dynamodb')

    # 이벤트 및 S3 관련 정보 파싱
    detail = event['detail']
    resources = event['resources']
    bucket_name = detail['recording_s3_bucket_name']
    key_prefix = detail['recording_s3_key_prefix']
    channel_arn = resources[0]
    channel_id_part = channel_arn.split(':')[-1]
    channel_id = channel_id_part.split('/')[-1]
    user_id = detail['channel_name']
    key_path = f"{key_prefix}/events/recording-ended.json"
    replay_url = f"https://{bucket_name}.s3.ap-northeast-1.amazonaws.com/{key_prefix}/"

    # S3에서 JSON 파일 읽기
    response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
    recording_data = json.loads(response['Body'].read())
    recording_start = recording_data['recording_started_at']
    recording_end = recording_data['recording_ended_at']

    # DynamoDB에서 스트림 이름 조회
    dynamodb_response = dynamodb_client.get_item(
        TableName='userData',
        Key={'userid': {'S': user_id}}
    )
    stream_name = dynamodb_response['Item']['streamname']['S']

    # RDS MySQL 데이터베이스 연결 및 데이터 삽입
    try:
        conn = pymysql.connect(host='shssk-db.c7kyigm8qng2.ap-northeast-1.rds.amazonaws.com', user='admin', password='VMware1!', db='testdb', connect_timeout=10)
        with conn.cursor() as cur:
            sql = "INSERT INTO replay (userid, channelid, replayurl, recordingstart, recordingend, streamname) VALUES (%s, %s, %s, %s, %s, %s)"
            cur.execute(sql, (user_id, channel_id, replay_url, recording_start, recording_end, stream_name))
            conn.commit()
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL database")
        print(e)
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }