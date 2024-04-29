import json
import boto3
import urllib.parse
import time
import os
import pymysql
from urllib.parse import quote

def lambda_handler(event, context):
    print('event: ')
    print(event)
    
    # 기본 응답 설정
    status_code = 200
    body = {'message': 'Job 생성이 완료되었습니다.'}
    
    # Source 정보 추출
    source_s3_bucket = event['Records'][0]['s3']['bucket']['name']
    # 파일 이름에서 버킷 이름을 제외하고 순수 경로만 추출
    source_s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8').replace(f'{source_s3_bucket}/', '')
    source_s3 = f's3://{source_s3_bucket}/{source_s3_key}'
    
    # Destination 정보 설정
    destination_s3 = 's3://' + os.environ['DestinationBucket']
    region = os.environ['AWS_DEFAULT_REGION']
    
    # Job 메타데이터 생성
    job_metadata = {
        'sourceS3Bucket': source_s3_bucket,
        'sourceS3Key': source_s3_key
    }
    
    try:
        # MediaConvert 작업 설정 파일 로드
        with open('job.json') as file:
            job_settings = json.load(file)
        
        # MediaConvert 클라이언트 생성
        mc_client = boto3.client('mediaconvert', region_name=region)
        endpoints = mc_client.describe_endpoints()
        client = boto3.client('mediaconvert', 
                              region_name=region, 
                              endpoint_url=endpoints['Endpoints'][0]['Url'], 
                              verify=False)
        
        # 입력 파일 경로 업데이트
        job_settings['Inputs'][0]['FileInput'] = source_s3
        
        # 출력 경로 업데이트
        path_part = source_s3_key.split('raw_vod/')[1]
        directory_path = os.path.splitext(path_part)[0]
        output_path = f'output/{directory_path}'
        destination_s3_final = f"{destination_s3}/{output_path}" 
        job_settings['OutputGroups'][0]['OutputGroupSettings']['HlsGroupSettings']['Destination'] = destination_s3_final
        
        # MediaConvert 작업 생성
        job_response = client.create_job(Role=os.environ['MediaConvertRole'],
                                UserMetadata=job_metadata,
                                Settings=job_settings)
                                
         # 작업 기다리기
        job_id = job_response['Job']['Id']
        
        # Wait for the job to complete
        while True:
            # Check the status of the job
            job_status = client.get_job(Id=job_id)['Job']['Status']
            
            # If the job is in a terminal state, break out of the loop
            if job_status in ['COMPLETE', 'ERROR']:
                break
            
            # Wait for 5 seconds before checking again
            time.sleep(5)
        
        # outfile 메타데이터 받아오기
        output_metadata = client.get_job(Id=job_id)['Job']['OutputGroupDetails'][0]['OutputDetails'][0]
    
        # 길이랑 사이즈 가져오기 가져오기
        output_duration = output_metadata.get('DurationInMs')
        #output_file_size = output_metadata.get('SizeInBytes')
        
    
        
    except Exception as error:
        print('Exception: ')
        print(error)
        status_code = 500
        body['message'] = 'MediaConvert Job 생성 중 에러 발생'
        raise
    
    user_id = directory_path.split("/")[0]
    recording_start = directory_path.split("/")[1]
    stream_name = directory_path.split("/")[2]
    encoding = quote(stream_name)
    replay_url = f'https://sesac4-vod.s3.ap-northeast-1.amazonaws.com/output/{user_id}/{recording_start}/{encoding}/'
    
    try:
        conn = pymysql.connect(host='shssk-db.c7kyigm8qng2.ap-northeast-1.rds.amazonaws.com', user='admin', password='VMware1!', db='testdb', connect_timeout=10)
        with conn.cursor() as cur:
            sql = "INSERT INTO replay (userid, replayurl, recordingstart, streamname, duration) VALUES (%s, %s, FROM_UNIXTIME(%s / 1000), %s, %s)"
            cur.execute(sql, (user_id, replay_url, recording_start, stream_name, output_duration))
            conn.commit()
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL database")
        print(e)
        raise e    

    finally:
        return {
            'statusCode': status_code,
            'headers': {'Content-Type':'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': body
        }
    
    
    
