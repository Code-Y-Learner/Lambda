def lambda_handler(event, context):
    print('event: ')
    print(event)
    
    status_code = 200
    body = {'messgae': 'Job 생성이 완료되었습니다.'}
    
    # Source ---------------------------------------------------
    
    # input
    source_s3_bucket = event['Records'][0]['s3']['bucket']['name']
    # vod/dev/mp4/1/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574.mp4
    source_s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') 
    # s3://input/vod/dev/mp4/1/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574.mp4
    source_s3 = 's3://' + source_s3_bucket + '/' + source_s3_key
    # Anime-84574    
    source_s3_base_name = os.path.splitext(os.path.basename(source_s3))[0] # Anime-84574
    
    # Destination ---------------------------------------------------
    # s3://output
    destination_s3 = 's3://' + os.environ['DestinationBucket'] 
    # output
    destination_s3_base_name = os.path.splitext(os.path.basename(destination_s3))[0] 
    # arn:aws:iam::123456:role/service-role/mediaconvert_mp4_to_hls_role
    media_convert_role = os.environ['MediaConvertRole'] 
    # ap-northeast-2 (서울)
    region = os.environ['AWS_DEFAULT_REGION'] 
    
    
    # Job 생성 ---------------------------------------------------
    
    
    job_metadata = {}
    
    splitted = source_s3_key.split('/')
    if len(splitted) == 6:
        base_folder = splitted[0] # vod
        server_type = splitted[1] # dev (qa, prod etc)
        file_type   = splitted[2] # mp4
        member_seq  = splitted[3] # 1
        uuid        = splitted[4] # 174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4
        file_name   = splitted[5] # Anime-84574.mp4
        
        job_metadata['assetID'] = uuid
        job_metadata['baseFolder'] = base_folder
        job_metadata['serverType'] = server_type
        job_metadata['fileType'] = file_type
        job_metadata['memberSeq'] = member_seq
        job_metadata['fileName'] = file_name
        
        # vod/dev/hls/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574
        job_metadata['outputPath'] = job_metadata['baseFolder'] + '/' + job_metadata['serverType'] + '/' + 'hls/' + job_metadata['assetID'] + '/' + source_s3_base_name 
        job_metadata['outputExtension'] = '.m3u8'                                     
        
    else:
        status_code = 400
        body['message'] = '지정된 규칙과 일치하지 않습니다. \n vod/서버종류/파일종류/맴버시퀀스/UUID/파일이름 \n ex)vod/dev/mp4/1/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574.mp4 '
        
        return {
            'status_code': status_code,
            'headers': {'Content-Type':'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': body
        }

    print('job_metadata: ')
    print(job_metadata)        
    


    try:
        with open('job.json') as file:
            job_settings = json.load(file)
            
        
        mc_client = boto3.client('mediaconvert', region_name=region)
        endpoints = mc_client.describe_endpoints()    
        
        client = boto3.client('mediaconvert', 
                              region_name=region, 
                              endpoint_url=endpoints['Endpoints'][0]['Url'], # https://bnklbqvoa.mediaconvert.ap-northeast-2.amazonaws.com
                              verify=False)
        
        # change input file name
        # s3://input/vod/dev/mp4/1/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574.mp4
        job_settings['Inputs'][0]['FileInput'] = source_s3 
        
        # vod/dev/hls/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574
        s3_key_hls = job_metadata['outputPath']
        # s3:out/vod/dev/hls/174e7217-bd3e-4aa2-a72b-a24b4eb6e3e4/Anime-84574             
        destination_s3_final = destination_s3 + '/' + s3_key_hls 
        
        # change output path
        job_settings['OutputGroups'][0]['OutputGroupSettings']['HlsGroupSettings']['Destination'] = destination_s3_final 
        
        job = client.create_job(Role=media_convert_role,
                            UserMetadata=job_metadata,
                            Settings=job_settings)
    
    
    except Exception as error:
        print('Exception: ')
        print(error)
    
        status_code = 500
        body['message'] = 'MediaConvert Job 생성중 에러발생'
        raise
        

    finally:
        return {
            'status_code': status_code,
            'headers': {'Content-Type':'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': body
        }

    return {
        'status_code': status_code,
        'headers': {'Content-Type':'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': body
    } 