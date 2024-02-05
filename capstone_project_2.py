import requests
import zipfile
import json
import pandas as pd
from sqlalchemy import create_engine
import boto3
import os

def download_file(url, destination):
    try: 
        with requests.get(url,headers=headers, stream=True) as response:
            response.raise_for_status()
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"File downloaded successfully to {destination}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")

# URL pointing to the zip file
url = 'https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip'

headers = {'user-agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 Edg/120.0.0.0',
          'Accept-Encoding': 'gzip, deflate, br',
          'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'}

# Destination folders
zip_destination_dir = "D:/Ajith Kumar/submissions.zip"
unzip_destination_dir = "D:/Ajith Kumar/data"
s3_bucket_name = "ajith-capstone-2"
rds_connection_string = "your-rds-connection-string"

# Step 1: Download the zip file
download_file(url, zip_destination_dir)

# Step 2: Extract data from the zip file

unzip_destination_dir = "D:/Ajith Kumar/s3test"
num_files_to_extract = 10
with zipfile.ZipFile(zip_destination_dir, 'r') as zip_ref:
    for file_info in zip_ref.infolist():
        if file_info.filename.endswith('.json') and num_files_to_extract > 0:
            zip_ref.extract(file_info.filename, unzip_destination_dir)
            num_files_to_extract -= 1

# step 3: Upload data to S3
source_dir='D:/Ajith Kumar/s3test'

def s3_file_upload(source_dir):
        aws_access_key_id = 'O4B2E5'
        aws_secret_access_key = 'fjTtJ782M'
        aws_region = 'ap-south-1'
        s3_bucket_name = 'ajith-capstone-2'
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_key = os.path.relpath(local_file_path, source_dir)  

                # Upload the file to S3
                s3_client.upload_file(local_file_path, s3_bucket_name, s3_key)

                print(f"File uploaded to S3: s3://{s3_bucket_name}/{s3_key}")

s3_file_upload(source_dir)

# upload data to dynamodb

def upload_to_dynamodb():
    aws_access_key_id = 'O4B2E5'
    aws_secret_access_key = 'TtJ782M'
    aws_region = 'ap-south-1'
    s3_bucket_name = 'ajith-capstone-2'
    dynamodb_table_name = 'capstone_project_2'

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    dynamodb_client = boto3.client('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

    response = s3_client.list_objects_v2(Bucket=s3_bucket_name)
    for obj in response.get('Contents', []):
        object_key = obj['Key']
        s3_response = s3_client.get_object(Bucket=s3_bucket_name, Key=object_key)
        json_content = s3_response['Body'].read().decode('utf-8')
        print(f"JSON content for {object_key}:")
        print(json_content)

        try:
            data = json.loads(json_content)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for {object_key}: {e}")
            continue

        attribute_map = {}
        for key, value in data.items():
            attribute_map[key] = {'S': str(value) if isinstance(value, (str, int, float)) else json.dumps(value)}

        try:
            dynamodb_client.put_item(TableName=dynamodb_table_name, Item=attribute_map)
            print(f"Uploaded data from {object_key} to DynamoDB")
        except Exception as e:
            print(f"Error uploading data to DynamoDB for {object_key}: {e}")
            continue

    print("Project Success")

upload_to_dynamodb()