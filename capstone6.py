import requests
import zipfile
import json
import boto3
import os
from dotenv import load_dotenv
from pathlib import Path
from botocore.exceptions import BotoCoreError, ClientError

# Load the environment variables
load_dotenv()

# Assign the env variables
url = os.getenv('url')
zip_destination_dir = Path(os.getenv('zip_destination_dir'))
unzip_destination_dir = Path(os.getenv('unzip_destination_dir'))

s3_bucket_name = os.getenv('s3_bucket_name')
rds_connection_string = os.getenv('rds_connection_string')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
aws_region = os.getenv('region')
dynamodb_table_name = os.getenv('dynamodb_table_name')
num_files_to_extract = int(os.getenv('num_files_to_extract'))

# Ensure environment variables are set
if not all([url, zip_destination_dir, s3_bucket_name, rds_connection_string, AWS_ACCESS_KEY, AWS_SECRET_KEY, aws_region, dynamodb_table_name]):
    print("Error: Missing required environment variables.")
    exit(1)

# Headers for HTTP requests
headers = {
    'User-Agent': 'ajithstark1@gmail.com',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}

# Assign AWS Clients
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=aws_region)
dynamodb_client = boto3.client('dynamodb', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=aws_region)

# Function to download the file if it doesn't already exist
def download_file(url, destination):
    # Check if file already exists
    if os.path.exists(destination):
        print(f"File already exists at {destination}. Skipping download.")
        return
    
    try:
        print(f"Attempting to download from {url}")
        with requests.get(url, headers=headers, stream=True) as response:
            response.raise_for_status()  # Raise an error for unsuccessful requests
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"File downloaded successfully to {destination}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")

# Function to unzip the file
def unzip_file(zip_destination, unzip_destination, num_files):
    try:
        with zipfile.ZipFile(zip_destination, 'r') as zip_ref:
            print(f"Unzipping {zip_destination} to {unzip_destination}")
            extracted_files = 0
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.json') and extracted_files < num_files:
                    zip_ref.extract(file_info, unzip_destination)
                    print(f"Extracted: {file_info.filename}")
                    extracted_files += 1
    except zipfile.BadZipFile as e:
        print(f"Error unzipping file: {e}")

# Function to upload files to S3 Bucket
def s3_file_upload(source_dir, s3_bucket):
    for local_file in source_dir.rglob('*'):
        if local_file.is_file():
            s3_key = local_file.relative_to(source_dir)
            try:
                print(f"Uploading {local_file} to s3://{s3_bucket}/{s3_key}")
                s3_client.upload_file(str(local_file), s3_bucket, str(s3_key))
                print(f"Uploaded {local_file} to S3")
            except (BotoCoreError, ClientError) as e:
                print(f"Error uploading {local_file} to S3: {e}")

# Function to upload data to DynamoDB
def upload_to_dynamodb(s3_client, dynamodb_client, s3_bucket, dynamodb_table):
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket)
        for obj in response.get('Contents', []):
            object_key = obj['Key']
            s3_response = s3_client.get_object(Bucket=s3_bucket, Key=object_key)
            json_content = s3_response['Body'].read().decode('utf-8')
            print(f"Processing JSON content for {object_key}")

            try:
                data = json.loads(json_content)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for {object_key}: {e}")
                continue

            # Prepare the attribute map for DynamoDB
            attribute_map = {
                key: {'N' if isinstance(value, (int, float)) else 'S': str(value)}
                for key, value in data.items()
            }

            try:
                dynamodb_client.put_item(TableName=dynamodb_table, Item=attribute_map)
                print(f"Uploaded data from {object_key} to DynamoDB")
            except Exception as e:
                print(f"Error uploading data to DynamoDB for {object_key}: {e}")
                continue
    except (BotoCoreError, ClientError) as e:
        print(f"Error accessing S3 bucket {s3_bucket}: {e}")


# Main function to execute the steps
def main():
    # Remove the existing zip file if it exists
#    if zip_destination_dir.exists() and zip_destination_dir.is_file():
#        print(f"Removing existing file at {zip_destination_dir}")
#        zip_destination_dir.unlink()

    # Download, unzip, and upload the file
    download_file(url, zip_destination_dir)
    unzip_file(zip_destination_dir, unzip_destination_dir, num_files_to_extract)
    s3_file_upload(unzip_destination_dir, s3_bucket_name)
    upload_to_dynamodb(s3_client, dynamodb_client, s3_bucket_name, dynamodb_table_name)

if __name__ == "__main__":
    main()
