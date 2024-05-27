import os

import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging


class S3Uploader:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name='us-east-1'):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
    def upload_file_to_s3(self,file_name, bucket, object_name=None):
        """
        Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified, file_name is used
        :return: True if file was uploaded, else False
        """
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        try:
         #Step 2 get all the buckets, check if the bucket
            #get the bucket
            response = self.s3_client.list_buckets()
            bucket_found = False
            for bucketx in response['Buckets']:
                if bucketx['Name'] == bucket:
                    print(f'  {bucketx["Name"]}')
                    bucket_found = True


            #Step 3 Create bucket if it doesn't Exist
            if not bucket_found:
                self.s3_client.create_bucket(Bucket=bucket)

            # Step 4 Upload the file with the bucket
            response = self.s3_client.upload_file(file_name,bucket, object_name)
            #upload the file
            #s3_client = session.resource('s3').Bucket(bucket).upload_file(file_name, object_name)
            print(response)
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

        return True

    def create_bucket(self, bucket):
        """
               Upload a file to an S3 bucket

               :param file_name: File to upload
               :param bucket: Bucket to upload to
               :param object_name: S3 object name. If not specified, file_name is used
               :return: True if file was uploaded, else False
               """

        try:
            # Step 2 get all the buckets, check if the bucket
            # get the bucket
            response = self.s3_client.list_buckets()
            bucket_found = False
            for bucketx in response['Buckets']:
                if bucketx['Name'] == bucket:
                    print(f'  {bucketx["Name"]}')
                    bucket_found = True

            # Step 3 Create bucket if it doesn't Exist
            if not bucket_found:
                self.s3_client.create_bucket(Bucket=bucket)

            print(response)
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
        except Exception as e:
            print(f"An error occurred: {e}{bucket}")
            return False

        return True

    def delete_bucket(self, bucket):
        """
               Upload a file to an S3 bucket

               :param file_name: File to upload
               :param bucket: Bucket to upload to
               :param object_name: S3 object name. If not specified, file_name is used
               :return: True if file was uploaded, else False
               """

        try:
            # Step 2 get all the buckets, check if the bucket
            # get the bucket
            response = self.s3_client.list_buckets()
            bucket_found = False
            for bucketx in response['Buckets']:
                if bucketx['Name'] == bucket:
                    print(f'  {bucketx["Name"]}')
                    bucket_found = True

            # Step 3 Create bucket if it doesn't Exist
            if  bucket_found:
                self.s3_client.delete_bucket(Bucket=bucket)

            print(response)
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

        return True

    def download_file(self, bucket, file_name, download_path):
        try:
            self.s3_client.download_file(bucket, file_name, download_path)
            print(f"File {file_name} downloaded to {download_path}")
        except FileNotFoundError:
            print("The file was not found on the local filesystem")
        except NoCredentialsError:
            print("Credentials not available")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(f"The object {file_name} does not exist in bucket {bucket}.")
            else:
                print(f"An error occurred: {e}")
    def list_files_in_bucket(self, bucket):
        """
        List all files in an S3 bucket

        :param bucket: Bucket to list files from
        :return: List of file names
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket)
            if 'Contents' in response:
                files = [item['Key'] for item in response['Contents']]
                return files
            else:
                print("Bucket is empty or does not exist")
                return []
        except NoCredentialsError:
            print("Credentials not available")
            return []
        except Exception as e:
            print(f"An error occurred: {e}")
            return []


# # Setup logging
logging.basicConfig(filename='s3_uploader.log', level=logging.ERROR)

# Usage
aws_access_key_id = 'key'
aws_secret_access_key = 'key'
uploader = S3Uploader(aws_access_key_id, aws_secret_access_key)
files_= uploader.list_files_in_bucket('jamacia-stockexchange-trading-data')
for file in files_:
    print(file)
    download_path = 'sownload_path'
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    uploader.download_file('jamacia-stockexchange-trading-data', file, download_path)

print(files_)
