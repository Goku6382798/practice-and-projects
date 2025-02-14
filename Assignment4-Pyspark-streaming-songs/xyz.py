import os
import boto3
from botocore.exceptions import NoCredentialsError

def download_s3_folder(bucket_name, s3_folder, local_dir):
    """
    Downloads an S3 folder to a local directory.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_folder (str): The path to the folder in the S3 bucket.
        local_dir (str): The local directory to save the files.

    """
    # Create the S3 client
    s3 = boto3.client('s3')

    # Ensure the local directory exists
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # List objects in the specified S3 folder
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder):
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_file_path = obj['Key']
                    # Remove the prefix folder path to get the relative path
                    relative_path = os.path.relpath(s3_file_path, s3_folder)
                    local_file_path = os.path.join(local_dir, relative_path)

                    # Ensure the local directory structure matches the S3 folder
                    local_file_dir = os.path.dirname(local_file_path)
                    if not os.path.exists(local_file_dir):
                        os.makedirs(local_file_dir)

                    # Download the file
                    print(f"Downloading {s3_file_path} to {local_file_path}")
                    s3.download_file(bucket_name, s3_file_path, local_file_path)

        print("Download complete.")

    except NoCredentialsError:
        print("Credentials not available. Ensure your AWS credentials are configured.")
    except Exception as e:
        print(f"Error: {e}")

# Usage
bucket_name = "your-bucket-name"
s3_folder = "your-folder-path/"  # Folder path in S3
local_dir = "/your/local/directory"  # Local directory to download to

download_s3_folder(bucket_name, s3_folder, local_dir)
