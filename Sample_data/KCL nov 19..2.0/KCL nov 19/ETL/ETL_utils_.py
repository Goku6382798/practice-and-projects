# mongo connection

import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

def mongo_connection():
    try:
        # Use the full MongoDB URI from .env
        client = MongoClient(os.getenv("MONGODB_URI"))

        # Select the database from env
        db = client[os.getenv("MONGODB_DATABASE")]

        print("✅ Successfully connected to MongoDB")
        return db
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return None

# Initialize connection when module is imported
db = mongo_connection()



# postgre connection

import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

def postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        )
        print("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        print("Error connecting to PostgreSQL: {e}")
        return None

# Initialize connection when module is imported
conn = postgres_connection()




# bucket connection

import os
import boto3
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Create S3 client
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

BASE_FOLDER = "dashboardImagesAll1/New folder"
PROFILE_IMAGE_FOLDER = "Users_images"
def list_s3_images(subfolder: str):
    """
    List all images in a given subfolder under 'New folder' and return public URLs.
    """
    prefix = f"{BASE_FOLDER}/{subfolder}"
    urls = []

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)

        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                if key.lower().endswith((".png", ".jpg", ".jpeg")):
                    encoded_key = quote_plus(key, safe="/")  # spaces → '+'
                    public_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{encoded_key}"
                    urls.append(public_url)

        return sorted(urls)

    except Exception as e:
        print(f"Error listing S3 images with prefix {prefix}: {e}")
        return []


def upload_to_s3(file_data, filename, content_type="image/jpeg"):
    """
    Upload a file (binary data) to AWS S3 and return its public URL.

    Args:
        file_data (bytes): File content (read in binary mode)
        filename (str): Desired filename (e.g., "myphoto.jpg")
        content_type (str): MIME type, default = "image/jpeg"
        folder_path (str): Optional subfolder path in the S3 bucket

    Returns:
        str: Public URL of uploaded file or None if failed.
    """
    try:
        s3_key = f"{PROFILE_IMAGE_FOLDER}/{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=file_data,
            ContentType=content_type
        )

        file_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        print(f"✅ Uploaded to S3: {file_url}")
        return file_url

    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")
        return None

# ------------------------------
# Refresh-token collection + indexes
# ------------------------------
from pymongo.errors import PyMongoError

refresh_tokens_coll = db["refresh_token_data"] if db is not None else None

def ensure_refresh_indexes():
    """
    Try to ensure indexes on refresh_token_data.
    If they already exist, ignore duplicate/conflict errors.
    """
    if refresh_tokens_coll is None:
        print("⚠️ Skipping index creation: no Mongo connection.")
        return
    try:
        refresh_tokens_coll.create_index([("jti", 1)], unique=True)
        refresh_tokens_coll.create_index([("userid", 1)])
        refresh_tokens_coll.create_index([("session_id", 1)])
        refresh_tokens_coll.create_index([("expires_at", 1)], expireAfterSeconds=0)
        print("✅ refresh_token_data indexes ensured (no conflict).")
    except PyMongoError as e:
        # Ignore “index already exists / options conflict” quietly
        msg = str(e)
        if "IndexOptionsConflict" in msg or "already exists" in msg:
            print("ℹ️ refresh_token_data indexes already exist — skipping creation.")
        else:
            print(f"❌ Failed ensuring refresh_token_data indexes: {e}")
