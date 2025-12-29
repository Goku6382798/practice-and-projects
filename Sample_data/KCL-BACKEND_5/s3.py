import os
import boto3
from dotenv import load_dotenv

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

BASE = "video_resolution/"


def make_placeholder(path: str):
    """ Upload a small placeholder file to create a folder """
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=path,
        Body=b"",        # empty byte file
        ContentType="text/plain"
    )
    print(f"✔ created: s3://{S3_BUCKET_NAME}/{path}")


def create_base_structure():
    """ Create top-level folders """
    folders = [
        "video_resolution/originals/.keep",
        "video_resolution/hls/.keep",
        "video_resolution/thumbnails/.keep"
    ]
    for f in folders:
        make_placeholder(f)


def create_sample_video_structure(video_id="vid_001"):
    """ Create demo HLS folder structure with placeholder files """

    base_path = f"video_resolution/hls/{video_id}/"

    # master playlist
    make_placeholder(base_path + "master.m3u8")

    quality_levels = ["v1080", "v720", "v480", "v360"]
    for q in quality_levels:
        folder = f"{base_path}{q}/"
        # create folder
        make_placeholder(folder + ".keep")
        # create placeholder segment files
        make_placeholder(folder + "stream.m3u8")
        make_placeholder(folder + "seg_00000.ts")

    # sample thumbnail
    make_placeholder(f"video_resolution/thumbnails/{video_id}.jpg")


if __name__ == "__main__":
    print("🚀 Creating video resolution folder structure in S3...")
    create_base_structure()
    create_sample_video_structure()
    print("🎉 Done! Check your S3 console.")
