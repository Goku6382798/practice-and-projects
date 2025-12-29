# controller/video_controller.py
import uuid
from datetime import datetime
from ETL.ETL_utils_ import db, s3_client, S3_BUCKET_NAME, AWS_REGION

VIDEO_BASE = "video_resolution"
ORIGINALS_PREFIX = f"{VIDEO_BASE}/originals"


def upload_original_video(file_obj, created_by="admin"):
    """
    Upload original MP4 to S3 and insert queued record in videos_master.
    Returns: videoId
    """

    # 1) generate unique videoId
    video_id = uuid.uuid4().hex  # ex: "a1b2c3..."

    # 2) decide s3 key
    s3_key = f"{ORIGINALS_PREFIX}/{video_id}.mp4"

    # 3) upload to S3
    content_type = getattr(file_obj, "mimetype", None) or "video/mp4"

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=file_obj.read(),
        ContentType=content_type
    )

    # (optional) public/original url for admin/debug
    original_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"

    # 4) insert Mongo doc (queued)
    videos_coll = db["videos_master"]

    videos_coll.insert_one({
        "videoId": video_id,
        "status": "queued",
        "original": {
            "s3_key": s3_key,
            "url": original_url,
            "height": None,
            "width": None,
            "duration_sec": None
        },
        "playback": {
            "master_url": None,
            "poster_url": None,
            "qualities": []
        },
        "error": {"message": None, "step": None},
        "created_by": created_by,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    })

    return video_id
