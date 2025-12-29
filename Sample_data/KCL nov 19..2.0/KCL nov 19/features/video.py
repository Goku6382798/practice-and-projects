import os
import uuid
import tempfile
import subprocess
from datetime import datetime

from bson import ObjectId

from ETL.ETL_utils_ import db, s3_client, S3_BUCKET_NAME, AWS_REGION
from dotenv import load_dotenv

load_dotenv()

CDN_BASE_URL = os.getenv("CDN_BASE_URL") or f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com"

VIDEOS_COLL = db["videos_master"] if db is not None else None

VIDEO_ORIGINAL_FOLDER = "originals"
VIDEO_HLS_FOLDER = "hls"
VIDEO_THUMB_FOLDER = "thumbnails"


def _run_cmd(cmd, step_name="unknown"):
    """Helper to run shell cmd (ffmpeg/ffprobe) and raise with context."""
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{step_name} failed: {result.stderr}")
    return result.stdout

def _probe_video(input_path):
    """
    Use ffprobe to get width, height, duration.
    Returns (width, height, duration_sec).
    """
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-show_entries", "format=duration",
        "-of", "json",
        input_path,
    ]
    import json

    out = _run_cmd(cmd, step_name="probe")
    data = json.loads(out)

    width = None
    height = None
    duration = None

    if "streams" in data and data["streams"]:
        st = data["streams"][0]
        width = int(st.get("width") or 0) or None
        height = int(st.get("height") or 0) or None

    if "format" in data and "duration" in data["format"]:
        try:
            duration = float(data["format"]["duration"])
        except Exception:
            duration = None

    return width, height, duration


def _choose_qualities(height: int | None):
    """
    Based on source height, choose qualities array as in your schema.
    """
    if not height:
        return []

    candidates = [1080, 720, 480, 360]
    return [q for q in candidates if q <= height]


def handle_video_upload(file_storage, created_by: str | None = None):
    """
    Main entry: receive FileStorage object, upload original, generate HLS (4s segments),
    upload to S3, store Mongo doc, and return the document.
    """

    if VIDEOS_COLL is None:
        raise RuntimeError("Mongo connection is not initialized (VIDEOS_COLL is None).")

    # 1) Generate stable videoId
    video_id = uuid.uuid4().hex  # e.g., "abc123..." but long; you can change to shortid later

    # 2) Prepare temp directory & save uploaded file locally
    tmp_dir = tempfile.mkdtemp(prefix="kcl_video_")
    original_ext = os.path.splitext(file_storage.filename)[1] or ".mp4"
    local_original_path = os.path.join(tmp_dir, f"{video_id}{original_ext}")

    file_storage.save(local_original_path)

    # 3) Probe video for metadata
    width = height = duration = None
    try:
        width, height, duration = _probe_video(local_original_path)
    except Exception as e:
        # Not fatal for upload, but good to log
        print(f"⚠️ ffprobe failed: {e}")

    # 4) Upload original to S3
    original_s3_key = f"{VIDEO_ORIGINAL_FOLDER}/{video_id}{original_ext}"
    s3_client.upload_file(
        local_original_path,
        S3_BUCKET_NAME,
        original_s3_key,
        ExtraArgs={"ContentType": "video/mp4"}
    )

    original_url = f"{CDN_BASE_URL}/{original_s3_key}"

    # 5) Generate thumbnail (poster) from 2 seconds
    thumbnail_local = os.path.join(tmp_dir, f"{video_id}.jpg")
    try:
        thumb_cmd = [
            "ffmpeg",
            "-y",
            "-i", local_original_path,
            "-ss", "00:00:02",
            "-vframes", "1",
            "-q:v", "2",
            thumbnail_local
        ]
        _run_cmd(thumb_cmd, step_name="thumbnail")
        thumb_s3_key = f"{VIDEO_THUMB_FOLDER}/{video_id}.jpg"
        s3_client.upload_file(
            thumbnail_local,
            S3_BUCKET_NAME,
            thumb_s3_key,
            ExtraArgs={"ContentType": "image/jpeg"}
        )
        poster_url = f"{CDN_BASE_URL}/{thumb_s3_key}"
    except Exception as e:
        print(f"⚠️ thumbnail generation failed: {e}")
        thumb_s3_key = None
        poster_url = None

    # 6) Generate HLS with 4-second segments
    hls_dir = os.path.join(tmp_dir, "hls")
    os.makedirs(hls_dir, exist_ok=True)

    local_master_playlist = os.path.join(hls_dir, "master.m3u8")
    local_segment_pattern = os.path.join(hls_dir, "segment_%03d.ts")

    # Simple single-bitrate HLS (same as source). Later you can add multi-bitrate variants.
    hls_cmd = [
        "ffmpeg",
        "-y",
        "-i", local_original_path,
        "-codec:v", "libx264",
        "-codec:a", "aac",
        "-start_number", "0",
        "-hls_time", "4",        # <--- 4-second chunks
        "-hls_list_size", "0",
        "-hls_segment_filename", local_segment_pattern,
        "-f", "hls",
        local_master_playlist
    ]

    try:
        _run_cmd(hls_cmd, step_name="encode_hls")
    except Exception as e:
        # On failure, create a failed doc and re-raise
        now = datetime.utcnow()
        doc = {
            "videoId": video_id,
            "status": "failed",
            "original": {
                "s3_key": original_s3_key,
                "url": original_url,
                "height": height,
                "width": width,
                "duration_sec": duration
            },
            "playback": {
                "master_url": None,
                "poster_url": poster_url,
                "qualities": _choose_qualities(height)
            },
            "error": {
                "message": str(e),
                "step": "encode"
            },
            "created_by": created_by or "system",
            "created_at": now,
            "updated_at": now
        }
        VIDEOS_COLL.insert_one(doc)
        raise

    # 7) Upload HLS files (master + segments) to S3
    hls_s3_prefix = f"{VIDEO_HLS_FOLDER}/{video_id}/"

    for fname in os.listdir(hls_dir):
        local_path = os.path.join(hls_dir, fname)
        key = f"{hls_s3_prefix}{fname}"

        content_type = "video/MP2T"
        if fname.endswith(".m3u8"):
            content_type = "application/vnd.apple.mpegurl"

        s3_client.upload_file(
            local_path,
            S3_BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": content_type}
        )

    master_s3_key = f"{hls_s3_prefix}master.m3u8"
    master_url = f"{CDN_BASE_URL}/{master_s3_key}"

    # 8) Build Mongo document as per your schema
    now = datetime.utcnow()
    qualities = _choose_qualities(height)

    doc = {
        "_id": ObjectId(),
        "videoId": video_id,         # stable unique key
        "status": "ready",           # queued | processing | ready | failed
        "original": {
            "s3_key": original_s3_key,
            "url": original_url,
            "height": height,
            "width": width,
            "duration_sec": duration
        },
        "playback": {
            "master_url": master_url,
            "poster_url": poster_url,
            "qualities": qualities
        },
        "error": {
            "message": None,
            "step": None
        },
        "created_by": created_by or "system",
        "created_at": now,
        "updated_at": now
    }

    VIDEOS_COLL.insert_one(doc)

    # Optional: clean tmp_dir later with shutil.rmtree(tmp_dir)

    # Return doc (stringify ObjectId)
    doc["id"] = str(doc["_id"])
    del doc["_id"]
    return doc
