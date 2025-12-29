import os
import shutil
import tempfile
import uuid
from datetime import datetime
import subprocess

import requests
import ffmpeg  # ffmpeg-python

from ETL.ETL_utils_ import db, s3_client, S3_BUCKET_NAME, AWS_REGION
from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG ----------------
VIDEO_BASE = "video_resolution"
ORIGINALS_PREFIX = f"{VIDEO_BASE}/originals"
HLS_PREFIX = f"{VIDEO_BASE}/hls"
THUMBS_PREFIX = f"{VIDEO_BASE}/thumbnails"

CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN", "").strip()

# ladder heights you support
LADDER = [1080, 720, 480, 360]

# Approx bitrates for each ladder rung (safe defaults)
BITRATES = {
    1080: "5000k",
    720:  "2800k",
    480:  "1400k",
    360:  "800k",
}
MAXRATES = {
    1080: "5350k",
    720:  "3000k",
    480:  "1500k",
    360:  "900k",
}
BUFSIZES = {
    1080: "7500k",
    720:  "4200k",
    480:  "2100k",
    360:  "1200k",
}

HLS_TIME = 4  # seconds per segment
# ----------------------------------------


def update_status(video_id, status, playback=None, original_meta=None):
    payload = {
        "status": status,
        "updated_at": datetime.utcnow()
    }

    if playback:
        payload["playback"] = playback
    if original_meta:
        payload["original"] = original_meta

    db["videos_master"].update_one({"videoId": video_id}, {"$set": payload})


def download_file(url, out_path):
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def probe_resolution(local_path):
    """
    Returns (width, height, duration_sec)
    """
    meta = ffmpeg.probe(local_path)
    vstream = next(s for s in meta["streams"] if s["codec_type"] == "video")

    width = int(vstream.get("width"))
    height = int(vstream.get("height"))
    duration = float(meta["format"].get("duration", 0))

    return width, height, int(duration)


def allowed_qualities(original_height):
    """
    Only include qualities <= original height
    """
    return [h for h in LADDER if h <= original_height]


def run_ffmpeg_hls(input_mp4, out_dir, height):
    """
    Generates HLS for one quality into out_dir
    Produces:
      out_dir/stream.m3u8
      out_dir/seg_00000.ts ...
    """
    os.makedirs(out_dir, exist_ok=True)
    seg_pattern = os.path.join(out_dir, "seg_%05d.ts")
    playlist_path = os.path.join(out_dir, "stream.m3u8")

    (
        ffmpeg
        .input(input_mp4)
        .filter("scale", f"trunc(oh*a/2)*2", height)  # keep aspect ratio, even width
        .output(
            playlist_path,
            format="hls",
            hls_time=HLS_TIME,
            hls_playlist_type="vod",
            hls_segment_filename=seg_pattern,
            vcodec="libx264",
            acodec="aac",
            video_bitrate=BITRATES[height],
            maxrate=MAXRATES[height],
            bufsize=BUFSIZES[height],
            preset="veryfast",
            g=HLS_TIME * 25,        # GOP ~= segment length (assuming 25fps avg)
            sc_threshold=0
        )
        .overwrite_output()
        .run(quiet=True)
    )


def make_master_playlist(video_dir, qualities):
    """
    Create master.m3u8 that references variant playlists.
    """
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3"
    ]

    # bandwidth approximate (bps) from bitrate k
    def bw_k_to_bps(k_str):
        return int(float(k_str.replace("k", "")) * 1000)

    for q in qualities:
        bw = bw_k_to_bps(BITRATES[q])
        lines.append(
            f'#EXT-X-STREAM-INF:BANDWIDTH={bw},RESOLUTION=0x{q}'
        )
        lines.append(f"v{q}/stream.m3u8")

    master_path = os.path.join(video_dir, "master.m3u8")
    with open(master_path, "w") as f:
        f.write("\n".join(lines))

    return master_path


def upload_folder_to_s3(local_root, s3_root):
    """
    Upload all files in local_root to s3_root preserving relative paths.
    """
    for root, _, files in os.walk(local_root):
        for name in files:
            local_path = os.path.join(root, name)
            rel_path = os.path.relpath(local_path, local_root)
            s3_key = f"{s3_root}/{rel_path}".replace("\\", "/")

            # content types
            if name.endswith(".m3u8"):
                ctype = "application/vnd.apple.mpegurl"
            elif name.endswith(".ts"):
                ctype = "video/MP2T"
            elif name.endswith(".jpg") or name.endswith(".jpeg"):
                ctype = "image/jpeg"
            else:
                ctype = "application/octet-stream"

            with open(local_path, "rb") as f:
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=f,
                    ContentType=ctype
                )


def build_public_url(s3_key):
    # CloudFront preferred if set
    if CLOUDFRONT_DOMAIN:
        return f"{CLOUDFRONT_DOMAIN.rstrip('/')}/{s3_key}"
    return f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


def process_one_video(doc):
    video_id = doc["videoId"]
    mp4_url = doc.get("original", {}).get("url")

    if not mp4_url:
        update_status(video_id, "failed", "No original.url found", "input")
        return

    update_status(video_id, "processing")

    temp_dir = tempfile.mkdtemp(prefix=f"hls_{video_id}_")
    try:
        local_mp4 = os.path.join(temp_dir, f"{video_id}.mp4")

        # 1) download original MP4
        download_file(mp4_url, local_mp4)

        # 2) probe resolution
        width, height, duration_sec = probe_resolution(local_mp4)

        # 3) decide qualities
        qualities = allowed_qualities(height)
        if not qualities:
            qualities = [min(LADDER)]  # safe fallback

        # 4) generate per-quality HLS
        video_out_dir = os.path.join(temp_dir, "hls_out")
        os.makedirs(video_out_dir, exist_ok=True)

        for q in qualities:
            run_ffmpeg_hls(
                input_mp4=local_mp4,
                out_dir=os.path.join(video_out_dir, f"v{q}"),
                height=q
            )

        # 5) create master playlist
        make_master_playlist(video_out_dir, qualities)

        # 6) upload to S3
        s3_video_root = f"{HLS_PREFIX}/{video_id}"
        upload_folder_to_s3(video_out_dir, s3_video_root)

        # (thumbnail generation later if you want; for now keep poster_url None)
        master_key = f"{s3_video_root}/master.m3u8"
        master_url = build_public_url(master_key)

        # 7) update Mongo ready
        update_status(
            video_id,
            "ready",
            playback={
                "master_url": master_url,
                "poster_url": doc.get("playback", {}).get("poster_url"),
                "qualities": qualities
            },
            original_meta={
                **doc.get("original", {}),
                "height": height,
                "width": width,
                "duration_sec": duration_sec
            }
        )

        print(f"✅ Processed videoId={video_id} qualities={qualities}")

    except Exception as e:
        update_status(video_id, "failed")
        print(f"❌ Failed videoId={video_id}: {e}")


    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    queued = list(db["videos_master"].find({"status": "queued"}))
    if not queued:
        print("ℹ️ No queued videos found.")
        return

    print(f"🚀 Found {len(queued)} queued videos. Starting HLS generation...")

    for doc in queued:
        process_one_video(doc)

    print("🎉 All queued videos processed.")


if __name__ == "__main__":
    main()
