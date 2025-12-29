# ETL/register_existing_videos.py
import uuid
from datetime import datetime
from ETL.ETL_utils_ import db

VIDEOS_COLL = "videos_master"

EPISODES_COLL = "episodes_master_copy"
TRAINING_COLL = "training_videos_master_copy"
CONTEST_COLL = "contest_info"   # based on your screenshots

VIDEO_BASE = "video_resolution"
CLOUDFRONT_DOMAIN = ""  # optional now; can keep empty, will update later


def get_or_create_video_id(mp4_url: str):
    """
    If mp4_url already registered in videos_master -> return existing videoId.
    Else create new videos_master doc -> return new videoId.
    """
    videos = db[VIDEOS_COLL]

    existing = videos.find_one({"original.url": mp4_url}, {"videoId": 1})
    if existing:
        return existing["videoId"], False

    video_id = uuid.uuid4().hex

    doc = {
        "videoId": video_id,
        "status": "queued",
        "original": {
            "s3_key": None,          # optional now
            "url": mp4_url,          # IMPORTANT: store original MP4 url for mapping
            "height": None,
            "width": None,
            "duration_sec": None
        },
        "playback": {
            "master_url": None,      # filled after FFmpeg HLS
            "poster_url": None,
            "qualities": []
        },
        "error": {"message": None, "step": None},
        "created_by": "migration_script",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }

    videos.insert_one(doc)
    return video_id, True


def migrate_episodes():
    episodes = db[EPISODES_COLL]
    count_new = 0
    count_updated = 0

    for ep in episodes.find({}, {"_id": 1, "video_url": 1, "videoId": 1}):
        mp4_url = ep.get("video_url")
        if not mp4_url:
            continue

        # if already has videoId, skip
        if ep.get("videoId"):
            continue

        video_id, created = get_or_create_video_id(mp4_url)
        if created:
            count_new += 1

        episodes.update_one(
            {"_id": ep["_id"]},
            {"$set": {"videoId": video_id}}
        )
        count_updated += 1

    print(f"[episodes] new videos registered: {count_new}, docs updated with videoId: {count_updated}")


def migrate_training():
    training = db[TRAINING_COLL]
    count_new = 0
    count_updated = 0

    for tr in training.find({}, {"_id": 1, "video_url": 1, "videoId": 1}):
        mp4_url = tr.get("video_url")
        if not mp4_url:
            continue

        if tr.get("videoId"):
            continue

        video_id, created = get_or_create_video_id(mp4_url)
        if created:
            count_new += 1

        training.update_one(
            {"_id": tr["_id"]},
            {"$set": {"videoId": video_id}}
        )
        count_updated += 1

    print(f"[training] new videos registered: {count_new}, docs updated with videoId: {count_updated}")


def migrate_contests():
    contests = db[CONTEST_COLL]
    count_new = 0
    count_updated = 0

    for c in contests.find({}, {"_id": 1, "bannersList": 1, "howToParticipate": 1}):
        update_needed = False
        banners = c.get("bannersList", []) or []

        # --- bannersList videos ---
        for b in banners:
            if b.get("type") == "video":
                mp4_url = b.get("source")
                if not mp4_url:
                    continue

                if not b.get("videoId"):
                    video_id, created = get_or_create_video_id(mp4_url)
                    if created:
                        count_new += 1
                    b["videoId"] = video_id
                    update_needed = True

        # --- howToParticipate video ---
        htp = c.get("howToParticipate") or {}
        htp_mp4 = htp.get("video")
        if htp_mp4 and not htp.get("videoId"):
            video_id, created = get_or_create_video_id(htp_mp4)
            if created:
                count_new += 1
            htp["videoId"] = video_id
            update_needed = True

        if update_needed:
            contests.update_one(
                {"_id": c["_id"]},
                {"$set": {
                    "bannersList": banners,
                    "howToParticipate": htp
                }}
            )
            count_updated += 1

    print(f"[contest_info] new videos registered: {count_new}, docs updated with videoId: {count_updated}")


def main():
    print("🚀 Starting one-time videoId migration...")

    migrate_episodes()
    migrate_training()
    migrate_contests()

    print("✅ Migration complete.")
    print("Next: run FFmpeg HLS generation using videoId from videos_master.")


if __name__ == "__main__":
    main()
