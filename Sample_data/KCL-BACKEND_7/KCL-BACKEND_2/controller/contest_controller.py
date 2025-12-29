from flask import Flask, request, jsonify, send_file, Blueprint
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timedelta
import re
from email_validator import validate_email, EmailNotValidError
import pandas as pd
import os
from flask_swagger_ui import get_swaggerui_blueprint
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from dotenv import load_dotenv
import googlemaps
import pymongo.errors
import uuid
from ETL.ETL_utils_ import db, conn

from ETL.ETL_utils_ import db
from ETL.video_hls_processor import BITRATES, build_public_url, HLS_PREFIX, THUMBS_PREFIX



def pick(val, language_code):
    if isinstance(val, dict):
        return val.get(language_code) or val.get("en") or ""
    return val or ""

def valid_lang(language_code):
    if not language_code:
        return "en"
    exists = db.language_master.find_one({"code": language_code}, {"_id": 1})
    return language_code if exists else "en"

def map_title_list(items, language_code):
    out = []
    for it in items or []:
        out.append({
            "id": it.get("id"),
            "image": it.get("image"),
            "title": pick(it.get("title"),language_code )
        })
    return out

# ---------------- VIDEO HELPER ------------------

def build_video_response(video_id):
    """
    Convert videos_master document → resolutions[] format
    """

    doc = db["videos_master"].find_one({"videoId": video_id})
    if not doc:
        return None

    playback = doc.get("playback", {})
    qualities = playback.get("qualities", [])

    resolutions = []
    for q in qualities:
        s3_key = f"{HLS_PREFIX}/{video_id}/v{q}/stream.m3u8"
        url = build_public_url(s3_key)

        resolutions.append({
            "quality": f"{q}p",
            "label": f"{q}p" if q < 1080 else "1080p HD",
            "url": url,
            "bitrate": int(BITRATES[q].replace("k", ""))  # convert "1400k" → 1400
        })

    # thumbnail
    thumb_key = f"{THUMBS_PREFIX}/{video_id}.jpg"
    thumb_url = build_public_url(thumb_key)

    return {
        "videoId": video_id,
        "resolutions": resolutions,
        "thumbnail": thumb_url
    }
