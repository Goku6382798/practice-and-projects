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


from ETL.ETL_utils_ import db, conn, list_s3_images


def repeat_dict(data):
   times = 2
   result = data*2
   return result

def resolve_lang(header_lang: str) -> str:
    code = (header_lang or "").strip().lower()
    if not code:
        return "en"
    # validate against language_master
    exists = db.language_master.find_one({"code": code}, {"_id": 1})
    return code if exists else "en"
def pick_dict(val, language_code: str):
    """Return localized string with fallback to English."""
    if isinstance(val, dict):
        return val.get(language_code) or val.get("en") or ""
    return val or ""