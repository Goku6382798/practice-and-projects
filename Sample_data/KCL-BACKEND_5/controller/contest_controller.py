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