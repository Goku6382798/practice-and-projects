# in your existing user_profile.py
import base64
import json
import uuid
from ETL.ETL_utils_ import db, upload_to_s3
from flask import Flask, request, jsonify, send_file, Blueprint
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timedelta
import re
import pandas as pd
import os
from flask_swagger_ui import get_swaggerui_blueprint
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from dotenv import load_dotenv
import googlemaps
import pymongo.errors

from controller.user_profile_controller import (strip_plus91_basic, _user_exists, _get_user_contact_and_flags,
                                                _gender_from_code, _language_from_code,
                                                _interests_from_codes, _build_profile_details)

# ------------------------------
# Load environment variables
# ------------------------------
load_dotenv()

# ------------------------------
# Flask App Setup
# ------------------------------
app = Flask(__name__)
CORS(app)

# JWT configuration
app.config['JWT_SECRET_KEY'] = os.getenv("SECRET_KEY", "default_secret_key")
jwt_exp_minutes = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES_MINUTES", 30))
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=jwt_exp_minutes)
jwt = JWTManager(app)

# ------------------------------
# Custom JWT error handlers
# ------------------------------
@jwt.unauthorized_loader
def custom_unauthorized_response(err_str):
    return jsonify({"statusCode": 401, "message": "Invalid user", "response": {}})


@jwt.invalid_token_loader
def custom_invalid_token_response(err_str):
    return jsonify({"statusCode": 422, "message": "Invalid token", "response": {}})


@jwt.expired_token_loader
def custom_expired_token_response(jwt_header, jwt_payload):
    return jsonify({"statusCode": 401, "message": "Token expired, please login again", "response": {}})



user_profile_bp = Blueprint("user_profile", __name__)


@user_profile_bp.route("/api/user_profile", methods=["POST"])
@jwt_required(optional=True)
def save_or_update_user_profile():
    try:
        user_id = request.headers.get("userId", None)

        is_multipart = bool(request.content_type and "multipart/form-data" in request.content_type.lower())
        data = request.form.to_dict() if is_multipart else (request.get_json() or {})

        profile_id = data.get("profileid")

        if not user_id:
            return jsonify({"statusCode": 400, "message": "userid is required", "response": {}})
        if not _user_exists(user_id):
            return jsonify({"statusCode": 400, "message": "Invalid userid (not found)", "response": {}})

        info = _get_user_contact_and_flags(user_id)

        # Handle Image Upload with user folder + counter
        # ------------------------------
        uploaded_image_url = None

        def generate_s3_filename(user_id, original_filename):
            ext = original_filename.split(".")[-1].lower()

            # Count how many files already exist for this user in db
            existing_urls = db.user_master.find_one({"userid": user_id}, {"profileImage": 1})

            counter = 1
            if existing_urls and existing_urls.get("profileImage"):
                # If previous image existed → increment counter
                prev_url = existing_urls["profileImage"]
                # Format stored: user_profile/<user_id>/<user_id>_<number>.jpg
                try:
                    old_num = int(prev_url.split("_")[-1].split(".")[0])
                    counter = old_num + 1
                except:
                    counter = 1

            return f"user_profile/{user_id}/{user_id}_{counter}.{ext}"

        # If multipart
        if is_multipart and "profileImage" in request.files:
            img_file = request.files["profileImage"]
            if img_file and img_file.filename:
                file_bytes = img_file.read()
                content_type = img_file.mimetype or "image/png"

                s3_key = generate_s3_filename(user_id, img_file.filename)
                uploaded_image_url = upload_to_s3(file_bytes, s3_key, content_type=content_type)

        # If base64 JSON upload
        else:
            raw_image = data.get("profileImage")
            if isinstance(raw_image, str) and raw_image.startswith("data:"):
                try:
                    header, b64data = raw_image.split(",", 1)
                    file_bytes = base64.b64decode(b64data)

                    if "image/png" in header:
                        ext = "png"
                        content_type = "image/png"
                    else:
                        ext = "jpg"
                        content_type = "image/jpeg"

                    s3_key = f"user_profile/{user_id}/{user_id}_1.{ext}"
                    uploaded_image_url = upload_to_s3(file_bytes, s3_key, content_type=content_type)
                except Exception:
                    pass

        # --- Parse fields ---
        def parse_obj(val):
            if isinstance(val, dict):
                return val
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except Exception:
                    return {}
            return {}

        def pick_code(obj):
            return (obj or {}).get("code", "") if isinstance(obj, dict) else ""

        gender_obj = parse_obj(data.get("gender"))
        lang_obj = parse_obj(data.get("preferredLanguage"))
        gender_code = pick_code(gender_obj)
        lang_code = pick_code(lang_obj)

        interests_val = data.get("interestWith")
        if isinstance(interests_val, str):
            try:
                interests_val = json.loads(interests_val)
            except Exception:
                interests_val = []
        if not isinstance(interests_val, list):
            interests_val = []
        interest_codes = [i.get("code") for i in interests_val if isinstance(i, dict) and i.get("code")]

        # --- Common fields ---
        set_common = {
            "name": data.get("name", ""),
            "nickname": data.get("nickname", ""),
            "userHandle": data.get("userHandle", ""),
            "gender": gender_code,
            "preferredLanguage": lang_code,
            "interestWith": interest_codes,
            "userid": user_id,
            "mobile": info["mobile"],
            "mail": info["mail"],
            "updated_at": datetime.utcnow(),
        }
        # --- Validation: if any of the mandatory fields are empty ---
        required_fields = {
            "name": set_common["name"],
            #"nickname": set_common["nickname"],
            "gender": set_common["gender"],
            #"userHandle": set_common["userHandle"],
            "preferredLanguage": set_common["preferredLanguage"],
            "interestWith": set_common["interestWith"],
        }

        # Check for any empty or missing values
        empty_fields = [k for k, v in required_fields.items() if not v or (isinstance(v, list) and len(v) == 0)]
        if empty_fields:
            return jsonify({
                "statusCode": 400,
                "message": "empty",
                "response": {"missing_fields": empty_fields}
            })

        # --- Simple duplicacy check for userHandle ---
        if db.user_master.find_one({"userHandle": set_common["userHandle"]}):
            return jsonify({
                "statusCode": 400,
                "message": "User handle already exists, please choose another one",
                "response": {}
            })

        profile_data = db.user_master.find_one({"userid": user_id}, {"_id":0})
        profile_id = profile_data.get("profileid", None)

        if profile_data and profile_id is not None:


            set_doc = dict(set_common)
            new_image_from_json = (not is_multipart) and isinstance(data.get("profileImage"), str) and data.get("profileImage").strip()
            if uploaded_image_url:
                set_doc["profileImage"] = uploaded_image_url
            elif new_image_from_json:
                set_doc["profileImage"] = data.get("profileImage").strip()

            db.user_master.update_one(
                {"userid": user_id},
                {"$set": set_doc})
            # if res.matched_count == 0:
            #     return jsonify({"statusCode": 404, "message": "Profile not found for given userid/profileid", "response": {}})
            #
            saved = db.user_master.find_one(
                {"userid": user_id},
                {"_id": 0, "created_at": 0, "updated_at": 0}
            )

            # db.user_mpin.update_many({"userid": user_id}, {"$set": {"isprofilesetup": True}})
            # db.user_mobile_login.update_many({"userid": user_id}, {"$set": {"isprofilesetup": True}})
            # db.user_mail_login.update_many({"userid": user_id}, {"$set": {"isprofilesetup": True}})
            db.user_master.update_one({"userid": user_id}, {"$set": {"isprofilesetup": True}})

            info_after = _get_user_contact_and_flags(user_id)

            return jsonify({
                "statusCode": 200,
                "message": "Profile updated successfully",
                "response": {
                    "details": _build_profile_details(saved),
                    "isprofilesetup": info_after["isprofilesetup"],
                    "ismpinsetup": info_after["ismpinsetup"]
                }
            })

        # --- CREATE ---
        elif profile_data and profile_id is None:

            now = datetime.utcnow()
            new_profile_id = str(uuid.uuid4())

            create_doc = {
                **set_common,
                "profileImage": uploaded_image_url if uploaded_image_url else data.get("profileImage", ""),
                "profileid": new_profile_id,
                "created_at": now,
            }

            db.user_master.update_one({"userid": user_id}, {"$set": create_doc})

            saved = db.user_master.find_one(
                {"profileid": new_profile_id, "userid": user_id},
                {"_id": 0, "created_at": 0, "updated_at": 0}
            )

            # db.user_mpin.update_many({"userid": user_id}, {"$set": {"isprofilesetup": True}})
            # db.user_mobile_login.update_many({"userid": user_id}, {"$set": {"isprofilesetup": True}})
            # db.user_mail_login.update_many({"userid": user_id}, {"$set": {"isprofilesetup": True}})
            db.user_master.update_one({"userid": user_id}, {"$set": {"isprofilesetup": True}})

            info_after = _get_user_contact_and_flags(user_id)

            return jsonify({
                "statusCode": 200,
                "message": "Profile created successfully",
                "response": {
                    "details": _build_profile_details(saved),
                    "isprofilesetup": info_after["isprofilesetup"],
                    "ismpinsetup": info_after["ismpinsetup"]
                }
            })

    except Exception as e:
        print(f"Error in /api/user_profile: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})




@user_profile_bp.route("/api/user_handle", methods=["POST"])
@jwt_required(optional=True)
def check_user_handle():
    try:
        data = request.get_json() or {}
        user_handle = data.get("userHandle", "").strip()

        user_handle_lst = db.user_master.find_one({"userHandle": user_handle})

        if not user_handle_lst:
            return jsonify({
                "statusCode": 200,
                "message": "user handle is available",
                "response" : {}
            })

        if user_handle_lst:
            return jsonify({
                "statusCode": 400,
                "message": "User handle already exists, please choose another one",
                "response": {}
            })


    except Exception as e:
        print("Error in /api/check_user_handle =>", e)
        return jsonify({
            "statusCode": 500,
            "message": "Internal server error",
            "response": {}
        })
