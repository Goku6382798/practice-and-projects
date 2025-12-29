from flask import Blueprint, request, jsonify
from datetime import datetime
from pymongo import errors
from flask_cors import CORS
from dotenv import load_dotenv
from ETL.ETL_utils_ import db

load_dotenv()

edit_your_profile_bp = Blueprint("edit_your_profile", __name__)
CORS(edit_your_profile_bp)

# Mongo collection
profile_collection = db["edit_your_profile"]

# Unique index on userhandle (stored in lowercase)
profile_collection.create_index("userhandle", unique=True)


@edit_your_profile_bp.route("/api/edit_your_profile", methods=["POST"])
def edit_profile():
    try:
        userid = request.headers.get("userid")
        data = request.json or {}

        if not userid:
            return jsonify({
                "statusCode": 400,
                "message": "userid is required.",
                "response": {}
            })

        # Extract fields
        name = data.get("name")
        nickname = data.get("nickname")

        # Convert to lowercase (CASE-INSENSITIVE USER HANDLE)
        userhandle_raw = data.get("userhandle", "")
        userhandle = userhandle_raw.strip().lower()

        gender = data.get("gender")

        
        language = data.get("language")

        interests = data.get("interests", [])

        if not name or not userhandle:
            return jsonify({
                "statusCode": 400,
                "message": "Name and User Handle are required.",
                "response": {}
            })

        if len(interests) > 5:
            return jsonify({
                "statusCode": 400,
                "message": "You can select a maximum of 5 interests.",
                "response": {}
            })

        allowed_genders = ["Male", "Female", "Prefer Not To Say"]
        if gender and gender not in allowed_genders:
            return jsonify({
                "statusCode": 400,
                "message": "{Please Select Your Gender}",
                "response": {}
            })

        # ------------- UNIQUE HANDLE CHECK -------------
        existing_user = profile_collection.find_one({
            "userhandle": userhandle,
            "userid": {"$ne": userid}
        })

        if existing_user:
            return jsonify({
                "statusCode": 409,
                "message": "User handle already taken.",
                "response": {}
            })

        # ------------- UPSERT LOGIC -------------
        profile_data = {
            "userid": userid,
            "name": name,
            "nickname": nickname,
            "userhandle": userhandle,
            "gender": gender,
            "language": language,   # <-- STORED AS SINGLE VALUE
            "interests": interests,
            "updated_at": datetime.utcnow()
        }

        try:
            profile_collection.update_one(
                {"userid": userid},
                {
                    "$set": profile_data,
                    "$setOnInsert": {"created_at": datetime.utcnow()}
                },
                upsert=True
            )

        except errors.DuplicateKeyError:
            return jsonify({
                "statusCode": 409,
                "message": "User handle already taken.",
                "response": {}
            })

        return jsonify({
            "statusCode": 200,
            "message": "Profile saved successfully.",
            "response": {}
        })

    except Exception as e:
        return jsonify({
            "statusCode": 500,
            "message": f"Internal Server Error: {str(e)}",
            "response": {}
        })
