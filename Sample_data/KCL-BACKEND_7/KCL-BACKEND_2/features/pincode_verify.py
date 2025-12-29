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
from controller.language_controller import translate_message

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

pincode_bp = Blueprint("pincode", __name__)


# ------------------------------
# Pincode Verify
# ------------------------------
@pincode_bp.route('/api/pincode_verify', methods=['POST'])
@jwt_required(optional=True)
def pincode_verify():
    try:
        pincode = request.json.get('pincode', None)
        languageCode = request.headers.get("languageCode", "en")

        if not pincode:
            return jsonify({"statusCode": 400,
                            "message": translate_message("Pincode is required", languageCode),
                            "response": {}})
        if not re.fullmatch(r'^[1-9][0-9]{5}$', pincode):
            return jsonify({"statusCode": 400,
                            "message": translate_message("Pincode must be 6 digits and cannot start with 0", languageCode),
                            "response": {}})

        pincode = int(pincode)
        # --- Fetch all matching records ---
        records = list(db.pincode_master.find({"pincode": pincode}, {"_id": 0}))

        if not records:
            return jsonify({
                "statusCode": 200,
                "message": translate_message("Sorry, you are not eligible.", languageCode),
                "isfromrural" : False,
                "response": {}})

        # --- Extract rural(flag) values (normalized) ---
        flags = {rec.get("rural(flag)", "").strip().lower() for rec in records}

        # --- Determine message ---
        if flags == {"yes"}:
            message = "Yes, you are eligible."
            isfromrural = True
        # elif flags == {"no"}:
        #     message = "urban pincode"
        elif flags == {"yes", "no"}:
            #message = "both rural and urban pincode"
            message = "Yes, you are eligible."
            isfromrural = True
        else:
            message = "Sorry, you are not eligible."
            isfromrural = False

        return jsonify({
            "statusCode": 200,
            "message": translate_message(message, languageCode),
            "isfromrural":isfromrural,
            "response": {}
        })

    except Exception as e:
        print(f"❌ Error in pincode_verify: {e}")
        return jsonify({"statusCode": 500, 
                        "message": translate_message("Internal server error", languageCode), 
                        "response": {}})
