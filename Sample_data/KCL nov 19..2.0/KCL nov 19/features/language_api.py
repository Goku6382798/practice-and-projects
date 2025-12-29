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

googlemaps_key = os.getenv("googlemaps_key", None)
gmaps = googlemaps.Client(key=googlemaps_key)

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


language_bp = Blueprint("language", __name__)

# ------------------------------
# Database placeholders
# ------------------------------
from ETL.ETL_utils_ import db, conn

def strip_plus91_basic(number: str) -> str:
    # Remove leading +, spaces, dashes first
    s = number.strip().replace(" ", "").replace("-", "")
    # Remove leading '+'
    if s.startswith("+"):
        s = s[1:]
    # If starts with '91' remove it
    if s.startswith("91") and len(s) > 10:
        s = s[2:]
    return s

import uuid

def six_digit_from_uuid():
    return f"{uuid.uuid4().int % 1_000_000:06d}"

def dup_id(mail_mob,login_type):
    while True:
        userid = six_digit_from_uuid()
        user_type = "2"
        try:
            if login_type == "1":
                db.user_mobile_login.insert_one({
                    "userid": userid,
                    "user_type": user_type,
                    "mobile": mail_mob
                })
            if login_type == "2":
                db.user_mail_login.insert_one({
                    "userid": userid,
                    "user_type": user_type,
                    "mail": mail_mob
                })

            break
        except pymongo.errors.DuplicateKeyError:
            print("entered except")
            # Try again if duplicate
            continue



# ------------------------------
# Get Languages
# ------------------------------
@language_bp.route('/api/get_languages', methods=['POST'])
@jwt_required(optional=True)
def get_languages():
    try:
        latitude = request.json.get("latitude", None)
        longitude = request.json.get("longitude", None)

        if not latitude and not longitude:
            try:
                languages_data = pd.DataFrame(list(db.language_master.find({}, {"_id": 0})))
                return jsonify({"statusCode": 200, "message": "Success", "response": {"languages": languages_data.to_dict('records')}})
            except Exception as e:
                print(f"Error in get_languages: {e}")
                return jsonify({"statusCode": 500, "message": "No language found", "response": {}})

        elif latitude and longitude:
            result = gmaps.reverse_geocode((latitude, longitude))
            state = None
            if result:
                for component in result[0]['address_components']:
                    if "administrative_area_level_1" in component['types']:
                        state = component['long_name']
                        break

            if state:
                try:
                    cursor = db.standard_state_ut_language.find({}, {"_id": 0})
                    data = pd.DataFrame(list(cursor))
                    english_data = data[data['code'] == 'en']
                    state_data = data[data['state_ut'] == state]
                    other_data = data[~data.index.isin(english_data.index) & ~data.index.isin(state_data.index)]
                    final_data = pd.concat([english_data, state_data, other_data])
                    final_data = final_data[["code", "name", "native", "symbol"]].drop_duplicates()
                    return jsonify({"statusCode": 200, "message": "Success", "response": {"languages": final_data.to_dict('records')}})
                except Exception as e:
                    print(f"Error fetching state languages: {e}")
                    return jsonify({"statusCode": 500, "message": "Error fetching languages", "response": {}})
            return jsonify({"statusCode": 404, "message": "No language found from given coordinates", "response": {}})
        else:
            return jsonify({"statusCode": 400, "message": "Either provide both latitude and longitude or none.", "response": {}})

    except Exception as e:
        print(f"Error in get_languages: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})

