from flask import Flask, Blueprint, request, jsonify
from datetime import datetime, date, timedelta
from pymongo import MongoClient
import re
from flask_cors import CORS
from email_validator import validate_email, EmailNotValidError
import pandas as pd
import os
from flask_swagger_ui import get_swaggerui_blueprint
from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity
from dotenv import load_dotenv
from ETL.ETL_utils_ import db
from deep_translator import GoogleTranslator
from controller.language_controller import translate_message         
 
 
# Load environment variables
 
load_dotenv()
 
# Flask App Setup
 
app = Flask(__name__)
CORS(app)
 
 
SWAGGER_URL = '/swagger'
API_URL = '/static/root.yaml'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "KCL Flask API"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)
 
 
basic_details_bp = Blueprint("basic_details", __name__)
 
@basic_details_bp.route("/api/basic_details_verification", methods=["POST"])
def participant_basic_details():
    try:
        userid = request.headers.get("userid")
        code = request.headers.get("languageCode", "en")   # default English
        data = request.json or {}
 
        dob_str = data.get("dob")
        gender = data.get("gender")
        pincode = data.get("pincode")
        contest_id = data.get("contest_id")
 
        # Required Header + Field
        if not userid:
            return jsonify({
                "statusCode": 400,
                "message": translate_message("userid is required.", code),
                "response": {}
            })
 
        if not contest_id:
            return jsonify({
                "statusCode": 400,
                "message": translate_message("contest_id is required.",code),
                "response": {}
            })
 
        # Fetch Eligibility Mapping
        mappings = list(db.eligibility_mapping.find({"contest_id": contest_id, "is_active": True}))
        if not mappings:
            return jsonify({
                "statusCode": 404,
                "message": translate_message("No active eligibility mapping found",code),
                "response": {}
            })
 
        eligibility_ids = [m["eligibility_id"] for m in mappings]
 
        # Fetch Rules
        eligibilities = list(db.eligibility_master.find({
            "eligibility_id": {"$in": eligibility_ids},
            "is_active": True
        }))
        if not eligibilities:
            return jsonify({
                "statusCode": 404,
                "message": translate_message("No eligibility rules found",code),
                "response": {}
            })
 
        # ------------------------------ PREP ------------------------------
        age = None
        invalid_dob_format = False
 
        if dob_str:
            try:
                dob = datetime.strptime(dob_str, "%d/%m/%Y").date()
                today = date.today()
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            except:
                invalid_dob_format = True
 
        # PINCODE process
        pincode_valid_format = False
        pincode_exists = True
        is_rural = True
        records = []
 
        if pincode:
            pincode_valid_format = bool(re.fullmatch(r'^[1-9][0-9]{5}$', str(pincode)))
 
            if pincode_valid_format:
                pincode_int = int(pincode)
                records = list(db.pincode_master.find({"pincode": pincode_int}, {"_id": 0}))
                pincode_exists = len(records) > 0
 
                if pincode_exists:
                    flags = {rec.get("rural(flag)", "").strip().lower() for rec in records}
                    is_rural = flags in [{"yes"}, {"yes", "no"}]
                else:
                    is_rural = False
 
        # ------------------------------ ERRORS ------------------------------
        custom_errors = []
 
        # AGE
        for rule in eligibilities:
            if rule["eligibility_name"].lower() == "age" and dob_str:
                min_age, max_age = rule["eligibility_value"]
 
                if invalid_dob_format:
                    custom_errors.append(translate_message("Invalid DOB format. Use DD/MM/YYYY.", code))
                elif age is not None and not (min_age <= age <= max_age):
                    custom_errors.append(
                        translate_message(f"Your Age does not lie between {min_age}-{max_age} Years",code)
                    )
 
        # GENDER
        for rule in eligibilities:
            if rule["eligibility_name"].lower() == "gender" and gender:
                expected = rule["eligibility_value"]
                if gender.lower() != expected.lower():
                    custom_errors.append(
                        translate_message(f"The contest is for '{expected}' Only",code)
                    )
 
        # PINCODE
        if pincode:
            if not pincode_valid_format:
                custom_errors.append(translate_message("Invalid Pincode format",code))
            elif not pincode_exists:
                custom_errors.append(translate_message("The Pincode does not exist in our system",code))
            elif not is_rural:
                custom_errors.append(translate_message("The Pincode does not lie in Rural India",code))
 
        # ------------------------------ RETURN IF NOT ELIGIBLE ------------------------------
        if custom_errors:
            return jsonify({
                "statusCode": 400,
                "message": translate_message("Sorry. You are not eligible.",code),
                "response": {"errors": custom_errors}
            })
 
        # ------------------------------ INSERT ONLY IF ELIGIBLE + ALL THREE FIELDS PRESENT ------------------------------
        if dob_str and gender and pincode:
            try:
                now = datetime.utcnow()
 
                insert_doc = {
                    "contest_id": contest_id,
                    "userid": userid,
                    "isPaymentDone": False,
                    "isEligible": True,
                    "created_by": userid,
                    "updated_by": userid,
                    "created_on": now,
                    "updated_on": now
                }
 
                db.user_contest_mapping.insert_one(insert_doc)
 
            except Exception as e:
                print("Insert Error:", str(e))
 
        # ------------------------------ SUCCESS RESPONSE ------------------------------
        return jsonify({
            "statusCode": 200,
            "message": translate_message("Yes. You are eligible.",code),
            "response": {}
        })
 
    except Exception as e:
        return jsonify({
            "statusCode": 500,
            "message": translate_message(f"Internal Server Error: {str(e)}",code),
            "response": {}
        }), 500