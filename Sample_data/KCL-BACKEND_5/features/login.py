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
#from mapping import master_mapping, empty_mapping
from controller.login_controller import master_mapping, empty_mapping, dup_id, blank_address
from controller.auth_controller import issue_tokens
from controller.logout_controller import (
    resolve_userid_from_body,
    logout_current_session,
    logout_all_sessions,
)
from controller.language_controller import translate_message

from ETL.ETL_utils_ import db, conn
import uuid

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

login_bp = Blueprint("login", __name__)


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


# ------------------------------
# Generate OTP
# ------------------------------
@login_bp.route("/api/generate_otp", methods=["POST"])
def generate_otp():
    try:
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)
        languageCode = request.headers.get("languageCode", "en")


        if login_type == "1" and mobile:

            result = db.user_master.find_one({'mobile': mobile}, {"_id": 0})
            if result:
                #### check and return
                if result.get('ismobileregister') == True:  #result login
                    if result.get('ismpinsetup') == True:    # result_mpin
                        print("2")

                        return jsonify({
                            "statusCode": 200,
                            "message": translate_message("OTP generated successfully", languageCode),
                            "response": {
                                "details": {
                                    "user_id": result.get('userid', None),
                                    "user_type": result.get('user_type', 1),
                                    "name": "Test Sample",
                                    "mobile": mobile,
                                    "mail": "features case.com",
                                    "dob": "05/05/2025",
                                    "address": "123 society name, city, pincode"
                                },
                                "ismpinsetup": result.get('ismpinsetup', False),
                                "isprofilesetup": result.get('isprofilesetup', False)
                            }
                        })
                    else:
                        return jsonify({
                            "statusCode": 200,
                            "message": translate_message("OTP generated successfully",languageCode),
                            "response": {
                                "details": {
                                    "user_id": result.get('userid', None),
                                    "user_type": result.get('user_type', "1"),
                                    "name": "Test Sample",
                                    "mobile": mobile,
                                    "mail": "features case.com",
                                    "dob": "05/05/2025",
                                    "address": "123 society name, city, pincode"
                                },
                                "ismpinsetup": result.get('ismpinsetup', False),
                                "isprofilesetup": result.get('isprofilesetup', False)
                            }
                        })
            else:
                if mobile is None:
                    return jsonify({"statusCode": 400, "message": "Mobile number is required", "response": {}})
                if not re.match(r"^\+[1-9]\d{1,3}[6-9]\d{9}$", mobile):
                    return jsonify({
                        "statusCode": 400,
                        "message": translate_message("Invalid mobile number format. Must include + country_code and numbers/digits",languageCode),
                        "response": {}
                    })

                dup_id(mobile, login_type)
                return jsonify({
                    "statusCode": 200,
                    "message": translate_message("OTP generated successfully",languageCode),
                    "response": {
                        "details": {
                            "user_id": "",
                            "user_type": "1",
                            "name": "",
                            "mobile": mobile,
                            "mail": "",
                            "dob": "",
                            "address": ""
                        },
                        "ismpinsetup": False,
                        "isprofilesetup": False
                    }
                })

        elif login_type == "2" and mail:
            result = db.user_master.find_one({'mail': mail}, {"_id": 0})
            if result:
                # check and return
                if result.get('ismailregister') == True:   #result_login
                    print("1")
                    if result.get('ismpinsetup') == True:   #result_mpin
                        return jsonify({
                            "statusCode": 200,
                            "message": translate_message("OTP generated successfully",languageCode),
                            "response": {
                                "details": {
                                    "user_id": result.get('userid', None),
                                    "user_type": result.get('user_type', 1),
                                    "name": "Test Sample",
                                    "mobile": "mobile",
                                    "mail": mail,
                                    "dob": "05/05/2025",
                                    "address": "123 society name, city, pincode"
                                },
                                "ismpinsetup": result.get('ismpinsetup', False),   #resultmpin
                                "isprofilesetup": result.get('isprofilesetup', False)   #resultmpin
                            }
                        })
                    else:
                        print("inside else")
                        return jsonify({
                            "statusCode": 200,
                            "message": translate_message("OTP generated successfully",languageCode),
                            "response": {
                                "details": {
                                    "user_id": result.get('userid', None),   # resultlogin
                                    "user_type": result.get('user_type', "1"),  # resultlogin
                                    "name": "Test Sample",
                                    "mobile": "mobile",
                                    "mail": mail,
                                    "dob": "05/05/2025",
                                    "address": "123 society name, city, pincode"
                                },
                                "ismpinsetup": result.get('ismpinsetup', False),
                                "isprofilesetup": result.get('isprofilesetup', False)
                            }
                        })
            else:
                if mail is None:
                    return jsonify({"statusCode": 400,
                                    "message": translate_message("Mail is required",languageCode),
                                    "response": {}})

                forbidden_chars = r'[#\$\%\^\&\*\(\)\=\+\|\\\<\>\?\/\`\~]'
                if re.search(forbidden_chars, mail.split('@')[0]):
                    return jsonify({"statusCode": 400,
                                    "message": translate_message("Mail contains invalid characters",languageCode),
                                    "response": {}})

                try:
                    validate_email(mail)
                    dup_id(mail, login_type)
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("OTP generated successfully",languageCode),
                        "response": {
                            "details": {
                                "user_id": "",
                                "user_type": "1",
                                "name": "",
                                "mobile": "",
                                "mail": mail,
                                "dob": "",
                                "address": ""
                            },
                            "ismpinsetup": False,
                            "isprofilesetup": False
                        }
                    })

                except EmailNotValidError:
                    return jsonify({"statusCode": 400,
                                    "message": translate_message("Mail is not valid",languageCode),
                                    "response": {}})
        else:
            return jsonify({"statusCode": 400,
                            "message": translate_message("Mail/Mobile limatch with login type", languageCode),
                           "response": {}})

    except Exception as e:
        print(f"Error in generate_otp: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})


# ------------------------------
# Verify OTP
# ------------------------------
from controller.auth_controller import issue_tokens

@login_bp.route("/api/verify_otp", methods=["POST"])
def verify_otp():
    try:
        languageCode = request.headers.get("languageCode", "en")
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)
        otp = request.json.get("otp", None)

        if not login_type:
            return jsonify({"statusCode": 400,
                            "message": translate_message("Login type is required", languageCode),
                            "response": {}})

        # -------------------- MOBILE --------------------
        if login_type == "1" and mobile:
            if not mobile or not otp:
                return jsonify({"statusCode": 400,
                                "message": translate_message("Mobile number and otp is required",languageCode),
                                "response": {}})
            if otp != "1234":
                return jsonify({"statusCode": 401,
                                "message": translate_message("Incorrect OTP, please try again.", languageCode),
                                "response": {}})

            result = db.user_master.find_one({"mobile": mobile}, {"_id": 0})
            if not result:
                return jsonify({"statusCode": 401,
                                "message": translate_message("Invalid mobile number",languageCode),
                                "response": {}})

            user_id = result.get('userid')
            isprofilesetup = result.get('isprofilesetup', False)

            # ALWAYS issue AT + RT here (new user gets full session now)
            access_token, refresh_token, session_id = issue_tokens(userid=user_id)

            if isprofilesetup is True:
                user_details = db.user_master.find_one(
                    {"userid": user_id},
                    {"_id": 0, 'created_at': 0, 'updated_at': 0}
                )
                user_details_mapped = master_mapping(user_details)
                return jsonify({
                    "statusCode": 200,
                    "message": translate_message("OTP verified successfully",languageCode),
                    "response": {
                        "details": user_details_mapped,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "session_id": session_id,
                        "ismpinsetup": result.get('ismpinsetup', False),
                        "isprofilesetup": result.get('isprofilesetup', False)
                    }
                })
            else:
                empty_details = empty_mapping()
                empty_details['userid'] = user_id
                empty_details['mobile'] = mobile
                return jsonify({
                    "statusCode": 200,
                    "message": translate_message("OTP verified successfully",languageCode),
                    "response": {
                        "details": empty_details,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "session_id": session_id,
                        "ismpinsetup": result.get('ismpinsetup', False),
                        "isprofilesetup": result.get('isprofilesetup', False)
                    }
                })

        # -------------------- MAIL --------------------
        elif login_type == "2" and mail:
            if not mail or not otp:
                return jsonify({"statusCode": 400,
                                "message": translate_message("Mail is required",languageCode),
                                "response": {}})
            if otp != "1234":
                return jsonify({"statusCode": 401,
                                "message": translate_message("Incorrect OTP, please try again.", languageCode),
                                "response": {}})

            result = db.user_master.find_one({"mail": mail}, {"_id": 0})
            if not result:
                return jsonify({"statusCode": 401,
                                "message": translate_message("Invalid mail",languageCode),
                                "response": {}})

            user_id = result.get('userid')
            isprofilesetup = result.get('isprofilesetup', False)

            # ALWAYS issue AT + RT
            access_token, refresh_token, session_id = issue_tokens(userid=user_id)

            if isprofilesetup is True:
                user_details = db.user_master.find_one(
                    {"userid": user_id},
                    {"_id": 0, 'created_at': 0, 'updated_at': 0}
                )
                user_details_mapped = master_mapping(user_details)
                return jsonify({
                    "statusCode": 200,
                    "message": translate_message("OTP verified successfully",languageCode),
                    "response": {
                        "details": user_details_mapped,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "session_id": session_id,
                        "ismpinsetup": result.get('ismpinsetup', False),
                        "isprofilesetup": result.get('isprofilesetup', False)
                    }
                })
            else:
                empty_details = empty_mapping()
                empty_details['userid'] = user_id
                empty_details['mail'] = mail
                return jsonify({
                    "statusCode": 200,
                    "message": translate_message("OTP verified successfully",languageCode),
                    "response": {
                        "details": empty_details,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "session_id": session_id,
                        "ismpinsetup": result.get('ismpinsetup', False),
                        "isprofilesetup": result.get('isprofilesetup', False)
                    }
                })

        else:
            return jsonify({"statusCode": 400,
                            "message": translate_message("Invalid login type", languageCode),
                            "response": {}})

    except Exception as e:
        print(f"Error in verify_otp: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})


# ------------------------------
# MPIN: Create
# ------------------------------
@login_bp.route('/api/create_mpin', methods=['POST'])
@jwt_required()
def create_mpin():
    try:
        languageCode = request.headers.get("languageCode", "en")
        login_type = request.json.get('login_type', None)
        mobile = request.json.get('mobile', None)
        mail = request.json.get('mail', None)
        mpin = request.json.get('mpin', None)

        if not all([login_type, mobile or mail, mpin]):
            return jsonify({"statusCode": 400,
                            "message": translate_message("All fields are required",languageCode),
                            "response": {}})

        if mobile:
            print("1")
            mpin_data = db.user_master.find_one({"mobile": mobile})
            print("2")
            if mpin_data.get('mpin'):
                return jsonify({"statusCode": 409,
                                "message": translate_message("MPIN already exists",languageCode),
                                "response": {}})
        elif mail:
            mpin_data = db.user_master.find_one({"mail": mail})
            if mpin_data.get('mpin'):
                return jsonify({"statusCode": 409,
                                "message": translate_message("MPIN already exists",languageCode),
                                "response": {}})
        if len(mpin) != 4 or not mpin.isdigit():
            return jsonify({"statusCode": 400,
                            "message": translate_message("MPIN must be exactly 4 digits",languageCode),
                            "response": {}})

        if mobile:
            result = db.user_master.find_one({'mobile': mobile}, {"_id": 0})
            user_id = result.get('userid', None)
            db.user_master.update_one(
                {'mobile': mobile},
                {'$set': {'mpin': mpin,"user_type": "3","ismpinsetup": True}})
            result = db.user_master.find_one({"mobile": mobile}, {"mpin": 1, "ismpinsetup": 1, "isprofilesetup": 1})
            return jsonify({"statusCode": 200,
                            "message": translate_message("MPIN created successfully",languageCode),
                            "response": {
                                "details": {
                                    "name": "Test Sample",
                                    "user_id": user_id,
                                    "user_type": "3",
                                    "mobile": mobile,
                                    "mail": "abc123@domain.com",
                                    "dob": "05/05/2025",
                                    "address": "123 society name, city, pincode"
                                },
                                "ismpinsetup": result.get('ismpinsetup'),
                                "isprofilesetup": result.get('isprofilesetup'),
                            }})
        if mail:
            result = db.user_master.find_one({'mail': mail}, {"_id": 0})
            user_id = result.get('userid', None)
            db.user_master.update_one(
                {'mail': mail},
                {'$set': {'mpin': mpin, "user_type": "3", "ismpinsetup": True}})
            result = db.user_master.find_one({"mail": mail}, {"mpin": 1, "ismpinsetup": 1, "isprofilesetup": 1})
            return jsonify({"statusCode": 200,
                            "message": translate_message("MPIN created successfully",languageCode),
                            "response": {
                                "details": {
                                    "name": "Test Sample",
                                    "user_id": user_id,
                                    "user_type": "3",
                                    "mobile": "+919876543210",
                                    "mail": mail,
                                    "dob": "05/05/2025",
                                    "address": "123 society name, city, pincode"
                                },
                                "ismpinsetup": result.get('ismpinsetup'),
                                "isprofilesetup": result.get('isprofilesetup'),
                            }})

    except Exception as e:
        print(f"Error in create_mpin: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})


# ------------------------------
# MPIN: Generate_otp
# ------------------------------
@login_bp.route("/api/mpin_generate_otp", methods=["POST"])
# @jwt_required()
def mpin_generate_otp():
    try:
        languageCode = request.headers.get("languageCode", "en")
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)

        if not all([login_type, mobile or mail]):
            return jsonify({"statusCode": 400,
                            "message": translate_message("All fields are required",languageCode),
                            "response": {}})
        if login_type == "1" and mobile:
            return jsonify({"statusCode": 200,
                            "message": translate_message("OTP generated successfully on mobile",languageCode),
                            "response": {}})
        elif login_type == "2" and mail:
            return jsonify({"statusCode": 200,
                            "message": translate_message("OTP generated successfully on mail",languageCode),
                            "response": {}})
        else:
            return jsonify({"statusCode": 400,
                            "message": translate_message("OTP generation failed", languageCode),
                            "response": {}})

    except Exception as e:
        print(f"Error in verify_otp: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})


# ------------------------------
# MPIN Verify OTP
# ------------------------------
@login_bp.route("/api/mpin_verify_otp", methods=["POST"])
@jwt_required()
def mpin_verify_otp():
    try:
        languageCode = request.headers.get("languageCode", "en")
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)
        otp = request.json.get("otp", None)

        if not login_type:
            return jsonify({"statusCode": 400,
                            "message": translate_message("Login type is required",languageCode),
                            "response": {}})
        if not otp:
            return jsonify({"statusCode": 400,
                            "message": translate_message("OTP is required", languageCode),
                            "response": {}})

        if login_type == "1":
            if not mobile or not otp:
                return jsonify({"statusCode": 400,
                                "message": translate_message("Mobile number and otp is required", languageCode),
                                "response": {}})

            if otp == "1234":
                result = db.user_master.find_one({"mobile": mobile}, {"_id": 0})
                if result:
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("OTP verified successfully",languageCode),
                        "response": {
                            "details": {
                                "name": "Test Sample",
                                "mobile": mobile,
                                "user_id": result.get('userid', None),
                                "user_type": result.get('user_type', None),
                                "mail": "features case.com",
                                "dob": "05/05/2025",
                                "address": "123 society name, city, pincode"
                            },
                            "ismpinsetup": result.get('ismpinsetup', False),
                            "isprofilesetup": result.get('isprofilesetup', False)
                        }
                    })
                else:
                    return jsonify({
                        "statusCode": 401,
                        "message": translate_message("OTP verification failed.",languageCode),
                        "response": {},

                    })

            else:
                return jsonify({"statusCode": 401,
                                "message": translate_message("Incorrect OTP, please try again.", languageCode),
                                "response": {}})

        elif login_type == "2":
            if not mail or not otp:
                return jsonify({"statusCode": 400,
                                "message": translate_message("Mail is required",languageCode),
                                "response": {}})

            if otp == "1234":
                result = db.user_master.find_one({"mail":mail}, {"_id":0})
                if result:
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("OTP verified successfully",languageCode),
                        "response": {
                            "details": {
                                "name": "Test Sample",
                                "mobile": "features case",
                                "user_id":result.get('userid', None),
                                "user_type":result.get('user_type', None),
                                "mail": mail,
                                "dob": "05/05/2025",
                                "address": "123 society name, city, pincode"
                            },
                            "ismpinsetup": result.get('ismpinsetup', False),
                            "isprofilesetup": result.get('isprofilesetup', False)
                        }
                    })
                else:
                    return jsonify({
                        "statusCode": 401,
                        "message": translate_message("OTP verification failed",languageCode),
                        "response": {}})

            else:
                return jsonify({"statusCode": 401,
                                "message": translate_message("Incorrect OTP, please try again.",languageCode),
                                "response": {}})

        else:
            return jsonify({"statusCode": 400,
                            "message": translate_message("Invalid login type",languageCode),
                            "response": {}})

    except Exception as e:
        print(f"Error in verify_otp: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})


# ------------------------------
# MPIN: Verify
# ------------------------------
@login_bp.route('/api/verify_mpin', methods=['POST'])
@jwt_required(optional=True)
def verify_mpin():
    try:
        languageCode = request.headers.get("languageCode", "en")
        login_type = request.json.get('login_type', None)
        mobile = request.json.get('mobile', None)
        mail = request.json.get('mail', None)
        mpin = request.json.get('mpin', None)

        if not all([login_type, (mobile or mail), mpin]):
            return jsonify({"statusCode": 400,
                            "message": translate_message("All fields are required",languageCode),
                            "response": {}})

        # -------------------- MOBILE --------------------
        if login_type == "1" and mobile:
            result = db.user_master.find_one({"mobile": mobile}, {"_id": 0})
            if not result:
                return jsonify({"statusCode": 400,
                                "message": translate_message("MPIN verification failed", languageCode),
                                "response": {}})

            isprofilesetup = result.get('isprofilesetup', False)
            user_id = result.get('userid')

            if result.get('mpin') == mpin:
                # FULL session after MPIN verify → AT + RT
                access_token, refresh_token, session_id = issue_tokens(userid=user_id)

                if isprofilesetup is True:
                    user_type = result.get('user_type')
                    user_details = db.user_master.find_one(
                        {"userid": user_id},
                        {"_id": 0, 'created_at': 0, 'updated_at': 0}
                    )
                    user_details_mapped = master_mapping(user_details)
                    user_details_mapped['user_type'] = user_type
                    user_details_mapped["isaadharverified"] = result.get('isaadharverified', False)
                    user_details_mapped["isCommunicationaddressadded"] = result.get('isCommunicationaddressadded', False)
                    communicationaddress, address = blank_address()
                    user_details_mapped["communicationaddress"] = result.get('communicationaddress', communicationaddress)
                    user_details_mapped["address"] = result.get('address', address)
                    user_details_mapped["dob"] = result.get('dob', "")
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("MPIN verified successfully",languageCode),
                        "response": {
                            "details": user_details_mapped,
                            "access_token": access_token,
                            "refresh_token": refresh_token,
                            "session_id": session_id,
                            "ismpinsetup": result.get('ismpinsetup'),
                            "isprofilesetup": result.get('isprofilesetup'),
                        }
                    })
                else:
                    empty_details = empty_mapping()
                    empty_details['userid'] = user_id
                    empty_details['mobile'] = mobile
                    empty_details["isaadharverified"] = result.get('isaadharverified', False)
                    empty_details["isCommunicationaddressadded"] = result.get('isCommunicationaddressadded', False)
                    communicationaddress, address = blank_address()
                    empty_details["communicationaddress"] = result.get('communicationaddress', communicationaddress)
                    empty_details["address"] = result.get('address', address)
                    empty_details["dob"] = result.get('dob', "")
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("MPIN verified successfully",languageCode),
                        "response": {
                            "details": empty_details,
                            "access_token": access_token,
                            "refresh_token": refresh_token,
                            "session_id": session_id,
                            "ismpinsetup": result.get('ismpinsetup', False),
                            "isprofilesetup": result.get('isprofilesetup', False)
                        }
                    })

            return jsonify({"statusCode": 400,
                            "message": translate_message("Incorrect MPIN, please try again.",languageCode),
                            "response": {}})

        # -------------------- MAIL --------------------
        elif login_type == "2" and mail:
            result = db.user_master.find_one({"mail": mail}, {"_id": 0})
            if not result:
                return jsonify({"statusCode": 400,
                                "message": translate_message("MPIN verification failed",languageCode),
                                "response": {}})

            isprofilesetup = result.get('isprofilesetup', False)
            user_id = result.get('userid')

            if result.get('mpin') == mpin:
                # FULL session after MPIN verify → AT + RT
                access_token, refresh_token, session_id = issue_tokens(userid=user_id)

                if isprofilesetup is True:
                    user_type = result.get('user_type')
                    user_details = db.user_master.find_one(
                        {"userid": user_id},
                        {"_id": 0, 'created_at': 0, 'updated_at': 0}
                    )
                    user_details_mapped = master_mapping(user_details)
                    user_details_mapped['user_type'] = user_type
                    user_details_mapped["isaadharverified"] = result.get('isaadharverified', False)
                    user_details_mapped["isCommunicationaddressadded"] = result.get('isCommunicationaddressadded', False)
                    communicationaddress, address = blank_address()
                    user_details_mapped["communicationaddress"] = result.get('communicationaddress', communicationaddress)
                    user_details_mapped["address"] = result.get('address', address)
                    user_details_mapped["dob"] = result.get('dob', "")
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("MPIN verified successfully", languageCode),
                        "response": {
                            "details": user_details_mapped,
                            "access_token": access_token,
                            "refresh_token": refresh_token,
                            "session_id": session_id,
                            "ismpinsetup": result.get('ismpinsetup'),
                            "isprofilesetup": result.get('isprofilesetup'),
                        }
                    })
                else:
                    empty_details = empty_mapping()
                    empty_details['userid'] = user_id
                    empty_details['mail'] = mail
                    empty_details["isaadharverified"] = result.get('isaadharverified', False)
                    empty_details["isCommunicationaddressadded"] = result.get('isCommunicationaddressadded', False)
                    communicationaddress, address = blank_address()
                    empty_details["communicationaddress"] = result.get('communicationaddress', communicationaddress)
                    empty_details["address"] = result.get('address', address)
                    empty_details["dob"] = result.get('dob', "")
                    return jsonify({
                        "statusCode": 200,
                        "message": translate_message("MPIN verified successfully",languageCode),
                        "response": {
                            "details": empty_details,
                            "access_token": access_token,
                            "refresh_token": refresh_token,
                            "session_id": session_id,
                            "ismpinsetup": result.get('ismpinsetup', False),
                            "isprofilesetup": result.get('isprofilesetup', False)
                        }
                    })

            return jsonify({"statusCode": 400,
                            "message": translate_message("Incorrect MPIN, please try again.", languageCode),
                            "response": {}})

        return jsonify({"statusCode": 400,
                        "message": translate_message("Invalid login type",languageCode),
                        "response": {}})

    except Exception as e:
        print(f"Error in verify_mpin: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})


# ------------------------------
# Regenerate MPIN
# ------------------------------
@login_bp.route('/api/regenerate_mpin', methods=['POST'])
@jwt_required()
def regenerate_mpin():
    try:
        languageCode = request.headers.get("languageCode", "en")
        login_type = request.json.get('login_type', None)
        mobile = request.json.get('mobile', None)
        mail = request.json.get('mail', None)
        mpin = request.json.get('mpin', None)

        if not all([login_type, mobile or mail, mpin]):
            return jsonify({"statusCode": 400,
                            "message": translate_message("All fields are required",languageCode),
                            "response": {}})

        if len(mpin) != 4 or not mpin.isdigit():
            return jsonify({"statusCode": 400,
                            "message": translate_message("MPIN must be exactly 4 digits", languageCode),
                            "response": {}})

        if mobile and login_type == "1":
            db.user_master.update_one({'mobile': mobile}, {'$set': {'mpin': mpin}})
            return jsonify(
                {"statusCode": 200,
                 "message": translate_message("MPIN regenerated successfully", languageCode),
                 "response": {"mobile": mobile}})
        elif mail and login_type == "2":
            db.user_master.update_one({'mail': mail}, {'$set': {'mpin': mpin}})
            return jsonify({"statusCode": 200,
                            "message": translate_message("MPIN regenerated successfully",languageCode),
                            "response": {"mail": mail}})
        else:
            return jsonify({"statusCode": 401,
                            "message": translate_message("MPIN regeneration failed",languageCode),
                            "response": {}})

    except Exception as e:
        print(f"Error in regenerate_mpin: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error", languageCode),
                        "response": {}})
