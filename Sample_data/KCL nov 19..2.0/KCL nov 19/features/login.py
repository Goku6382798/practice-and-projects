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
from controller.login_controller import master_mapping, empty_mapping, strip_plus91_basic, six_digit_from_uuid, dup_id
from controller.auth_controller import issue_tokens
from controller.logout_controller import (
    resolve_userid_from_body,
    logout_current_session,
    logout_all_sessions,
)

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


def dup_id(mail_mob, login_type):
    while True:
        userid = six_digit_from_uuid()
        user_type = "2"
        try:
            if login_type == "1":
                db.user_master.insert_one({
                    "userid": userid,
                    "user_type": user_type,
                    "mobile": mail_mob,
                    "isprofilesetup":False,
                    "ismpinsetup":False,
                    "ismobileregister":True,
                    "ismailregister": False
                })
            if login_type == "2":
                db.user_master.insert_one({
                    "userid": userid,
                    "user_type": user_type,
                    "mail": mail_mob,
                    "isprofilesetup": False,
                    "ismpinsetup":False,
                    "ismobileregister": False,
                    "ismailregister": True
                })
            break
        except pymongo.errors.DuplicateKeyError:
            print("entered except")
            # Try again if duplicate
            continue


# ------------------------------
# Generate OTP
# ------------------------------
@login_bp.route("/api/generate_otp", methods=["POST"])
def generate_otp():
    try:
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)
        # if mobile:
        #     mobile = strip_plus91_basic(mobile)

        if login_type == "1" and mobile:
            # result_login = db.user_mobile_login.find_one({'mobile': mobile}, {"_id": 0})
            # result_mpin = db.user_mpin.find_one({'mobile': "+91" + mobile}, {"_id": 0})
            result = db.user_master.find_one({'mobile': mobile}, {"_id": 0})
            if result:
                #### check and return
                if result.get('ismobileregister') == True:  #result login
                    print("1")
                    if result.get('ismpinsetup') == True:    # result_mpin
                        print("2")
                        # if result_mpin.get('ismpinsetup') == True:
                        #     print("3")
                        return jsonify({
                            "statusCode": 200,
                            "message": "OTP generated successfully",
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
                            "message": "OTP generated successfully",
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
                        "message": "Invalid mobile number format. Must include + country_code and numbers/digits",
                        "response": {}
                    })

                dup_id(mobile, login_type)
                return jsonify({
                    "statusCode": 200,
                    "message": "OTP generated successfully",
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
            # result_login = db.user_mail_login.find_one({'mail': mail}, {"_id": 0})
            # result_mpin = db.user_mpin.find_one({'mail': mail}, {"_id": 0})

            result = db.user_master.find_one({'mail': mail}, {"_id": 0})

            if result:

                # check and return
                if result.get('ismailregister') == True:   #result_login
                    print("1")
                    if result.get('ismpinsetup') == True:   #result_mpin
                        # print("2")
                        # if result_mpin.get('ismpinsetup') == True:
                        return jsonify({
                            "statusCode": 200,
                            "message": "OTP generated successfully",
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
                            "message": "OTP generated successfully",
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
                    return jsonify({"statusCode": 400, "message": "Mail is required", "response": {}})

                forbidden_chars = r'[#\$\%\^\&\*\(\)\=\+\|\\\<\>\?\/\`\~]'
                if re.search(forbidden_chars, mail.split('@')[0]):
                    return jsonify({"statusCode": 400, "message": "Mail contains invalid characters", "response": {}})

                try:
                    validate_email(mail)
                    dup_id(mail, login_type)
                    return jsonify({
                        "statusCode": 200,
                        "message": "OTP generated successfully",
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
                    return jsonify({"statusCode": 400, "message": "Mail is not valid", "response": {}})
        else:
            return jsonify({"statusCode": 400, "message": "Mail/Mobile limatch with login type", "response": {}})

    except Exception as e:
        print(f"Error in generate_otp: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})


# ------------------------------
# Verify OTP
# ------------------------------
from controller.auth_controller import issue_tokens

@login_bp.route("/api/verify_otp", methods=["POST"])
def verify_otp():
    try:
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)
        otp = request.json.get("otp", None)

        if not login_type:
            return jsonify({"statusCode": 400, "message": "Login type is required", "response": {}})

        # -------------------- MOBILE --------------------
        if login_type == "1" and mobile:
            if not mobile or not otp:
                return jsonify({"statusCode": 400, "message": "Mobile number and otp is required", "response": {}})
            if otp != "1234":
                return jsonify({"statusCode": 401, "message": "Incorrect OTP, please try again.", "response": {}})

            result = db.user_master.find_one({"mobile": mobile}, {"_id": 0})
            if not result:
                return jsonify({"statusCode": 401, "message": "Invalid mobile number", "response": {}})

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
                    "message": "OTP verified successfully",
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
                    "message": "OTP verified successfully",
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
                return jsonify({"statusCode": 400, "message": "Mail is required", "response": {}})
            if otp != "1234":
                return jsonify({"statusCode": 401, "message": "Incorrect OTP, please try again.", "response": {}})

            result = db.user_master.find_one({"mail": mail}, {"_id": 0})
            if not result:
                return jsonify({"statusCode": 401, "message": "Invalid mail", "response": {}})

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
                    "message": "OTP verified successfully",
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
                    "message": "OTP verified successfully",
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
            return jsonify({"statusCode": 400, "message": "Invalid login type", "response": {}})

    except Exception as e:
        print(f"Error in verify_otp: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})


# ------------------------------
# MPIN: Create
# ------------------------------
@login_bp.route('/api/create_mpin', methods=['POST'])
@jwt_required()
def create_mpin():
    try:
        login_type = request.json.get('login_type', None)
        mobile = request.json.get('mobile', None)
        mail = request.json.get('mail', None)
        mpin = request.json.get('mpin', None)

        if not all([login_type, mobile or mail, mpin]):
            return jsonify({"statusCode": 400, "message": "All fields are required", "response": {}})

        # if mobile and db.user_mpin.find_one({"mobile": mobile}):
        #     return jsonify({"statusCode": 409, "message": "MPIN already exists", "response": {}})
        # if mail and db.user_mpin.find_one({"mail": mail}):
        #     return jsonify({"statusCode": 409, "message": "MPIN already exists", "response": {}})

        if mobile:
            print("1")
            mpin_data = db.user_master.find_one({"mobile": mobile})
            print("2")
            if mpin_data.get('mpin'):
                return jsonify({"statusCode": 409, "message": "MPIN already exists", "response": {}})
        elif mail:
            mpin_data = db.user_master.find_one({"mail": mail})
            if mpin_data.get('mpin'):
                return jsonify({"statusCode": 409, "message": "MPIN already exists", "response": {}})
        if len(mpin) != 4 or not mpin.isdigit():
            return jsonify({"statusCode": 400, "message": "MPIN must be exactly 4 digits", "response": {}})

        if mobile:
            #mobile_strip = strip_plus91_basic(mobile)
            #result_login = db.user_mobile_login.find_one({'mobile': mobile_strip}, {"_id": 0})
            result = db.user_master.find_one({'mobile': mobile}, {"_id": 0})
            user_id = result.get('userid', None)
            # db.user_mpin.insert_one({'mobile': mobile}, {'mpin': mpin,
            #                          'userid': user_id, "user_type": "3",
            #                          "ismpinsetup": True, "isprofilesetup": False})
            db.user_master.update_one(
                {'mobile': mobile},
                {'$set': {'mpin': mpin,"user_type": "3","ismpinsetup": True}})
            result = db.user_master.find_one({"mobile": mobile}, {"mpin": 1, "ismpinsetup": 1, "isprofilesetup": 1})
            return jsonify({"statusCode": 200,
                            "message": "MPIN created successfully",
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
            # db.user_mpin.insert_one({'mail': mail, 'mpin': mpin,
            #                          'userid': user_id, "user_type": "3",
            #                          "ismpinsetup": True, "isprofilesetup": False})
            db.user_master.update_one(
                {'mail': mail},
                {'$set': {'mpin': mpin, "user_type": "3", "ismpinsetup": True}})
            result = db.user_master.find_one({"mail": mail}, {"mpin": 1, "ismpinsetup": 1, "isprofilesetup": 1})
            return jsonify({"statusCode": 200,
                            "message": "MPIN created successfully",
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
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})


# ------------------------------
# MPIN: Generate_otp
# ------------------------------
@login_bp.route("/api/mpin_generate_otp", methods=["POST"])
# @jwt_required()
def mpin_generate_otp():
    try:
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)

        if not all([login_type, mobile or mail]):
            return jsonify({"statusCode": 400, "message": "All fields are required", "response": {}})
        if login_type == "1" and mobile:
            return jsonify({"statusCode": 200, "message": "OTP generated successfully on mobile", "response": {}})
        elif login_type == "2" and mail:
            return jsonify({"statusCode": 200, "message": "OTP generated successfully on mail", "response": {}})
        else:
            return jsonify({"statusCode": 400, "message": "OTP generation failed", "response": {}})

    except Exception as e:
        print(f"Error in verify_otp: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})


# ------------------------------
# MPIN Verify OTP
# ------------------------------
@login_bp.route("/api/mpin_verify_otp", methods=["POST"])
@jwt_required()
def mpin_verify_otp():
    try:
        login_type = request.json.get("login_type", None)
        mobile = request.json.get("mobile", None)
        mail = request.json.get("mail", None)
        otp = request.json.get("otp", None)

        if not login_type:
            return jsonify({"statusCode": 400, "message": "Login type is required", "response": {}})
        if not otp:
            return jsonify({"statusCode": 400, "message": "OTP is required", "response": {}})

        if login_type == "1":
            if not mobile or not otp:
                return jsonify({"statusCode": 400, "message": "Mobile number and otp is required", "response": {}})
            # else:
            #     user_mobile_number = db.user_master.find_one({"mobile": mobile})

            if otp == "1234":
                result = db.user_master.find_one({"mobile": mobile}, {"_id": 0})
                if result:
                    return jsonify({
                        "statusCode": 200,
                        "message": "OTP verified successfully",
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
                        "message": "OTP verification failed",
                        "response": {},

                    })

            else:
                return jsonify({"statusCode": 401, "message": "Incorrect OTP, please try again.", "response": {}})

        elif login_type == "2":
            if not mail or not otp:
                return jsonify({"statusCode": 400, "message": "Mail is required", "response": {}})
            # else:
            #     user_mail = db.user_mpin.find_one({"mail": mail})
            if otp == "1234":
                result = db.user_master.find_one({"mail":mail}, {"_id":0})
                if result:
                    return jsonify({
                        "statusCode": 200,
                        "message": "OTP verified successfully",
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
                        "message": "OTP verification failed",
                        "response": {}})

            else:
                return jsonify({"statusCode": 401, "message": "Incorrect OTP, please try again.", "response": {}})

        else:
            return jsonify({"statusCode": 400, "message": "Invalid login type", "response": {}})

    except Exception as e:
        print(f"Error in verify_otp: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})


# ------------------------------
# MPIN: Verify
# ------------------------------
@login_bp.route('/api/verify_mpin', methods=['POST'])
@jwt_required(optional=True)
def verify_mpin():
    try:
        body = request.get_json(silent=True) or {}

        login_type = body.get('login_type')
        mobile = body.get('mobile')
        mail = body.get('mail')
        mpin = body.get('mpin')

        access_token = request.headers.get("access_token")

        if not all([login_type, (mobile or mail), mpin]):
            return jsonify({
                "statusCode": 400,
                "message": "All fields are required",
                "response": {}
            })

        # -------------------- MOBILE --------------------
        if login_type == "1" and mobile:
            if access_token is None:
                access_token = create_access_token(identity=mobile)

            result = db.user_master.find_one({"mobile": mobile}, {"_id": 0})
            if not result:
                return jsonify({
                    "statusCode": 400,
                    "message": "MPIN verification failed",
                    "response": {}
                })

            isprofilesetup = result.get('isprofilesetup')
            user_id = result.get('userid')

            # -----------------------------------------
            # PROFILE SETUP ALREADY DONE → OLD FLOW
            # -----------------------------------------
            if isprofilesetup is True:
                if result.get('mpin') == mpin:
                    user_type = result.get('user_type')
                    user_details = db.user_master.find_one(
                        {"userid": user_id},
                        {"_id": 0, "created_at": 0, "updated_at": 0}
                    )
                    user_details_mapped = master_mapping(user_details)
                    user_details_mapped["user_type"] = user_type

                    return jsonify({
                        "statusCode": 200,
                        "message": "MPIN verified successfully",
                        "response": {
                            "details": user_details_mapped,
                            "access_token": access_token,
                            "ismpinsetup": result.get("ismpinsetup"),
                            "isprofilesetup": result.get("isprofilesetup")
                        }
                    })

                return jsonify({
                    "statusCode": 400,
                    "message": "MPIN verification failed",
                    "response": {}
                })

            # =========================================
            # PROFILE NOT SETUP → DATA DIRECTLY FRONTEND SE LO
            # =========================================
            address_body = body.get("address", {}) or {}
            comm_address_body = body.get("communicationaddress", {}) or {}

            details = {
                "userid": user_id,
                "profileid": body.get("profileid", ""),
                "name": body.get("name", ""),
                "nickname": body.get("nickname", ""),
                "userHandle": body.get("userHandle", ""),
                "isaadharverified": body.get("isaadharverified", False),
                "isCommunicationaddressadded": body.get("isCommunicationaddressadded", False),

                # ADDRESS ALWAYS RETURN, EVEN EMPTY
                "address": {
                    "addresslineOne": address_body.get("addresslineOne", ""),
                    "addresslineTwo": address_body.get("addresslineTwo", ""),
                    "City":          address_body.get("City", ""),
                    "State":         address_body.get("State", ""),
                    "pincode":       address_body.get("pincode", ""),
                    # 👉 mobilenumber ONLY from payload (address.mobilenumber or top-level mobile)
                    "mobilenumber":  address_body.get("mobilenumber", "") or mobile or ""
                },

                "communicationaddress": {
                    "addresslineOne": comm_address_body.get("addresslineOne", ""),
                    "addresslineTwo": comm_address_body.get("addresslineTwo", ""),
                    "City":           comm_address_body.get("City", ""),
                    "State":          comm_address_body.get("State", ""),
                    "pincode":        comm_address_body.get("pincode", "")
                },

                "image": body.get("image", ""),
                "gender": body.get("gender", {"code": "", "name": ""}),
                "interestWith": body.get("interestWith", []),
                "preferredLanguage": body.get(
                    "preferredLanguage",
                    {"code": "", "native": ""}
                ),
                "user_type": body.get("user_type", result.get("user_type", ""))
            }

            return jsonify({
                "statusCode": 200,
                "message": "MPIN verified successfully",
                "response": {
                    "details": details,
                    "access_token": access_token,
                    "ismpinsetup": result.get("ismpinsetup", False),
                    "isprofilesetup": result.get("isprofilesetup", False)
                }
            })

        # -------------------- MAIL --------------------
        elif login_type == "2" and mail:
            if access_token is None:
                access_token = create_access_token(identity=mail)

            result = db.user_master.find_one({"mail": mail}, {"_id": 0})
            if not result:
                return jsonify({
                    "statusCode": 400,
                    "message": "MPIN verification failed",
                    "response": {}
                })

            isprofilesetup = result.get('isprofilesetup')
            user_id = result.get('userid')

            # -----------------------------------------
            # PROFILE SETUP ALREADY DONE → OLD FLOW
            # -----------------------------------------
            if isprofilesetup is True:
                if result.get('mpin') == mpin:
                    user_type = result.get('user_type')
                    user_details = db.user_master.find_one(
                        {"userid": user_id},
                        {"_id": 0, "created_at": 0, "updated_at": 0}
                    )
                    user_details_mapped = master_mapping(user_details)
                    user_details_mapped["user_type"] = user_type

                    return jsonify({
                        "statusCode": 200,
                        "message": "MPIN verified successfully",
                        "response": {
                            "details": user_details_mapped,
                            "access_token": access_token,
                            "ismpinsetup": result.get("ismpinsetup"),
                            "isprofilesetup": result.get("isprofilesetup")
                        }
                    })

                return jsonify({
                    "statusCode": 400,
                    "message": "MPIN verification failed",
                    "response": {}
                })

            # =========================================
            # PROFILE NOT SETUP → DATA DIRECTLY FRONTEND SE LO
            # =========================================
            address_body = body.get("address", {}) or {}
            comm_address_body = body.get("communicationaddress", {}) or {}

            details = {
                "userid": user_id,
                "profileid": body.get("profileid", ""),
                "name": body.get("name", ""),
                "nickname": body.get("nickname", ""),
                "userHandle": body.get("userHandle", ""),
                "isaadharverified": body.get("isaadharverified", False),
                "isCommunicationaddressadded": body.get("isCommunicationaddressadded", False),

                "address": {
                    "addresslineOne": address_body.get("addresslineOne", ""),
                    "addresslineTwo": address_body.get("addresslineTwo", ""),
                    "City":          address_body.get("City", ""),
                    "State":         address_body.get("State", ""),
                    "pincode":       address_body.get("pincode", ""),
                    # yahan bhi wahi rule — payload se hi mobile
                    "mobilenumber":  address_body.get("mobilenumber", "") or mobile or ""
                },

                "communicationaddress": {
                    "addresslineOne": comm_address_body.get("addresslineOne", ""),
                    "addresslineTwo": comm_address_body.get("addresslineTwo", ""),
                    "City":           comm_address_body.get("City", ""),
                    "State":          comm_address_body.get("State", ""),
                    "pincode":        comm_address_body.get("pincode", "")
                },

                "image": body.get("image", ""),
                "gender": body.get("gender", {"code": "", "name": ""}),
                "interestWith": body.get("interestWith", []),
                "preferredLanguage": body.get(
                    "preferredLanguage",
                    {"code": "", "native": ""}
                ),
                "user_type": body.get("user_type", result.get("user_type", ""))
            }

            return jsonify({
                "statusCode": 200,
                "message": "MPIN verified successfully",
                "response": {
                    "details": details,
                    "access_token": access_token,
                    "ismpinsetup": result.get("ismpinsetup", False),
                    "isprofilesetup": result.get("isprofilesetup", False)
                }
            })

        # INVALID LOGIN TYPE
        return jsonify({
            "statusCode": 401,
            "message": "MPIN verification failed",
            "response": {}
        })

    except Exception as e:
        print("Error in verify_mpin:", e)
        return jsonify({
            "statusCode": 500,
            "message": "Internal server error",
            "response": {}
        })



# ------------------------------
# Regenerate MPIN
# ------------------------------
@login_bp.route('/api/regenerate_mpin', methods=['POST'])
@jwt_required()
def regenerate_mpin():
    try:
        login_type = request.json.get('login_type', None)
        mobile = request.json.get('mobile', None)
        mail = request.json.get('mail', None)
        mpin = request.json.get('mpin', None)

        if not all([login_type, mobile or mail, mpin]):
            return jsonify({"statusCode": 400, "message": "All fields are required", "response": {}})

        if len(mpin) != 4 or not mpin.isdigit():
            return jsonify({"statusCode": 400, "message": "MPIN must be exactly 4 digits", "response": {}})

        if mobile and login_type == "1":
            # result_mpin = db.user_mpin.find_one({"mobile": mobile}, {"userid": 1, "user_type": 1})
            # user_id = result_mpin.get('userid', None)
            # user_type = result_mpin.get('user_type', None)
            # db.user_mpin.delete_many({'mobile': mobile})
            # db.user_mpin.insert_one({'mobile': mobile, 'mpin': mpin,
            #                          'userid': user_id, "user_type": user_type,
            #                          "ismpinsetup": True, "isprofilesetup": False})
            db.user_master.update_one({'mobile': mobile}, {'$set': {'mpin': mpin}})
            return jsonify(
                {"statusCode": 200, "message": "MPIN regenerated successfully", "response": {"mobile": mobile}})
        elif mail and login_type == "2":
            # result_mpin = db.user_mpin.find_one({"mail": mail}, {"userid": 1, "user_type": 1})
            # user_id = result_mpin.get('userid', None)
            # user_type = result_mpin.get('user_type', None)
            # db.user_mpin.delete_many({'mail': mail})
            # db.user_mpin.insert_one({'mail': mail, 'mpin': mpin,
            #                          'userid': user_id, "user_type": user_type,
            #                          "ismpinsetup": True, "isprofilesetup": False})
            db.user_master.update_one({'mail': mail}, {'$set': {'mpin': mpin}})
            return jsonify({"statusCode": 200, "message": "MPIN regenerated successfully", "response": {"mail": mail}})
        else:
            return jsonify({"statusCode": 401, "message": "MPIN regeneration failed", "response": {}})

    except Exception as e:
        print(f"Error in regenerate_mpin: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})