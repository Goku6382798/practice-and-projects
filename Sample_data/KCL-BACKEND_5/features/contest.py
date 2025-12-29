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
from controller.contest_controller import pick, valid_lang, map_title_list


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

contest_bp = Blueprint("contest", __name__)

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

contest_bp = Blueprint("contest", __name__)




@contest_bp.route("/api/contest_this_season", methods=["POST"])
def contest_this_season():
    contest_id = request.json.get("contest_id", None)
    language_code= valid_lang(request.headers.get("languageCode", None))
    user_id = request.headers.get("userid", None)

    if language_code is None or language_code== "":
        return jsonify({
                "statusCode": 400,
                "message": "Language code is required in headers",
                "response": {}
            })

    if contest_id is None or str(contest_id).strip() == "":
        return jsonify({
            "statusCode": 400,
            "message": "contest_id is required in request body",
            "response": {}
        })

    contest_id_str = str(contest_id).strip()
    contest_id_int = int(contest_id_str) if contest_id_str.isdigit() else None
    id_filter = {"$in": [contest_id_str] + ([contest_id_int] if contest_id_int is not None else [])}


    #contest_info_lst = db.contest_info.find({"contest_id":contest_id},{"_id": 0, "contest_id": 1, "bannersList": 1, "dates": 1, "benefitsList": 1,"eligibleCriteriaList": 1, "contestFee": 1, "selectionProcessList": 1, "howToParticipate": 1})
    contest_info_lst = list(db.contest_info.find(
        {"contest_id": id_filter},
        {"_id": 0, "contest_id": 1, "bannersList": 1, "dates": 1, "benefitsList": 1,
         "eligibleCriteriaList": 1, "contestFee": 1, "selectionProcessList": 1, "howToParticipate": 1}
    ))

    '''
    contest_master_lst = {
        str(m.get("contest_id")): m.get("contest_name")
        for m in db.contest_master_copy.find({"contest_id":contest_id}, {"_id": 0, "contest_id": 1, "contest_name": 1})
    }
    '''
    contest_master_lst = {
        str(m.get("contest_id")): m.get("contest_name")
        for m in db.contest_master_copy.find(
            {"contest_id": id_filter},
            {"_id": 0, "contest_id": 1, "contest_name": 1}
        )
    }
    if not contest_info_lst:
        return jsonify({
            "statusCode": 400,
            "message": "Invalid contest_id",
            "response": {}
        })

    userid = user_id
    '''user_exist = db.user_master.find_one({"userid": userid}, {"_id": 1})
    if not user_exist:
        return jsonify({
            "statusCode": 401,
            "message": "Invalid userid",
            "response": {}
        })'''

    # 1) Get user from user_master using mobile (if provided)
    user_doc = db.user_master.find_one(
        {"userid": userid},
        {"_id": 0, "isaadharverified": 1, "isCommunicationaddressadded": 1}
    )

    isaadharverified = bool(user_doc.get("isaadharverified", False)) if user_doc else False
    isCommunicationaddressadded = bool(user_doc.get("isCommunicationaddressadded", False)) if user_doc else False

    # 2) Get contest mapping flags from user_contest_mapping using userid + contest_id
    mapping_doc = db.user_contest_mapping.find_one(
        {"contest_id": contest_id_str, "userid": userid},
        {"_id": 0, "isPaymentDone": 1, "isEligible": 1}
    )

    isPaymentDone = bool(mapping_doc.get("isPaymentDone", False)) if mapping_doc else False
    isEligible = bool(mapping_doc.get("isEligible", False)) if mapping_doc else False

    data = []
    for d in contest_info_lst:
        cid = d.get("contest_id")

        if not db.contest_master_copy.find_one({"contest_id": {"$in": [contest_id_str, int(contest_id_str)]}}):
            return jsonify({
                "statusCode": 400,
                "message": f"Invalid contest_id: {contest_id}",
                "response": {}
            })

        dates = d.get("dates", {}) or {}
        fee = d.get("contestFee", {}) or {}
        cname_dict = contest_master_lst.get(str(cid))

        data.append({
            "contest_id": d.get("contest_id"),
            "bannersList": d.get("bannersList", []),
            "contest": pick(cname_dict, language_code),
            "dates": {
                "startDate": pick(dates.get("startDate"), language_code),
                "endDate": pick(dates.get("endDate"), language_code)
            },
            "benefitsList": map_title_list(d.get("benefitsList"), language_code),
            "eligibleCriteriaList": map_title_list(d.get("eligibleCriteriaList"), language_code),
            "selectionProcessList": map_title_list(d.get("selectionProcessList"), language_code),
            "contestFee": {
                "isLimitedOfferTime": bool(fee.get("isLimitedOfferTime", False)),
                "actualPrice": fee.get("actualPrice", ""),
                "afterDiscountPrice": fee.get("afterDiscountPrice", ""),
                "title": pick(fee.get("title"), language_code),
                "validateDate": pick(fee.get("validateDate"), language_code)
            },
            "howToParticipate": d.get("howToParticipate", {})
        })

    return jsonify({
        "statusCode": 200,
        "message": "success",
        "response": {
            "isaadharverified": isaadharverified,
            "isEligible": isEligible,
            "isCommunicationaddressadded": isCommunicationaddressadded,
            "isPaymentDone": isPaymentDone,
            "contestData": (data[0] if data else {})
        }
    })
