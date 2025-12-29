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
from ETL.ETL_utils_ import db, conn

def master_mapping(user_details):
    # ------------- GENDER MAPPING -------------
    gender_code = user_details.get("gender")
    if gender_code:
        gender_master = db.gender_master.find_one(
            {"code": gender_code},
            {"_id": 0, "code": 1, "en": 1}
        )
        if gender_master:
            user_details["gender"] = {
                "code": gender_master["code"],
                "name": gender_master["en"]
            }

    # ------------- INTERESTS MAPPING -------------
    interest_codes = user_details.get("interestWith", [])
    if interest_codes:
        interests_master = list(
            db.interest_master.find(
                {"code": {"$in": interest_codes}},
                {"_id": 0, "code": 1, "en": 1}
            )
        )
        user_details["interestWith"] = [
            {"code": i["code"], "name": i["en"]}
            for i in interests_master
        ]

    # ------------- LANGUAGE MAPPING -------------
    preferred_lang = user_details.get("preferredLanguage")
    if preferred_lang:
        lang_master = db.standard_state_ut_language.find_one(
            {"code": preferred_lang},
            {"_id": 0, "code": 1, "native": 1}
        )
        if lang_master:
            user_details["preferredLanguage"] = {
                "code": lang_master["code"],
                "native": lang_master["native"]
            }

    return user_details


def empty_mapping():
    details = {"gender": {
                          "code": "",
                          "name": "",
                          },
               "profileImage": "",
               "interestWith": [
                          {
                           "code": "",
                           "name": ""
                        },
                        {
                        "code": "",
                        "name": ""
                        }
                               ],
               "name": "",
               "nickname": "",
               "preferredLanguage": {
                                        "code": "",
                                        "native": ""
                                    },
               "profileid": "",
               "userHandle": ""}
    return details




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
                    "ismailregister": False,
                    "isaadharverified": False,
                    "isCommunicationaddressadded": False,
                    "communicationaddress":
                        {"addresslineOne": "",
                         "addresslineTwo":"",
                         "City": "",
                         "State": "",
                         "pincode": ""},
                    "address":
                        {"addresslineOne": "",
                         "addresslineTwo": "",
                         "City":"",
                         "State":"",
                         "pincode":"",
                         "mobilenumber":""}

                })
            if login_type == "2":
                db.user_master.insert_one({
                    "userid": userid,
                    "user_type": user_type,
                    "mail": mail_mob,
                    "isprofilesetup": False,
                    "ismpinsetup":False,
                    "ismobileregister": False,
                    "ismailregister": True,
                    "isaadharverified": False,
                    "isCommunicationaddressadded": False,
                    "communicationaddress":
                        {"addresslineOne": "",
                         "addresslineTwo": "",
                         "City": "",
                         "State": "",
                         "pincode": ""},
                    "address":
                        {"addresslineOne": "",
                         "addresslineTwo": "",
                         "City": "",
                         "State": "",
                         "pincode": "",
                         "mobilenumber": ""}

                })
            break
        except pymongo.errors.DuplicateKeyError:
            print("entered except")
            # Try again if duplicate
            continue


def blank_address():
    communicationaddress = \
        {
            "addresslineOne": "",
            "addresslineTwo": "",
            "City": "",
            "State": "",
            "pincode": ""
        }

    address = \
        {
            "addresslineOne": "",
            "addresslineTwo": "",
            "City": "",
            "State": "",
            "pincode": "",
            "mobilenumber": ""
        }
    return communicationaddress, address

