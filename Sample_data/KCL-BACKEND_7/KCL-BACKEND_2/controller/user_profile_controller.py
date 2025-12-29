# in your existing user_profile.py
import base64
import json
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from datetime import datetime
import uuid
from controller.language_controller import translate_dict, translate_message

from ETL.ETL_utils_ import db, upload_to_s3

from controller.login_controller import blank_address

def strip_plus91_basic(number: str) -> str:
    s = number.strip().replace(" ", "").replace("-", "")
    if s.startswith("+"):
        s = s[1:]
    if s.startswith("91") and len(s) > 10:
        s = s[2:]
    else:
        s = "+91" + s
    return s

def _user_exists(user_id: str) -> bool:
    if not user_id:
        return False
    if db.user_master.find_one({"userid": user_id}, {"_id": 1}): return True
    return False

# ---------- helpers ----------
def _get_user_contact_and_flags(user_id: str):
    mobile = ""
    mail = ""
    isprofilesetup = False
    ismpinsetup = False

    #mpin = db.user_mpin.find_one({"userid": user_id}, {"_id": 0, "mobile": 1, "mail": 1, "isprofilesetup": 1, "ismpinsetup": 1})
    user_data = db.user_master.find_one({"userid": user_id}, {"_id": 0})
    #if mpin:
    if user_data:
        mobile = user_data.get("mobile", "") or mobile
        mail = user_data.get("mail", "") or mail
        isprofilesetup = bool(user_data.get("isprofilesetup", isprofilesetup))
        ismpinsetup = bool(user_data.get("ismpinsetup", ismpinsetup))

    #mob = db.user_mobile_login.find_one({"userid": user_id}, {"_id": 0, "mobile": 1, "isprofilesetup": 1})
    #if mob:
    if user_data.get('ismobileregister') == True:
        mobile = user_data.get("mobile", "") or mobile
        isprofilesetup = bool(user_data.get("isprofilesetup", isprofilesetup))

    #em = db.user_mail_login.find_one({"userid": user_id}, {"_id": 0, "mail": 1, "isprofilesetup": 1})
    #if em:
    elif user_data.get('ismailregister') == True:
        mail = user_data.get("mail", "") or mail
        isprofilesetup = bool(user_data.get("isprofilesetup", isprofilesetup))

    return {
        "mobile": mobile or "",
        "mail": mail or "",
        "isprofilesetup": bool(isprofilesetup),
        "ismpinsetup": bool(ismpinsetup),
    }

def _gender_from_code(code: str, lang: str = "en"):
    if not code:
        return {"name": "", "code": ""}
    doc = db.gender_master.find_one({"code": code}, {"_id": 0})
    if not doc:
        return {"name": "", "code": code}
    name = doc.get(lang) or doc.get("en") or doc.get("name") or ""
    return {"name": name, "code": code}

def _language_from_code(code: str):
    if not code:
        return {"native": "", "code": ""}
    doc = db.standard_state_ut_language.find_one({"code": code}, {"_id": 0})
    native = (doc or {}).get("native", "")
    return {"native": native, "code": code}

def _interests_from_codes(codes, lang: str = "en"):
    out = []
    if not isinstance(codes, list):
        return out
    for c in codes:
        if not c:
            continue
        doc = db.interest_master.find_one({"code": c}, {"_id": 0})
        name = (doc or {}).get(lang) or (doc or {}).get("en") or (doc or {}).get("name") or ""
        out.append({"name": name, "code": c})
    return out
communicationaddress, address = blank_address()

def _build_profile_details(doc,languageCode, lang: str = "en"):
    if not doc:
        return {}
    return {
        "profileid": doc.get("profileid", ""),
        "userid": doc.get("userid", ""),
        "profileImage": doc.get("profileImage", ""),  # fixed key
        "name": translate_message(doc.get("name", ""),languageCode),
        "nickname": doc.get("nickname", ""),
        "userHandle": doc.get("userHandle", ""),
        "gender": _gender_from_code(doc.get("gender", ""), lang),
        "preferredLanguage": _language_from_code(doc.get("preferredLanguage", "")),
        "interestWith": _interests_from_codes(doc.get("interestWith", []), lang),
        "mobile": doc.get("mobile", ""),
        "mail": doc.get("mail", ""),
        "communicationaddress" : translate_dict(doc.get('communicationaddress', communicationaddress),languageCode),
        "address" : translate_dict(doc.get('address', address),languageCode),
        "dob": doc.get("dob", ""),
        "user_type": doc.get("user_type", ""),
        "mpin": doc.get("mpin", ""),

        "ismobileregister": doc.get("ismobileregister", False),
        "ismailregister": doc.get("ismailregister", False),
        "ismpinsetup": doc.get("ismpinsetup", False),
        "isprofilesetup": doc.get("isprofilesetup", False),
        "isaadharverified": doc.get("isaadharverified", False),
        "isCommunicationaddressadded": doc.get("isCommunicationaddressadded", False)
    }
# -----------------------------
