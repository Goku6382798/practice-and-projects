from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from ETL.ETL_utils_ import db
import pandas as pd

profile_setup_bp = Blueprint("categories", __name__)

@profile_setup_bp.route("/api/profile_setup", methods=["POST"])
@jwt_required(optional=True)
def get_categories():
    try:
        lang = (request.json or {}).get("language", "en").strip().lower()

        language = db.prefered_language_master.find_one({"code": lang},{"_id": 0, "name": 1})

        lan = []
        if language and "name" in language:
            for code, value in language["name"].items():
                lan.append({"code": code, "name": value})

        gen = list(db.gender_master.find({}, {"_id": 0}))
        inte = list(db.interest_master.find({}, {"_id": 0}))

        # 4) Localize the "name" field using the picked language, fallback to en -> base name
        def localize(items):
            out = []
            for it in items:
                localized = it.get(lang) or it.get("en")
                try:
                    out.append({"code": it.get("code"),"image": it.get("image"), "name": localized})
                except:
                    out.append({"code": it.get("code"), "name": localized})

            return out

        gen_loc = localize(gen)
        inte_loc = localize(inte)

        return jsonify({
            "statusCode": 200,
            "message": "Success",
            "response": {
                "languages": lan,
                "gender": gen_loc,
                "interests": inte_loc
            }
        })
    except Exception as e:
        print(f"Error in /api/categories: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})
