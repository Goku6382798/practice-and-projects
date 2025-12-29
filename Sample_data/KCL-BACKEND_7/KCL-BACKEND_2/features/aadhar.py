from flask import Flask, Blueprint, request, jsonify
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity
from dotenv import load_dotenv
from ETL.ETL_utils_ import db
from controller.login_controller import master_mapping, blank_address
from controller.user_profile_controller import address
from controller.aadhar_controller import aadhar_details
# Load environment variables
from datetime import datetime
from controller.language_controller import translate_message
import re
load_dotenv()

# Flask App Setup

app = Flask(__name__)
CORS(app)


SWAGGER_URL = '/swagger'
API_URL = '/static/root.yaml'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "KCL Flask API"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

aadhar_bp = Blueprint("aadhar", __name__)



@aadhar_bp.route("/api/aadhar_generate_otp", methods=["POST"])
@jwt_required(optional=True)
def aadhar_generate_otp():
    try:
        user_id = request.headers.get('user_id', None)
        aadhar_number = request.json.get('aadhar_number', None)
        languageCode = request.headers.get("languageCode", "en")

        print("otp will get generated")

        return jsonify({"statusCode": 200,
                        "message": translate_message("OTP generated successfully.", languageCode),
                        "response": {}})

    except Exception as e:
        print(f"Error in aadhar_generate_otp: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error",languageCode),
                        "response": {}})





@aadhar_bp.route("/api/aadhar_verify_otp", methods=["POST"])
@jwt_required(optional=True)
def aadhar_verify_otp():
    try:
        user_id = request.headers.get('userId', None)
        aadhar_number = request.json.get('aadhar_number', None)
        aadhar_otp = request.json.get('aadhar_otp', None)
        languageCode = request.headers.get("languageCode", "en")

        if aadhar_otp == "123456":
            result = aadhar_details()

            # -----------------------------
            # FAILURE CASE: No Aadhaar data
            # -----------------------------
            if not result or len(result) == 0:
                return jsonify({
                    "statusCode": 400,
                    "message": translate_message("Aadhar verification failed. Details not found.", languageCode),
                    "response": {}
                }), 400

            name, dob, gender, address = result
            dob_obj = datetime.strptime(dob, "%d/%m/%Y")
            age = (datetime.now() - dob_obj).days // 365

            # gender returned as string like "male"
            # gender returned by aadhar_details()
            if isinstance(gender, tuple):
                gender = gender[0]  # take first value

            gender_value = str(gender).strip().lower()

            # case-insensitive match in gender master
            gender_data = db.gender_master.find_one(
                {"en": {"$regex": f"^{gender_value}$", "$options": "i"}},
                {"_id": 0, "code": 1}
            )

            gender_code = gender_data["code"] if gender_data else None

            db.user_master.update_one(
                {"userid": user_id},
                {
                    "$set": {
                        #"name": name,
                        "dob": dob,
                        "age": age,
                        "gender": gender_code,
                        # address
                        "address.addresslineOne": address["addresslineOne"],
                        "address.addresslineTwo": address["addresslineTwo"],
                        "address.City": address["City"],
                        "address.State": address["State"],
                        "address.pincode": address["pincode"],
                        "address.mobilenumber": address['mobilenumber'],
                        "isaadharverified" :True
                    }
                }
            )
            user_details = db.user_master.find_one(
                {"userid": user_id},
                {"_id": 0, 'created_at': 0, 'updated_at': 0}
            )
            if user_details:
                user_details_mapped = master_mapping(user_details)
                user_details_mapped['user_type'] = user_details.get('user_type', False)
                user_details_mapped["isaadharverified"] = user_details.get('isaadharverified', False)
                user_details_mapped["isCommunicationaddressadded"] = user_details.get('isCommunicationaddressadded', False)
                communicationaddress, address = blank_address()
                user_details_mapped["communicationaddress"] = user_details.get('communicationaddress', communicationaddress)
                user_details_mapped["address"] = user_details.get('address', address)
                user_details_mapped["dob"] = user_details.get('dob', "")

                return jsonify({
                    "statusCode": 200,
                    "message": translate_message("aadhar otp verified",languageCode),
                    "response":
                        {"details": user_details_mapped,
                         "ismpinsetup": user_details.get('ismpinsetup', False),
                         "isprofilesetup": user_details.get('isprofilesetup', False)
                         },
                        })
        else:
            return jsonify({"statusCode": 400,
                            "message": translate_message("OTP Verification failed.", languageCode),
                            "response": {}})
    except Exception as e:
        print(f"Error in aadhar: {e}")
        return jsonify({"statusCode": 500,
                        "message": translate_message("Internal server error", languageCode),
                        "response": {}})


@aadhar_bp.route("/api/aadhar_eligibility", methods=["POST"])
@jwt_required(optional=True)
def aadhar_eligibility():
    try:
        languageCode = request.headers.get("languageCode", "en")
        user_id = request.headers.get("userId")
        contest_id = request.json.get("contest_id")
        print("contest_id")

        if not user_id or not contest_id:
            return jsonify({
                "statusCode": 400,
                "message": translate_message("Missing required fields", languageCode),
                "response": {}
            })

        # ---------------------- USER DETAILS ----------------------
        user_details = db.user_master.find_one(
            {"userid": user_id},
            {"_id": 0, "name": 1, "gender": 1, "age": 1, "address": 1, "isaadharverified":1}
        )

        if user_details.get('isaadharverified') is True:

            name = user_details.get("name", "")
            age = user_details.get("age", None)
            genderCode = user_details.get("gender", None)

            # Extract pincode
            pincode = user_details.get("address", {}).get("pincode")

            # ------------------ GET HUMAN READABLE GENDER ------------------
            gender_master = db.gender_master.find_one({"code": genderCode}, {"_id": 0})
            gender_str = gender_master.get("en", "")
            # ------------------ GET ACTIVE ELIGIBILITIES FOR CONTEST ------------------
            mapped_elig = list(db.eligibility_mapping.find(
                {"contest_id": str(contest_id), "is_active": True},
                {"_id": 0, "eligibility_id": 1}
            ))

            elig_ids = [e["eligibility_id"] for e in mapped_elig]

            eligibility_rules = list(db.eligibility_master.find(
                {"eligibility_id": {"$in": elig_ids}, "is_active": True},
                {"_id": 0}
            ))
            # ------------------ RESULT STRUCTURE ------------------
            response = {
                "name": translate_message(name, languageCode),
                "isEligible": True,

                "ageDetails": {
                    "age": translate_message(f"{age} years" if age else "", languageCode),
                    "isEligible": True,
                    "eligibility": ""
                },
                "genderDetails": {
                    "gender": gender_str,
                    "isEligible": True,
                    "eligibility": ""
                },
                "pincodeDetails": {
                    "pincode": pincode,
                    "isEligible": True,
                    "eligibility": ""
                },
                "errors": []
            }

            # ------------------ APPLY ELIGIBILITY RULES ---------------
            for rule in eligibility_rules:

                # ------------- AGE RULE -------------
                if rule["eligibility_name"] == "age":
                    min_age, max_age = rule["eligibility_value"]
                    if not (min_age <= age <= max_age):
                        translated_age_message = translate_message(rule["faliure_message"], languageCode)
                        response["ageDetails"]["isEligible"] = False
                        response["ageDetails"]["eligibility"] = translated_age_message
                        response["errors"].append(translated_age_message)

                # ------------- GENDER RULE -------------
                elif rule["eligibility_name"] == "gender":
                    required_gender = rule["eligibility_value"].strip().lower()
                    if gender_str.lower() != required_gender:
                        translated_gender_message = translate_message(rule["faliure_message"], languageCode)
                        response["genderDetails"]["isEligible"] = False
                        response["genderDetails"]["eligibility"] = translated_gender_message
                        response["errors"].append(translated_gender_message)

                # ------------- PINCODE RULE (RURAL CHECK) -------------
                elif rule["eligibility_name"] == "country":
                    if not pincode:
                        translated_pincode_message = translate_message(rule["faliure_message"], languageCode)
                        response["pincodeDetails"]["isEligible"] = False
                        response["pincodeDetails"]["eligibility"] = translated_pincode_message
                        response["errors"].append(translated_pincode_message)
                    else:
                        try:
                            pin = int(pincode)
                            records = list(db.pincode_master.find({"pincode": pin}, {"_id": 0}))
                            if not records:
                                translated_pincode_message = translate_message(rule["faliure_message"], languageCode)
                                response["pincodeDetails"]["isEligible"] = False
                                response["pincodeDetails"]["eligibility"] = translated_pincode_message
                                response["errors"].append(translated_pincode_message)
                            else:
                                has_rural = any(rec.get("rural(flag)", "").strip().lower() == "yes" for rec in records)
                                if not has_rural:
                                    translated_pincode_message = translate_message(rule["faliure_message"], languageCode)
                                    response["pincodeDetails"]["isEligible"] = False
                                    response["pincodeDetails"]["eligibility"] = translated_pincode_message
                                    response["errors"].append(translated_pincode_message)
                        except (ValueError, TypeError):
                            translated_pincode_message = translate_message(rule["faliure_message"], languageCode)
                            response["pincodeDetails"]["isEligible"] = False
                            response["pincodeDetails"]["eligibility"] = translated_pincode_message
                            response["errors"].append(translated_pincode_message)

            # ------------------ FINAL OVERALL ELIGIBILITY ------------------
            response["isEligible"] = all([
                response["ageDetails"]["isEligible"],
                response["genderDetails"]["isEligible"],
                response["pincodeDetails"]["isEligible"]
            ])
            final_status = 200 if response["isEligible"] else 401
            final_message = "You are eligible." if response["isEligible"] else "Sorry, you are not eligible."
            return jsonify({
                "statusCode": final_status,
                "message": translate_message(final_message, languageCode),
                "response": response
            })
        else:
            return jsonify({
                "statusCode": 400,
                "message": translate_message("Sorry, you are not eligible.", languageCode),
                "response": {}
            })

    except Exception as e:
        return jsonify({
            "statusCode": 500,
            "message": translate_message("Internal server error", languageCode),
            "response": {}
        })