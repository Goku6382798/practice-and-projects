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

        print("otp will get generated")

        return jsonify({"statusCode": 200, "message": "OTP generated successfully.", "response": {}})

    except Exception as e:
        print(f"Error in aadhar_generate_otp: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})





@aadhar_bp.route("/api/aadhar_verify_otp", methods=["POST"])
@jwt_required(optional=True)
def aadhar_verify_otp():
    try:
        user_id = request.headers.get('userId', None)
        aadhar_number = request.json.get('aadhar_number', None)
        aadhar_otp = request.json.get('aadhar_otp', None)
        print(user_id)

        if aadhar_otp == "123456":
            name, dob, gender, address = aadhar_details()
            db.user_master.update_one(
                {"userid": user_id},
                {
                    "$set": {
                        "name": name,
                        "dob": dob,
                        "gender": gender,
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
                    "message": "aadhar otp verified",
                    "response":
                        {"details": user_details_mapped,
                         "ismpinsetup": user_details.get('ismpinsetup', False),
                         "isprofilesetup": user_details.get('isprofilesetup', False)
                         },
                        })
    except Exception as e:
        print(f"Error in aadhar: {e}")
        return jsonify({"statusCode": 500, "message": "Internal server error", "response": {}})

