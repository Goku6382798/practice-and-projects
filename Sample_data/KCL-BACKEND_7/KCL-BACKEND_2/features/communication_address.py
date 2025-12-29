from flask import Blueprint, request, jsonify
from ETL.ETL_utils_ import db

communication_bp = Blueprint("communication", __name__)


@communication_bp.route("/api/communication_address", methods=["POST"])
def communication_address():
    try:
        userid = request.headers.get("userid", None)
        body = request.json or {}

        if not userid:
            return jsonify({
                "statusCode": 400,
                "message": "userid is required in headers",
                "response": {}
            })

        user_doc = db.user_master.find_one({"userid": userid})

        if not user_doc:
            return jsonify({
                "statusCode": 401,
                "message": "Invalid userid",
                "response": {}
            })

        use_existing = body.get("useExistingAddress", None)

        if use_existing is None:
            return jsonify({
                "statusCode": 400,
                "message": "useExistingAddress is required in request body (true/false)",
                "response": {}
            })

        update_fields = {}

        # CASE 1: Use existing address

        if bool(use_existing) is True:
            base_address = user_doc.get("address")

            if not base_address:
                return jsonify({
                    "statusCode": 400,
                    "message": "Base address not found for this user",
                    "response": {}
                })

            communication_address = {
                "addresslineOne": base_address.get("addresslineOne", ""),
                "addresslineTwo": base_address.get("addresslineTwo", ""),
                "City": base_address.get("City", ""),
                "State": base_address.get("State", ""),
                "pincode": base_address.get("pincode", ""),
                "mobilenumber": base_address.get("mobilenumber", "")
            }

            update_fields["communicationaddress"] = communication_address
            update_fields["isCommunicationaddressadded"] = True

        # CASE 2: Add new communication address
        else:
            addressline_one = (body.get("addresslineOne") or "").strip()
            addressline_two = (body.get("addresslineTwo") or "").strip()
            city = (body.get("City") or "").strip()
            state = (body.get("State") or "").strip()
            pincode = (body.get("pincode") or "").strip()

            if not addressline_one or not city or not state or not pincode:
                return jsonify({
                    "statusCode": 400,
                    "message": "addresslineOne, City, State and pincode are required for communication address",
                    "response": {}
                })

            communication_address = {
                "addresslineOne": addressline_one,
                "addresslineTwo": addressline_two,
                "City": city,
                "State": state,
                "pincode": pincode
            }

            update_fields["communicationaddress"] = communication_address
            update_fields["isCommunicationaddressadded"] = True

        db.user_master.update_one(
            {"userid": userid},
            {"$set": update_fields}
        )

        updated_user = db.user_master.find_one(
            {"userid": userid},
            {
                "_id": 0,
                "userid": 1,
                "isaadharverified": 1,
                "isCommunicationaddressadded": 1,
                "address": 1,
                "communicationaddress": 1,
            }
        )

        return jsonify({
            "statusCode": 200,
            "message": "Communication address updated successfully",
            "response": {
                "details": updated_user
            }
        })

    except Exception as e:
        return jsonify({
            "statusCode": 500,
            "message": "Internal server error",
            "response": {}
        })
