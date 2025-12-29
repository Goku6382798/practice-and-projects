from flask import Blueprint, request, jsonify
from ETL.ETL_utils_ import db

user_bp = Blueprint("user", __name__)

@user_bp.route("/api/user/communication_address", methods=["POST"])
def communication_address():
    userid = request.headers.get("userid")

    if not userid or userid.strip() == "":
        return jsonify({
            "statusCode": 400,
            "message": "userid is required in headers",
            "response": {}
        })

    body = request.get_json(silent=True) or {}

    use_same = body.get("use_same_address")

    if use_same is None:
        return jsonify({
            "statusCode": 400,
            "message": "use_same_address is required in body",
            "response": {}
        })

    user_doc = db.user_master.find_one(
        {"userid": userid},
        {"_id": 0, "address": 1}
    )

    if not user_doc:
        return jsonify({
            "statusCode": 400,
            "message": "Invalid userid",
            "response": {}
        })

    # -----------------------------
    # OPTION A – USE SAME ADDRESS
    # -----------------------------
    if str(use_same).lower() == "true":
        perm_addr = user_doc.get("address")

        if not perm_addr:
            return jsonify({
                "statusCode": 400,
                "message": "User permanent address not found",
                "response": {}
            })

        db.user_master.update_one(
            {"userid": userid},
            {
                "$set": {
                    "communicationaddress": perm_addr,
                    "isCommunicationaddressadded": True
                }
            }
        )

        return jsonify({
            "statusCode": 200,
            "message": "Communication address set to permanent address",
            "response": perm_addr
        })

    # -----------------------------
    # OPTION B – ADD NEW ADDRESS
    # -----------------------------
    required = ["address_line1", "address_line2", "city", "state", "pincode"]
    missing = [f for f in required if not body.get(f)]

    if missing:
        return jsonify({
            "statusCode": 400,
            "message": f"Missing fields: {', '.join(missing)}",
            "response": {}
        })

    new_addr = {
        "addresslineOne": body.get("address_line1"),
        "addresslineTwo": body.get("address_line2", ""),
        "City": body.get("city"),
        "State": body.get("state"),
        "pincode": body.get("pincode")
    }

    db.user_master.update_one(
        {"userid": userid},
        {
            "$set": {
                "communicationaddress": new_addr,
                "isCommunicationaddressadded": True
            }
        }
    )

    return jsonify({
        "statusCode": 200,
        "message": "New communication address saved",
        "response": new_addr
    })