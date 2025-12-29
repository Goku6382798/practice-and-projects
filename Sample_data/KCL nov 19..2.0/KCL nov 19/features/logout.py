# features/logout.py
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required, get_jwt

from controller.logout_controller import (
    resolve_userid_from_body,
    logout_current_session,
    logout_all_sessions,
)

logout_bp = Blueprint("logout_bp", __name__)


@logout_bp.route("/api/logout", methods=["POST"])
@jwt_required()
def logout():
    """
    Logout THIS device/session:
      - Revokes all active refresh tokens for the current session_id.
      - Access tokens are not stored/blacklisted; they expire naturally.
    Reads userid/session_id from token claims (new flow) or from body (old flow).
    """
    try:
        payload = get_jwt()
        userid = payload.get("userid")
        session_id = payload.get("session_id")


        if not userid:
            return jsonify({
                "statusCode": 400,
                "message": "userid missing (claim/body)",
                "response": {}
            }), 400

        if not session_id:
            return jsonify({
                "statusCode": 400,
                "message": "session_id missing (claim/body)",
                "response": {}
            }), 400

        revoked_count = logout_current_session(userid, session_id)

        return jsonify({
            "statusCode": 200,
            "message": "Logged out successfully",
            "response": {
                "userid": userid,
                "session_id": session_id,
                "revoked_count": revoked_count
            }
        }), 200

    except Exception as e:
        print(f"Error in /api/logout: {e}")
        return jsonify({
            "statusCode": 500,
            "message": "Internal server error",
            "response": {}
        }), 500


@logout_bp.route("/api/logout_all", methods=["POST"])
@jwt_required()
def logout_all():
    """
    Logout ALL devices/sessions for this user:
      - Revokes all active refresh tokens for the userid.
    Reads userid from token claims (new flow) or resolves via body (old flow).
    """
    try:
        payload = get_jwt()
        userid = payload.get("userid")

        if not userid:
            return jsonify({
                "statusCode": 400,
                "message": "userid missing (claim/body)",
                "response": {}
            }), 400

        revoked_count = logout_all_sessions(userid)

        return jsonify({
            "statusCode": 200,
            "message": "Logged out from all sessions",
            "response": {
                "userid": userid,
                "revoked_count": revoked_count
            }
        }), 200

    except Exception as e:
        print(f"Error in /api/logout_all: {e}")
        return jsonify({
            "statusCode": 500,
            "message": "Internal server error",
            "response": {}
        }), 500
