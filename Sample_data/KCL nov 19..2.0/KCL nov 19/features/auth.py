# features/auth.py
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from controller.auth_controller import rotate_refresh
import traceback

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/api/auth/refresh", methods=["POST"])
def refresh_token():
    """
    Accepts a valid refresh token and returns a new access+refresh pair.
    """
    try:
        data = request.get_json(silent=True) or {}
        refresh_token = data.get("refresh_token")

        # allow sending token via header too
        if not refresh_token:
            refresh_token = request.headers.get("Authorization", "").replace("Bearer ", "")

        if not refresh_token:
            return jsonify({
                "statusCode": 400,
                "message": "Refresh token missing",
                "response": {}
            }), 400

        try:
            new_access, new_refresh, session_id, userid = rotate_refresh(refresh_token)
        except ValueError as ve:
            # rotation failed (revoked, expired, etc.)
            return jsonify({
                "statusCode": 401,
                "message": f"Refresh failed: {str(ve)}",
                "response": {}
            }), 401

        return jsonify({
            "statusCode": 200,
            "message": "Token refreshed successfully",
            "response": {
                "access_token": new_access,
                "refresh_token": new_refresh,
                "session_id": session_id,
                "userid": userid
            }
        }), 200

    except Exception as e:
        print("Error in /api/auth/refresh:", e)
        traceback.print_exc()
        return jsonify({
            "statusCode": 500,
            "message": "Internal server error",
            "response": {}
        }), 500
