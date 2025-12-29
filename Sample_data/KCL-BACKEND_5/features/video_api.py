# features/video_api.py
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from controller.video_controller import upload_original_video

video_bp = Blueprint("video", __name__)


@video_bp.route("/api/video/upload", methods=["POST"])
@jwt_required(optional=True)
def video_upload():
    try:
        # file comes as multipart/form-data with key "video"
        if "video" not in request.files:
            return jsonify({
                "statusCode": 400,
                "message": "video file is required",
                "response": {}
            })

        file_obj = request.files["video"]

        # optional: who uploaded
        user = get_jwt_identity() or "admin"

        video_id = upload_original_video(file_obj, created_by=user)

        return jsonify({
            "statusCode": 200,
            "message": "Video uploaded and queued for processing",
            "response": {
                "videoId": video_id,
                "status": "queued"
            }
        })

    except Exception as e:
        return jsonify({
            "statusCode": 500,
            "message": f"Upload failed: {str(e)}",
            "response": {}
        })
