from flask import Blueprint, request, jsonify
from features.video import handle_video_upload

video_bp = Blueprint("video_bp", __name__)

@video_bp.route("/api/videos/upload", methods=["POST"])
def upload_video():
    """
    Accepts multipart/form-data with:
      - file: video file
      - created_by: optional (string)
    """
    file = request.files.get("file")
    created_by = request.form.get("created_by", "anonymous")

    if not file:
        return jsonify({
            "status": "error",
            "message": "No file provided. Use form-data with key 'file'.",
            "data": {}
        }), 400

    try:
        doc = handle_video_upload(file, created_by=created_by)

        return jsonify({
            "status": "success",
            "message": "Video uploaded and processed successfully",
            "data": doc
        }), 201

    except Exception as e:
        # In real code, log the full traceback
        return jsonify({
            "status": "error",
            "message": "Failed to upload/process video",
            "data": {"error": str(e)}
        }), 500
