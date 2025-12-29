from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_swagger_ui import get_swaggerui_blueprint
import os
from datetime import timedelta, datetime
from ETL.ETL_utils_ import ensure_refresh_indexes, db

app = Flask(__name__)
CORS(app)

app.config['JWT_SECRET_KEY'] = os.getenv("SECRET_KEY", "default_secret_key")
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(
    minutes=int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES_MINUTES", 30))
)
jwt = JWTManager(app)

@jwt.unauthorized_loader
def custom_unauthorized_response(err): return jsonify({"statusCode":401,"message":"Invalid user","response":{}})
@jwt.invalid_token_loader
def custom_invalid_token_response(err): return jsonify({"statusCode":422,"message":"Invalid token","response":{}})
@jwt.expired_token_loader
def custom_expired_token_response(h,p): return jsonify({"statusCode":401,"message":"Token expired, please login again","response":{}})


ensure_refresh_indexes()
@jwt.token_in_blocklist_loader
def check_if_token_revoked(jwt_header, jwt_payload):
    try:
        if jwt_payload.get("type") == "refresh":
            if db is None:
                return True  # safest: block if no DB
            doc = db["refresh_token_data"].find_one({"jti": jwt_payload.get("jti")}, {"revoked": 1})
            return (doc is None) or bool(doc.get("revoked", False))
        return False
    except Exception as e:
        print(f"Error checking token blocklist: {e}")
        return True

@jwt.token_in_blocklist_loader
def check_if_token_revoked(jwt_header, jwt_payload):
    jti = jwt_payload.get("jti")
    token_type = jwt_payload.get("type", "access")
    if token_type == "refresh":
        doc = db.refresh_token_data.find_one({"jti": jti, "revoked": True})
        return doc is not None
    return False


SWAGGER_URL = '/swagger'
API_URL = '/static/swagger/root.yaml'
#app.register_blueprint(get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "KCL Flask API"}), url_prefix=SWAGGER_URL)


swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "KCL Unified API"
    }
)

app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)




# import and register all blueprints in one place
from features.login import login_bp
from features.language_api import language_bp
from features.dashboard_api import dashboard_bp
from features.logout import logout_bp
from features.user_details import profile_setup_bp
from features.user_profile import user_profile_bp
from features.pincode_verify import pincode_bp
from features.contest import contest_bp
from features.auth import auth_bp
from features.communication import user_bp
from controller.video_controller import video_bp 


app.register_blueprint(user_bp)
app.register_blueprint(login_bp)
app.register_blueprint(language_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(logout_bp)
app.register_blueprint(profile_setup_bp)
app.register_blueprint(user_profile_bp)
app.register_blueprint(pincode_bp)
app.register_blueprint(contest_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(video_bp)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
