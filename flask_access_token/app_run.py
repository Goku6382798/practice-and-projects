from flask import Flask, request, jsonify
import sqlite3
import secrets
import hashlib
from datetime import datetime, timedelta
import os
import jwt

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "tokens.db")

SECRET_KEY = "Batman_12345_change_this_country"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     user_id TEXT NOT NULL,
                     token_hash TEXT NOT NULL UNIQUE,
                     expires_at TEXT NOT NULL,
                     created_at TEXT NOT NULL,
                     revoked INTEGER NOT NULL DEFAULT 0
                     )""")
        
    print("DB READY")

init_db()

def create_access_token(user_id):
    payload = {
        "user_id": user_id,
        "exp": datetime.utcnow() + timedelta(minutes=15),
        "iat": datetime.utcnow()
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return token

@app.route("/refresh-token",methods=["POST"])
def create_refresh_token():
    """
    BODY JSON: {"user_id": "123"}
    Returns: {"access_token": "...", "refresh_token": "...", "expires_at": "..."}
    """
    data = request.get_json(silent=True) or {}
    user_id = str(data.get("user_id","")).strip()
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    
    plaintext_refresh = secrets.token_urlsafe(48)
    token_hash = hashlib.sha256(plaintext_refresh.encode("utf-8")).hexdigest()

    expires_at = (datetime.utcnow() + timedelta(days=30)).isoformat() + "Z"
    created_at = datetime.utcnow().isoformat() + "Z"

    with db() as conn:
        conn.execute(
            "INSERT INTO refresh_tokens (user_id, token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (user_id, token_hash, expires_at, created_at),
        )

    access_token = create_access_token(user_id)

    return jsonify({
        "access_token": access_token,
        "refresh_token": plaintext_refresh,
        "refresh_expires_at": expires_at
    }),201

if __name__ == "__main__":
    app.run(debug=True)