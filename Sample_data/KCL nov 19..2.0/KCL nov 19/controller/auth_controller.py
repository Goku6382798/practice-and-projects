# controller/auth_controller.py
from datetime import datetime, timedelta
import os
import uuid
import pytz
from flask_jwt_extended import create_access_token, create_refresh_token
from flask_jwt_extended.utils import decode_token
from pymongo.errors import PyMongoError

from ETL.ETL_utils_ import db

# Mongo collection
_rt = db["refresh_token_data"] if db is not None else None

IST = pytz.timezone("Asia/Kolkata")

def _refresh_expires_dt() -> datetime:
    days = float(os.getenv("JWT_REFRESH_EXPIRES_DAYS", "30"))
    return datetime.utcnow() + timedelta(days=days)

def _now() -> datetime:
    return datetime.utcnow()

def _new_session_id() -> str:
    return f"S-{uuid.uuid4()}"

def _extract_jti(encoded_token: str) -> str:
    # decode_token is from flask_jwt_extended; gets claims without verifying signature again
    data = decode_token(encoded_token)
    return data.get("jti")

# ---------------------------
# Public helpers (import these)
# ---------------------------

def issue_tokens(userid: str, session_id: str | None = None, extra_claims: dict | None = None):
    """
    Create a fresh Access + Refresh token pair, persist the refresh token row,
    and return (access_token, refresh_token, session_id).
    """
    if session_id is None:
        session_id = _new_session_id()

    claims_access = {"type": "access", "userid": userid, "session_id": session_id}
    if extra_claims:
        claims_access.update(extra_claims)

    claims_refresh = {"type": "refresh", "userid": userid, "session_id": session_id}

    access_token = create_access_token(identity=userid, additional_claims=claims_access)
    refresh_token = create_refresh_token(identity=userid, additional_claims=claims_refresh)

    # Persist refresh token metadata
    jti = _extract_jti(refresh_token)
    doc = {
        "jti": jti,
        "userid": userid,
        "session_id": session_id,
        "type": "refresh",
        "revoked": False,
        "issued_at": _now(),
        "expires_at": _refresh_expires_dt(),
    }
    try:
        if _rt is not None:
            _rt.insert_one(doc)
    except PyMongoError as e:
        # If insert fails, we still return tokens, but log the error
        print(f"❌ Failed to save refresh token doc: {e}")

    return access_token, refresh_token, session_id


def rotate_refresh(old_refresh_token: str):
    """
    Revoke the old refresh token by its JTI and issue a fresh pair (AT, RT).
    Returns (access_token, refresh_token, session_id, userid) or raises ValueError.
    """
    if _rt is None:
        raise ValueError("No DB connection for refresh tokens")

    decoded = decode_token(old_refresh_token)
    if decoded.get("type") != "refresh":
        raise ValueError("Not a refresh token")

    old_jti = decoded.get("jti")
    userid = decoded.get("sub") or decoded.get("userid")
    session_id = decoded.get("session_id")

    if not old_jti or not userid or not session_id:
        raise ValueError("Refresh token missing required claims")

    # Check doc
    doc = _rt.find_one({"jti": old_jti}, {"revoked": 1, "expires_at": 1})
    if doc is None:
        # Missing = treat as invalid
        raise ValueError("Refresh token not found")
    if doc.get("revoked", False):
        # Reuse attempt; caller can decide to logout-all if needed
        raise ValueError("Refresh token already revoked")
    if doc.get("expires_at") and doc["expires_at"] < _now():
        raise ValueError("Refresh token expired")

    # Revoke old
    _rt.update_one({"jti": old_jti}, {"$set": {"revoked": True, "revoked_at": _now()}})

    # Issue new pair and persist new RT
    at, rt, _ = issue_tokens(userid=userid, session_id=session_id)
    return at, rt, session_id, userid


def revoke_by_session(userid: str, session_id: str):
    """
    Revoke all active refresh tokens for a single device/session.
    """
    if _rt is None:
        return 0
    res = _rt.update_many(
        {"userid": userid, "session_id": session_id, "type": "refresh", "revoked": False},
        {"$set": {"revoked": True, "revoked_at": _now()}}
    )
    return res.modified_count


def revoke_by_user(userid: str):
    """
    Revoke all active refresh tokens for a user (logout-all).
    """
    if _rt is None:
        return 0
    res = _rt.update_many(
        {"userid": userid, "type": "refresh", "revoked": False},
        {"$set": {"revoked": True, "revoked_at": _now()}}
    )
    return res.modified_count
