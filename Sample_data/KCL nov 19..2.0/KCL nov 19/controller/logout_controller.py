# controller/logout_controller.py
from datetime import datetime, timezone
from ETL.ETL_utils_ import db
from controller.auth_controller import revoke_by_session, revoke_by_user  # reuse existing logic

_rt = db["refresh_token_data"] if db is not None else None

def now_utc():
    return datetime.now(timezone.utc)

def resolve_userid_from_body(login_type: str | None, mobile: str | None, mail: str | None) -> str | None:
    """
    Fallback resolver when old access tokens don't carry `userid`.
    Uses login_type + mobile/mail to fetch userid from user_master.
    """
    if db is None:
        return None
    if login_type == "1" and mobile:
        doc = db.user_master.find_one({"mobile": mobile}, {"userid": 1, "_id": 0})
        return (doc or {}).get("userid")
    if login_type == "2" and mail:
        doc = db.user_master.find_one({"mail": mail}, {"userid": 1, "_id": 0})
        return (doc or {}).get("userid")
    return None

def logout_current_session(userid: str, session_id: str) -> int:
    """
    Revoke all active refresh tokens for this user's single session/device.
    Returns number of refresh docs marked revoked.
    """
    # prefer using central auth_controller so revocation stays consistent
    return revoke_by_session(userid, session_id)

def logout_all_sessions(userid: str) -> int:
    """
    Revoke all active refresh tokens across all devices for this user.
    Returns number of refresh docs marked revoked.
    """
    return revoke_by_user(userid)

def is_session_active(userid: str, session_id: str) -> bool:
    """
    Optional utility: check if there is at least one non-revoked, non-expired
    refresh token for the given session.
    """
    if _rt is None:
        return False
    q = {
        "userid": userid,
        "session_id": session_id,
        "type": "refresh",
        "revoked": False,
        "expires_at": {"$gt": now_utc()}
    }
    return _rt.find_one(q, {"_id": 1}) is not None
