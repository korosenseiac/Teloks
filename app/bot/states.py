# Simple in-memory state management
# user_id -> { "step": str, "data": dict }

user_login_states = {}

# Profile setup states (separate from login states)
user_profile_states = {}

class LoginStep:
    ASK_API_ID = "ASK_API_ID"
    ASK_API_HASH = "ASK_API_HASH"
    ASK_PHONE = "ASK_PHONE"
    ASK_CODE = "ASK_CODE"
    ASK_PASSWORD = "ASK_PASSWORD"

class ProfileStep:
    ASK_GENDER = "ASK_GENDER"
    ASK_AGE = "ASK_AGE"
