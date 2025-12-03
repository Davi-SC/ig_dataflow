import os
from dotenv import load_dotenv

# Carrega variaveis de ambiente do .env 
load_dotenv()

META_API_CONFIG = {
    "ig_user_id": os.getenv("IG_USER_ID"),
    "app_id": os.getenv("META_APP_ID"),
    "app_secret": os.getenv("META_APP_SECRET"),
    "access_token": os.getenv("META_ACCESS_TOKEN"),
    "api_version": os.getenv("META_API_VERSION", "v24.0")
}

def get_config():
    if not all([
        META_API_CONFIG["ig_user_id"],
        META_API_CONFIG["app_id"],
        META_API_CONFIG["app_secret"],
        META_API_CONFIG["access_token"]
    ]):
        raise ValueError(
            "Missing required Meta API credentials. "
            "Please check your .env file and ensure all variables are set: "
            "IG_USER_ID, META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN"
        )
    
    return META_API_CONFIG
