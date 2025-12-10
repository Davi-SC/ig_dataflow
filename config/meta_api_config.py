import os
from dotenv import load_dotenv

# Carrega variaveis de ambiente do .env 
load_dotenv()

# Configuração para múltiplos apps da Meta API
# Cada app tem suas próprias credenciais para distribuir a carga de requisições
META_API_CONFIGS = {
    1: {
        "ig_user_id": os.getenv("IG_USER_ID_1"),
        "app_id": os.getenv("META_APP_ID_1"),
        "app_secret": os.getenv("META_APP_SECRET_1"),
        "access_token": os.getenv("META_ACCESS_TOKEN_1"),
        "api_version": os.getenv("META_API_VERSION", "v24.0")
    },
    2: {
        "ig_user_id": os.getenv("IG_USER_ID_2"),
        "app_id": os.getenv("META_APP_ID_2"),
        "app_secret": os.getenv("META_APP_SECRET_2"),
        "access_token": os.getenv("META_ACCESS_TOKEN_2"),
        "api_version": os.getenv("META_API_VERSION", "v24.0")
    },
    3: {
        "ig_user_id": os.getenv("IG_USER_ID_3"),
        "app_id": os.getenv("META_APP_ID_3"),
        "app_secret": os.getenv("META_APP_SECRET_3"),
        "access_token": os.getenv("META_ACCESS_TOKEN_3"),
        "api_version": os.getenv("META_API_VERSION", "v24.0")
    },
    4: {
        "ig_user_id": os.getenv("IG_USER_ID_4"),
        "app_id": os.getenv("META_APP_ID_4"),
        "app_secret": os.getenv("META_APP_SECRET_4"),
        "access_token": os.getenv("META_ACCESS_TOKEN_4"),
        "api_version": os.getenv("META_API_VERSION", "v24.0")
    }
}

def get_config(app_id=1):
    if app_id not in META_API_CONFIGS:
        raise ValueError(
            f"App ID inválido: {app_id}. "
            f"Por favor, use um app_id entre 1 e {len(META_API_CONFIGS)}."
        )
    
    config = META_API_CONFIGS[app_id]
    
    if not all([
        config["ig_user_id"],
        config["app_id"],
        config["app_secret"],
        config["access_token"]
    ]):
        raise ValueError(
            f"Missing required Meta API credentials for App {app_id}. "
            f"Please check your .env file and ensure all variables are set: "
            f"IG_USER_ID_{app_id}, META_APP_ID_{app_id}, META_APP_SECRET_{app_id}, META_ACCESS_TOKEN_{app_id}"
        )
    
    return config
