from airflow.decorators import task
import os
import json
from datetime import datetime
import pendulum

# Salva em json
def save_data_to_json(data, base_path="/opt/airflow/data/meta_api"):
    username = data.get("username")
    timestamp = data.get("timestamp")
    posts = data.get("posts", [])
    profile_data = data.get("profile_data", {})
    
    if not username or not timestamp:
        print("Error: Missing username or timestamp in data.")
        return
    
    current_datetime = pendulum.now("America/Sao_Paulo").strftime("%d-%m-%Y_%H-%M-%S")
    
    dir_path = os.path.join(base_path, username)
    os.makedirs(dir_path, exist_ok=True)
    
    # File path: /meta_api/username/username_YYYY-MM-DD_HH-MM-SS.json
    file_path = os.path.join(dir_path, f"{username}_{current_datetime}.json")
    
    output_data = {
        "username": username,
        "collected_at": timestamp,
        "profile": profile_data,
        "posts": posts,
        "total_posts": len(posts)
    }
    
    try:
        with open(file_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(output_data, jsonfile, indent=2, ensure_ascii=False)
        print(f"Successfully saved {len(posts)} posts to {file_path}")
        
    except Exception as e:
        print(f"Error saving JSON for {username}: {e}")
        raise


@task
def save_raw_json(data):
    save_data_to_json(data)
    return True
