from airflow.decorators import task

from airflow.decorators import task
import os
import csv

def save_profile_data_to_csv(data, base_path="/opt/airflow/data/perfil"):
    username = data.get("username")
    profile_data = data.get("profile_data", {})
    posts_data = data.get("posts_data", [])
    timestamp = data.get("timestamp")

    if not username or not timestamp:
        print("Error: Missing username or timestamp in data.")
        return

    dir_path = os.path.join(base_path, timestamp)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, f"{username}.csv")

    rows = []
    if posts_data:
        for post in posts_data:
            row = {**profile_data, **post}
            rows.append(row)
    else:
        rows.append(profile_data)

    if not rows:
        print(f"No data to save for {username}")
        return

    fieldnames = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    try:
        with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Successfully saved data to {file_path}")

    except Exception as e:
        print(f"Error saving CSV for {username}: {e}")


@task
def save_raw_csv(data):
    save_profile_data_to_csv(data)
    return True
