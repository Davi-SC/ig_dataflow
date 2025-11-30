from airflow.decorators import dag
from datetime import datetime

from tasks.instaloader.fetch_data_instaloader import fetch_data_instaloader
from tasks.instaloader.save_raw_csv import save_raw_csv

@dag(
    dag_id="instagram_instaloader_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["instagram", "instaloader"]
)
def dag():
    
    perfis = [
        "ifmacoelhoneto",
        "neabicoelhonetoifma",
        "meninaemulhernaciencia",
        "roboticaifmacn"
    ]

    raw_data = fetch_data_instaloader.expand(username=perfis)
    save_raw_csv.expand(data = raw_data)
    
dag()
