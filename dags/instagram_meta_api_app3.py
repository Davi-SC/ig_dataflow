from airflow.decorators import dag
from datetime import datetime

from tasks.meta_api.fetch_data_meta_api import fetch_data_meta_api
from tasks.meta_api.save_raw_json import save_raw_json

@dag(
    dag_id="instagram_meta_api_app3_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["instagram", "meta_api", "app3"],
    description="Collect Instagram data using Meta Graph API - App 3 - Sequential Execution"
)
def instagram_meta_api_app3_pipeline():
    
    # Lista de perfis para coletar dados - App 3
    perfis = [
        "cnnbrasil",
        # Adicione mais perfis aqui
    ]
    
    # Execução Sequencial: Um perfil por vez
    # fetch -> save -> fetch_next -> save_next
    
    previous_task = None
    
    for profile in perfis:
        # max_posts como limite de posts e fetch
        fetch_task = fetch_data_meta_api.override(task_id=f"fetch_{profile}")( 
            username=profile, 
            fetch_all_posts=True,
            max_posts=5000,
            app_id=3  # Usa credenciais do App 3
        )
        
        save_task = save_raw_json.override(task_id=f"save_{profile}")(data=fetch_task)
        
        # Se houver uma tarefa anterior, define a dependência
        if previous_task:
            previous_task >> fetch_task
            
        # Atualiza a tarefa anterior para a atual (save_task), o próximo fetch só começa depois que o save atual terminar
        previous_task = save_task

# Instanciando o DAG
instagram_meta_api_app3_pipeline()
