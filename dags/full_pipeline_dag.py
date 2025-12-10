# dags/full_pipeline_dag.py
from datetime import datetime
import os
# Hum DAG import nahi karenge, seedha DbtDag use karenge
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from airflow.providers.docker.operators.docker import DockerOperator

# ================= CONFIGURATION =================
DBT_PROJECT_PATH = "/opt/airflow/dbt"
DBT_PROFILES_FILE_PATH = "/opt/airflow/dbt/profiles.yml"
DBT_PROFILE_NAME = "netflix"
DBT_TARGET_NAME = "dev"
ML_IMAGE_NAME = "netflix-style-recommender-system-ml-trainer:latest"

# ================= DAG DEFINITION =================

# Cosmos ka DbtDag directly use kar rahe hain.
# Yeh khud hi hamara main Airflow DAG hai.
full_netflix_pipeline_dag = DbtDag(
    # --- Airflow DAG ki settings ---
    dag_id="full_netflix_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "ml", "netflix"],

    # --- Cosmos dbt settings ---
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=DBT_TARGET_NAME,
        profiles_yml_filepath=DBT_PROFILES_FILE_PATH,
    ),
    operator_args={
        "install_deps": True,
    },
)

# --- Task 2: ML Model Training (DockerOperator) ---
# Is task ko hum alag se define kar rahe hain
ml_training_task = DockerOperator(
    task_id='train_ml_model',
    image=ML_IMAGE_NAME,
    api_version='auto',
    auto_remove=True,
    command="python ml_scripts/train_model.py",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
        {
            "source": "netflix-data",  # keep data volume only (optional)
            "target": "/app/data",
            "type": "volume"
        }
    ],
   
    # Environment variables Airflow container se liye jayenge (jo .env se aaye hain)
    environment={
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_PASSWORD': os.getenv('SNOWFLAKE_PASSWORD'),
        'SNOWFLAKE_ROLE': os.getenv('SNOWFLAKE_ROLE'),
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
    },
    # Zaroori: Is task ko hamare main DAG ke saath jodna
    dag=full_netflix_pipeline_dag
)

# --- Dependency Setting (Crucial Part) ---
# Hamein batana hai ki dbt khatam hone ke BAAD hi ML training shuru ho.
# Cosmos mein dbt ka aakhri task usually 'model_name_run' hota hai.
# Hamare case mein aakhri model 'fact_ratings' hai.

# Hum DAG ke aakhri task ko dhoondh kar uske baad ML task laga rahe hain.
# 'fact_ratings_run' hamara final dbt task hai.
dbt_final_task = full_netflix_pipeline_dag.get_task('fact_ratings_run')
dbt_final_task >> ml_training_task