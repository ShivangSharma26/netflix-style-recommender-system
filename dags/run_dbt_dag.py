from datetime import datetime
import os
# Hum DAG import nahi karenge, seedha DbtDag use karenge
from cosmos import DbtDag, ProjectConfig, ProfileConfig

# ================= CONFIGURATION =================
DBT_PROJECT_PATH = "/opt/airflow/dbt"
DBT_PROFILES_FILE_PATH = "/opt/airflow/dbt/profiles.yml"
DBT_PROFILE_NAME = "netflix"
DBT_TARGET_NAME = "dev"

# ================= DAG DEFINITION =================

# Cosmos ka DbtDag directly use kar rahe hain.
# Yeh khud hi ek Airflow DAG hai.
netflix_dbt_dag = DbtDag(
    # --- Airflow DAG ki settings yahan aayengi ---
    dag_id="netflix_dbt_dag",      # <- YEH MISSING THA
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "netflix"],

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