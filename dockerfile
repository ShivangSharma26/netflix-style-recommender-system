# Start from the official Airflow image
FROM apache/airflow:2.7.1

# Switch to root to install system packages
USER root

# Install git (required by dbt to download packages)
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

# Switch back to the airflow user so things run securely
USER airflow

# --- SIMPLEST INSTALLATION ---
# Bina version ke seedha install kar rahe hain (latest uthayega)
RUN pip install --no-cache-dir dbt-snowflake astronomer-cosmos