# 🌊 Flowstream - MLOps Recommendation System

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-orange)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8)
![FastAPI](https://img.shields.io/badge/FastAPI-Serving-009688)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED)

## 📌 Overview
**Flowstream** is an end-to-end MLOps pipeline designed to build and serve a scalable **Collaborative Filtering Recommendation Engine**. 

The system automates the entire lifecycle of data: ingesting raw interactions into **Snowflake**, transforming them via **dbt**, orchestrating the ML training workflow with **Apache Airflow**, and deploying a real-time inference API using **FastAPI**.

**DEMO VIDEO LINK = https://drive.google.com/file/d/15OLSg0wFvG5yR1c2td0PUb2m1UhpDhpG/view?usp=sharing**



## 🏗 Architecture
The pipeline consists of three main stages:
1.  **Data Engineering:** dbt transforms raw data in Snowflake into a clean `FACT_RATINGS` table.
2.  **Machine Learning:** Airflow triggers a Dockerized training script that fetches data from Snowflake, trains a K-Nearest Neighbors (KNN) model, and serializes the artifacts.
3.  **Model Serving:** A FastAPI container loads the trained artifacts and provides an endpoint to get movie recommendations for a specific User ID.

### Workflow
`Snowflake (Raw Data)` ➡️ `dbt (Transformation)` ➡️ `Airflow (Orchestration)` ➡️ `ML Training (Scikit-Learn)` ➡️ `FastAPI (Inference)`

## 📂 Project Structure
```bash
.
├── api/
│   ├── main.py              # FastAPI application for serving recommendations
│   ├── dockerfile           # Dockerfile for the API service
│   └── requirements.txt     # API dependencies
├── dags/
│   ├── full_pipeline_dag.py # Main Airflow DAG (dbt + ML Trigger)
│   └── run_dbt_dag.py       # Standalone dbt DAG
├── dbt/                     # dbt project folder
│   ├── profiles.yml         # Snowflake connection profile
│   └── dbt_project.yml      # dbt configuration
├── ml_scripts/
│   ├── train_model.py       # Script to fetch Snowflake data & train KNN
│   └── requirements_ml.txt  # Training dependencies
└── requirements.txt         # Project-level requirements
🚀 Tech Stack
Data Warehouse: Snowflake

Transformation: dbt (Data Build Tool) with cosmos

Orchestration: Apache Airflow (running in Docker)

Machine Learning: Scikit-Learn (NearestNeighbors), Pandas

API Framework: FastAPI, Uvicorn

Containerization: Docker

⚙️ Setup & Installation
1. Prerequisites
Docker Desktop installed.

A Snowflake account (Account URL, User, Password, Role, Warehouse).

Python 3.10+.

2. Environment Variables
Create a .env file in the root or ensure your Airflow environment has the following variables set for Snowflake connection:

Code snippet

SNOWFLAKE_ACCOUNT=hec19639.us-east-1
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=netflix_db
SNOWFLAKE_SCHEMA=PUBLIC
3. Running the Pipeline (Airflow)
The DAG full_netflix_pipeline handles the orchestration.

The DAG first runs dbt models using the cosmos library to prepare the FACT_RATINGS table.

Upon success, it triggers the train_ml_model task using the DockerOperator.

This task executes ml_scripts/train_model.py, creating recommender_model.pkl and user_item_matrix.pkl in the shared volume.

4. Running the API
Once the model artifacts are generated, build and run the API container.

Build the image:

Bash

cd api
docker build -t flowstream-api .
Run the container: Make sure to mount the volume where Airflow saved the models (e.g., ./data) to /app/data in the container.

Bash

docker run -p 8000:8000 -v $(pwd)/data:/app/data flowstream-api
📡 API Usage
The API runs on http://localhost:8000.

Health Check
GET /

JSON

{
  "status": "ok",
  "message": "Recommender API is up and running!"
}
Get Recommendations
POST /recommend

Request Body:

JSON

{
  "user_id": 123
}
Response:

JSON

{
  "user_id": 123,
  "recommendations": [
    "The Dark Knight",
    "Inception",
    "Interstellar",
    ...
  ],
  "message": "Top 10 movies recommended successfully."
}
🧠 Model details
Algorithm: K-Nearest Neighbors (KNN)

Metric: Cosine Similarity

Input: User-Item Interaction Matrix (from fact_ratings table)

Output: Top N similar movies based on user viewing history.

🤝 Contributing
Fork the repository.

Create your feature branch (git checkout -b feature/AmazingFeature).

Commit your changes.

Push to the branch.

Open a Pull Request.

📜 License
Distributed under the MIT License. See LICENSE for more information.
