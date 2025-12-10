# ml_scripts/train_model.py
import os
import pandas as pd
from snowflake.connector import connect
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors
import joblib

# --- 1. Snowflake Connection Details (REPLACE THESE!) ---
# Yahan apne sahi credentials daalein
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ROLE = os.getenv('SNOWFLAKE_ROLE')
SF_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Paths define karte hain
MODEL_DIR = "/app/data" # Container ke andar ka path
MODEL_PATH = os.path.join(MODEL_DIR, "recommender_model.pkl")
USER_ITEM_MATRIX_PATH = os.path.join(MODEL_DIR, "user_item_matrix.pkl")

def get_data_from_snowflake():
    print("Connecting to Snowflake...")
    ctx = connect(
        account="hec19639.us-east-1",
        user="SHIVANG26",
        password="Shivang@123444",
        role="ACCOUNTADMIN",
        warehouse="COMPUTE_WH",
        database="netflix_db",
        schema="PUBLIC"
    )
    cs = ctx.cursor()
    
    print("Fetching data from fact_ratings table...")
    # Sirf zaroori columns le rahe hain
    query = "SELECT user_id, movie_id, rating FROM fact_ratings"
    cs.execute(query)
    
    # Data ko Pandas DataFrame mein convert karte hain
    df = cs.fetch_pandas_all()
    ctx.close()
    
    print(f"Original Columns: {df.columns.tolist()}")
    df.columns = df.columns.str.lower()
    print(f"Data fetched successfully! Shape: {df.shape}")
    return df

def train_model(df):
    print("Preparing data for training...")
    
    initial_rows = df.shape[0]
    df = df.drop_duplicates(subset=['user_id', 'movie_id'], keep='last')
    final_rows = df.shape[0]
    print(f"Dropped {initial_rows - final_rows} duplicate rating entries.")
    # Pivot table banate hain: Rows=Users, Columns=Movies, Values=Ratings
    # Missing values ko 0 se bharte hain
    user_item_matrix = df.pivot(index='user_id', columns='movie_id', values='rating').fillna(0)
    
    # Hum K-Nearest Neighbors algorithm use karenge (Collaborative Filtering ke liye)
    # 'cosine' metric use karenge similarity nikalne ke liye
    print("Training KNN model...")
    model_knn = NearestNeighbors(metric='cosine', algorithm='brute', n_neighbors=20, n_jobs=-1)
    
    # Matrix format convert karte hain (sparse matrix for efficiency)
    from scipy.sparse import csr_matrix
    user_item_matrix_sparse = csr_matrix(user_item_matrix.values)
    
    model_knn.fit(user_item_matrix_sparse)
    print("Model trained successfully!")
    
    return model_knn, user_item_matrix

def save_artifacts(model, matrix):
    print(f"Saving artifacts to {MODEL_DIR}...")
    # Agar folder nahi hai toh banao
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Model aur Matrix ko save karte hain
    joblib.dump(model, MODEL_PATH)
    joblib.dump(matrix, USER_ITEM_MATRIX_PATH)
    print("Artifacts saved!")

if __name__ == "__main__":
    print("--- Starting ML Training Pipeline ---")
    # 1. Data lao
    ratings_df = get_data_from_snowflake()
    
    # 2. Train karo
    trained_model, user_item_matrix = train_model(ratings_df)
    
    # 3. Save karo
    save_artifacts(trained_model, user_item_matrix)
    print("--- ML Training Pipeline Complete ---")