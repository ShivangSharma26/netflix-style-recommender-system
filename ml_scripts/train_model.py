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
    query = "SELECT * FROM FACT_RATINGS"

    cs.execute(query)
    
    # Data ko Pandas DataFrame mein convert karte hain
    df = cs.fetch_pandas_all()
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(r'\s+', '_', regex=True)
    rename_map = {}
    if 'userid' in df.columns and 'user_id' not in df.columns: rename_map['userid'] = 'user_id'
    if 'user' in df.columns and 'user_id' not in df.columns: rename_map['user'] = 'user_id'
    if 'movieid' in df.columns and 'movie_id' not in df.columns: rename_map['movieid'] = 'movie_id'
    if 'movie' in df.columns and 'movie_id' not in df.columns: rename_map['movie'] = 'movie_id'
    if 'score' in df.columns and 'rating' not in df.columns: rename_map['score'] = 'rating'
    if rename_map:
     df = df.rename(columns=rename_map)

    required = {'user_id','movie_id','rating'}
    missing = required - set(df.columns)
    if missing:
       raise RuntimeError(f"Missing columns after normalization: {missing}. Columns present: {list(df.columns)}")
    ctx.close()
    
    # <<< ADD THESE DEBUG LINES BELOW >>>
    print("\n" + "="*50)
    print("DEBUG: ACTUAL COLUMNS FROM SNOWFLAKE (BEFORE ANY CHANGE)")
    print("Columns exactly as returned:", df.columns.tolist())
    print("Column names as strings:")
    for i, col in enumerate(df.columns):
        print(f"  Column {i}: '{col}'")  # Shows exact spelling, case, spaces
    print("Data types:")
    print(df.dtypes)
    print("Shape:", df.shape)
    print("First 5 rows (sample data):")
    print(df.head(5))
    print("="*50 + "\n")
    
    

    print(f"Original Columns: {df.columns.tolist()}")
    print(f"Lowercased Columns: {df.columns.tolist()}", flush=True)
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