# api/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import numpy as np
import os

# --- CONFIGURATION ---
MODEL_PATH = "data/recommender_model.pkl"
MATRIX_PATH = "data/user_item_matrix.pkl"
MOVIES_CSV_PATH = "data/movies.csv"  # Movies file path (host ./data/movies.csv)
N_RECOMMENDATIONS = 10

# Globals
model_knn = None
user_item_matrix = None
movie_title_lookup = None

app = FastAPI(
    title="Netflix Style Movie Recommender API",
    description="Serves personalized movie recommendations based on a trained KNN model."
)

class RecommendationRequest(BaseModel):
    user_id: int

def _normalize_movies_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r'\s+', '_', regex=True)
    )

    rename_map = {}
    # common id variants
    if 'movieid' in df.columns and 'movie_id' not in df.columns:
        rename_map['movieid'] = 'movie_id'
    if 'movie_id' not in df.columns:
        # find any column that looks like id
        for c in df.columns:
            if 'id' in c and df[c].dtype.kind in 'iu':
                rename_map[c] = 'movie_id'
                break
    # title variants
    if 'title' not in df.columns:
        for c in df.columns:
            if 'title' in c or 'name' in c:
                rename_map[c] = 'title'
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    return df

def load_artifacts():
    global model_knn, user_item_matrix, movie_title_lookup

    # check model artifacts
    if not os.path.exists(MODEL_PATH) or not os.path.exists(MATRIX_PATH):
        print("Model files not found. The model needs to be trained first.")
        raise RuntimeError("Model artifacts are missing. Run the Airflow pipeline first.")

    try:
        print(f"Loading model from {MODEL_PATH}...")
        model_knn = joblib.load(MODEL_PATH)
        user_item_matrix = joblib.load(MATRIX_PATH)
        print("Model and matrix loaded successfully!")

        # --- Load Titles (robust to header variants) ---
        if not os.path.exists(MOVIES_CSV_PATH):
            raise FileNotFoundError(f"{MOVIES_CSV_PATH} not found on host. Put movies.csv in ./data/")

        movies_df = pd.read_csv(MOVIES_CSV_PATH)
        movies_df = _normalize_movies_df(movies_df)

        # final check and fallback heuristics
        if 'movie_id' not in movies_df.columns or 'title' not in movies_df.columns:
            # try additional heuristics: numeric id column and text column
            # (only if normalization above failed)
            for c in movies_df.columns:
                if 'id' in c and movies_df[c].dtype.kind in 'iu':
                    movies_df = movies_df.rename(columns={c: 'movie_id'})
                    break
            if 'title' not in movies_df.columns:
                for c in movies_df.columns:
                    if movies_df[c].dtype == object:
                        movies_df = movies_df.rename(columns={c: 'title'})
                        break

        if 'movie_id' not in movies_df.columns or 'title' not in movies_df.columns:
            raise RuntimeError(f"movies.csv must contain 'movie_id' and 'title' columns. Found: {list(movies_df.columns)}")

        # keep only necessary columns
        movies_df = movies_df[['movie_id', 'title']].copy()
        # ensure movie_id is suitable index (convert to numeric if possible)
        try:
            movies_df['movie_id'] = pd.to_numeric(movies_df['movie_id'], errors='ignore')
        except Exception:
            pass

        movies_df = movies_df.set_index('movie_id')
        movie_title_lookup = movies_df['title'].to_dict()
        print("Movie title lookup table loaded.")

    except Exception as e:
        print(f"An error occurred during loading: {e}")
        raise RuntimeError(f"Error loading artifacts or titles: {e}")

@app.on_event("startup")
async def startup_event():
    load_artifacts()

def get_recommendations(user_id: int):
    if user_id not in user_item_matrix.index:
        raise ValueError(f"User ID {user_id} not found in the dataset.")

    user_index = user_item_matrix.index.get_loc(user_id)
    user_vector = user_item_matrix.iloc[user_index].values.reshape(1, -1)

    distances, indices = model_knn.kneighbors(user_vector, n_neighbors=N_RECOMMENDATIONS + 1)

    similar_users_indices = indices.flatten()[1:]
    similar_users_ratings = user_item_matrix.iloc[similar_users_indices]

    rated_movies = user_item_matrix.columns[user_item_matrix.iloc[user_index] > 0]

    recommendation_scores = similar_users_ratings.sum(axis=0)
    recommendation_scores = recommendation_scores.drop(rated_movies, errors='ignore')

    top_recommendation_ids = recommendation_scores.sort_values(ascending=False).head(N_RECOMMENDATIONS).index.tolist()
    return top_recommendation_ids

@app.post("/recommend")
async def get_movie_recommendations(request: RecommendationRequest):
    try:
        user_id = request.user_id
        recommendation_ids = get_recommendations(user_id)

        recommendation_titles = [
            movie_title_lookup.get(int(id) if not isinstance(id, str) and hasattr(id, "__int__") else id,
                                   f"ID {id} (Title Not Found)")
            for id in recommendation_ids
        ]

        return {
            "user_id": user_id,
            "recommendations": recommendation_titles,
            "message": f"Top {N_RECOMMENDATIONS} movies recommended successfully."
        }

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Recommender API is up and running!"}
