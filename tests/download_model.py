# download_model.py
from sentence_transformers import SentenceTransformer
import os

cache_path = os.path.expanduser("~/models/bge-base-en-v1.5")
os.makedirs(cache_path, exist_ok=True)

print(f"Downloading model to: {cache_path}")
model = SentenceTransformer(
    "BAAI/bge-base-en-v1.5",
    cache_folder=cache_path
)
print("Model downloaded successfully!")
