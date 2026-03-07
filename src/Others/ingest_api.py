import os
import json
import requests
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

# =============================
# CONFIG
# =============================
load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")
BUCKET_NAME = "movies-raw-gustavo-portfolio"
BASE_URL = "https://api.themoviedb.org/3"
MAX_PAGES = 10  # limite de segurança

if not API_KEY:
    raise ValueError("TMDB_API_KEY not found in environment variables")

s3 = boto3.client("s3")

# =============================
# BUSCAR IDs JÁ SALVOS NO S3
# =============================
existing_ids = set()

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(
    Bucket=BUCKET_NAME,
    Prefix="bronze/movies/"
)

for page in pages:
    if "Contents" in page:
        for obj in page["Contents"]:
            key = obj["Key"]
            filename = key.split("/")[-1]

            if filename.startswith("movie_"):
                parts = filename.split("_")
                if len(parts) >= 2:
                    try:
                        movie_id_saved = int(parts[1])
                        existing_ids.add(movie_id_saved)
                    except ValueError:
                        pass

print(f"{len(existing_ids)} filmes já ingeridos.")

# =============================
# PAGINAR API ATÉ ENCONTRAR NOVO FILME
# =============================
next_movie = None

for page_number in range(1, MAX_PAGES + 1):

    print(f"Consultando página {page_number}...")

    response = requests.get(
        f"{BASE_URL}/movie/popular",
        params={"api_key": API_KEY, "page": page_number}
    )
    response.raise_for_status()

    popular_movies = response.json().get("results", [])

    if not popular_movies:
        break

    for movie in popular_movies:
        if movie["id"] not in existing_ids:
            next_movie = movie
            break

    if next_movie:
        break

if not next_movie:
    print("Nenhum novo filme encontrado nas páginas consultadas.")
    exit()

movie_id = next_movie["id"]

# =============================
# BUSCAR DETALHES
# =============================
details = requests.get(
    f"{BASE_URL}/movie/{movie_id}",
    params={"api_key": API_KEY}
)
details.raise_for_status()
details = details.json()

# =============================
# BUSCAR CRÉDITOS
# =============================
credits = requests.get(
    f"{BASE_URL}/movie/{movie_id}/credits",
    params={"api_key": API_KEY}
)
credits.raise_for_status()
credits = credits.json()

actors = [actor["name"] for actor in credits.get("cast", [])[:10]]

data = {
    "id": movie_id,
    "title": details.get("title"),
    "release_date": details.get("release_date"),
    "budget": details.get("budget"),
    "revenue": details.get("revenue"),
    "runtime": details.get("runtime"),
    "actors": actors,
}

print("Filme ingerido nesta execução:")
print(data)

# =============================
# TIMESTAMP UTC
# =============================
now = datetime.now(timezone.utc)
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
timestamp = now.strftime("%Y%m%d_%H%M%S")

s3_key = (
    f"bronze/movies/"
    f"year={year}/"
    f"month={month}/"
    f"day={day}/"
    f"movie_{movie_id}_{timestamp}.json"
)

# =============================
# UPLOAD PARA S3
# =============================
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=s3_key,
    Body=json.dumps(data),
    ContentType="application/json"
)

print(f"Arquivo enviado para S3 em: {s3_key}")