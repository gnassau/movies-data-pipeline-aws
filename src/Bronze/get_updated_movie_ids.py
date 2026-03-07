def run_incremental_ids():

    import os
    import requests
    import json
    from datetime import datetime, timezone
    import boto3
    from dotenv import load_dotenv

    load_dotenv()

    API_KEY = os.getenv("TMDB_API_KEY")

    if not API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada")

    BUCKET_NAME = "movies-raw-gustavo-portfolio"
    BASE_URL = "https://api.themoviedb.org/3/movie/changes"

    s3 = boto3.client("s3")

    now = datetime.now(timezone.utc)

    params = {
        "api_key": API_KEY,
        "page": 1
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])

    movie_ids = [{"movie_id": movie["id"], "ingestion_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")} for movie in results]

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    s3_key = f"bronze/updated_movie_ids/{year}-{month}-{day}/ids.json"

    json_lines = "\n".join(json.dumps(movie) for movie in movie_ids)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"IDs atualizados enviados para: {s3_key}")