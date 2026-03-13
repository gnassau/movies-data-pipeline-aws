def run_ingestion(mode="incremental_refresh", start_date=None, end_date=None):

    import os
    import requests
    import json
    from datetime import datetime, timezone, timedelta
    import boto3
    from airflow.models import Variable
    from dotenv import load_dotenv

    # =============================
    # LOAD ENV
    # =============================
    load_dotenv()

    # API_KEY = os.getenv("TMDB_API_KEY")
    API_KEY = Variable.get("TMDB_API_KEY")

    if not API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada")

    # =============================
    # CONFIG
    # =============================
    BUCKET_NAME = "movies-raw-gustavo-portfolio"
    BASE_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"
    BASE_MOVIE_URL = "https://api.themoviedb.org/3/movie"

    s3 = boto3.client("s3")

    now = datetime.now(timezone.utc)

    # =============================
    # DEFINE DATE RANGE
    # =============================
    if mode == "full_refresh":

        start_date = start_date or datetime(2026, 3, 1).date()
        end_date = end_date or datetime.now().date()

        print("RUNNING FULL REFRESH")

    elif mode == "incremental_refresh":

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=1)

        print("RUNNING INCREMENTAL REFRESH")

    else:
        raise ValueError("mode deve ser full_refresh ou incremental_refresh")

    print(f"Collecting from {start_date} to {end_date}")

    movies_data = []
    page = 1

    # =============================
    # DISCOVER MOVIES
    # =============================
    while True:

        params = {
            "api_key": API_KEY,
            "primary_release_date.gte": start_date,
            "primary_release_date.lte": end_date,
            "sort_by": "primary_release_date.desc",
            "page": page
        }

        response = requests.get(BASE_DISCOVER_URL, params=params)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        print(f"PAGE {page} - MOVIES {len(results)}")

        if not results:
            break

        for movie in results:

            movie_id = movie["id"]

            movie_response = requests.get(
                f"{BASE_MOVIE_URL}/{movie_id}",
                params={"api_key": API_KEY}
            )

            movie_response.raise_for_status()
            movie_detail = movie_response.json()

            movie_record = {
                "movie_id": movie_detail.get("id"),
                "title": movie_detail.get("title"),
                "release_date": movie_detail.get("release_date"),
                "budget": movie_detail.get("budget"),
                "revenue": movie_detail.get("revenue"),
                "runtime": movie_detail.get("runtime"),
                "popularity": movie_detail.get("popularity"),
                "vote_average": movie_detail.get("vote_average"),
                "vote_count": movie_detail.get("vote_count"),
                "language": movie_detail.get("original_language"),
                "status": movie_detail.get("status"),
                "ingestion_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
            }

            movies_data.append(movie_record)

        total_pages = data.get("total_pages", 1)

        # segurança contra loops gigantes
        if page >= total_pages or page >= 500:
            break

        page += 1

    print(f"Total de filmes coletados: {len(movies_data)}")

    # =============================
    # S3 WRITE (APPEND ONLY)
    # =============================

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    # s3_key = f"bronze/movies/{year}-{month}-{day}/movies.json"
    s3_key = f"bronze/movies/{year}/{month}/{day}/movies.json"

    json_lines = "\n".join(json.dumps(movie) for movie in movies_data)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Arquivo enviado para S3 em: {s3_key}")

if __name__ == "__main__":
    run_ingestion()