def run_full_load():

    import os
    import requests
    import json
    from datetime import datetime, timezone
    import boto3
    from dotenv import load_dotenv

    # =============================
    # LOAD ENV
    # =============================
    load_dotenv()

    API_KEY = os.getenv("TMDB_API_KEY")

    if not API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada")

    # =============================
    # CONFIG
    # =============================
    BUCKET_NAME = "movies-raw-gustavo-portfolio"
    BASE_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"
    BASE_MOVIE_URL = "https://api.themoviedb.org/3/movie"

    s3 = boto3.client("s3")

    start_date = "2025-06-01"
    end_date = datetime.now().date()

    now = datetime.now(timezone.utc)

    movies_data = []
    page = 1

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

        if page >= total_pages or page >= 500:
            break

        page += 1

    print(f"Total de filmes coletados: {len(movies_data)}")

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    s3_key = f"bronze/movies/{year}-{month}-{day}/movies.json"

    json_lines = "\n".join(json.dumps(movie) for movie in movies_data)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Arquivo enviado para S3 em: {s3_key}")