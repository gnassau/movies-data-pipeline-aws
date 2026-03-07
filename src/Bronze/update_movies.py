def run_update_movies():

    import os
    import json
    import requests
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
    PREFIX = "bronze/updated_movie_ids/"
    BASE_MOVIE_URL = "https://api.themoviedb.org/3/movie"

    s3 = boto3.client("s3")

    # =============================
    # ENCONTRAR ARQUIVO MAIS RECENTE
    # =============================
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=PREFIX
    )

    if "Contents" not in response:
        raise ValueError("Nenhum arquivo encontrado em updated_movie_ids")

    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])

    key = latest_file["Key"]

    print("Lendo arquivo:", key)

    obj = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )

    content = obj["Body"].read().decode("utf-8")

    movie_ids = [json.loads(line) for line in content.splitlines()]

    print("Total de IDs:", len(movie_ids))

    # =============================
    # BUSCAR DETALHES DOS FILMES
    # =============================
    now = datetime.now(timezone.utc)

    movies_data = []

    for movie in movie_ids:

        movie_id = movie["movie_id"]

        response = requests.get(
            f"{BASE_MOVIE_URL}/{movie_id}",
            params={"api_key": API_KEY}
        )

        response.raise_for_status()

        movie_detail = response.json()

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

        print(f"Atualizado: {movie_record['title']}")

    # =============================
    # SALVAR NO S3
    # =============================
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    s3_key = (
        f"bronze/movies_updates/"
        f"{year}-{month}-{day}/"
        f"movies_updates.json"
    )

    json_lines = "\n".join(json.dumps(movie) for movie in movies_data)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Arquivo enviado para S3: {s3_key}")