def run_get_updated_ids(mode="incremental_refresh", start_date=None, end_date=None):

    import requests
    import json
    from datetime import datetime, timezone, timedelta
    import boto3
    from airflow.models import Variable

    # =============================
    # LOAD ENV
    # =============================

    API_KEY = Variable.get("TMDB_API_KEY")

    if not API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada")

    # =============================
    # CONFIG
    # =============================

    BUCKET_NAME = "movies-raw-gustavo-portfolio"
    BASE_URL = "https://api.themoviedb.org/3/movie/changes"

    s3 = boto3.client("s3")

    now = datetime.now(timezone.utc)

    # =============================
    # DEFINE DATE RANGE
    # =============================

    if mode == "full_refresh":

        if not start_date or not end_date:
            start_date = start_date or datetime(2026, 3, 1).date()
            end_date = end_date or datetime.now().date()

        print("RUNNING FULL REFRESH")

    elif mode == "incremental_refresh":

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=1)

        print("RUNNING INCREMENTAL REFRESH")

    else:
        raise ValueError("mode deve ser full_refresh ou incremental_refresh")

    print(f"Collecting updates from {start_date} to {end_date}")

    # =============================
    # PAGINAÇÃO DA API
    # =============================

    page = 1
    movie_ids = []

    while True:

        params = {
            "api_key": API_KEY,
            "start_date": start_date,
            "end_date": end_date,
            "page": page
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        print(f"PAGE {page} - IDS {len(results)}")

        if not results:
            break

        for movie in results:

            movie_ids.append({
                "movie_id": movie["id"],
                "ingestion_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
            })

        total_pages = data.get("total_pages", 1)

        if page >= total_pages:
            break

        page += 1

    print(f"Total de IDs coletados: {len(movie_ids)}")

    # =============================
    # SALVAR NO S3
    # =============================

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    s3_key = f"bronze/updated_movie_ids/{year}/{month}/{day}/ids.json"

    json_lines = "\n".join(json.dumps(movie) for movie in movie_ids)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Arquivo enviado para S3 em: {s3_key}")


if __name__ == "__main__":
    run_get_updated_ids()