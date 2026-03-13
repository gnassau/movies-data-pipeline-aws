def run_silver_table():

    print("Iniciando processamento da camada Silver")

    import boto3
    import json
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datetime import datetime
    from collections import defaultdict

    BUCKET_NAME = "movies-raw-gustavo-portfolio"

    s3 = boto3.client("s3")

    # =============================
    # LISTAR TODOS OS ARQUIVOS JSON
    # =============================

    def list_json_files(prefix):

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

        keys = []

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if key.endswith(".json"):
                    keys.append(key)

        return keys


    # =============================
    # LER JSON LINES
    # =============================

    def read_json_from_key(key):

        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        body = obj["Body"].read().decode("utf-8")

        data = []

        for line in body.splitlines():
            data.append(json.loads(line))

        return data


    # =============================
    # LER TODOS OS DADOS DA BRONZE
    # =============================

    print("Lendo bronze full load")

    movies_keys = list_json_files("bronze/movies/")
    updates_keys = list_json_files("bronze/movies_updates/")

    all_movies = []

    for key in movies_keys:
        print("Lendo:", key)
        all_movies.extend(read_json_from_key(key))

    for key in updates_keys:
        print("Lendo:", key)
        all_movies.extend(read_json_from_key(key))

    print("Total registros bronze:", len(all_movies))

    # =============================
    # DEDUP POR MOVIE_ID + TIMESTAMP
    # =============================

    latest_movies = {}

    for movie in all_movies:

        movie_id = movie["movie_id"]
        ts = movie["ingestion_timestamp"]

        if movie_id not in latest_movies:

            latest_movies[movie_id] = movie

        else:

            current_ts = latest_movies[movie_id]["ingestion_timestamp"]

            if ts > current_ts:
                latest_movies[movie_id] = movie

    silver_movies = list(latest_movies.values())

    print("Após deduplicação:", len(silver_movies))

    # =============================
    # LIMPEZA
    # =============================

    silver_movies = [
        m for m in silver_movies
        if m.get("release_date")
    ]

    # =============================
    # AGRUPAR POR ANO (PARTIÇÃO)
    # =============================

    movies_by_year = defaultdict(list)

    for movie in silver_movies:

        try:

            year = movie["release_date"][:4]

            movie["year"] = year

            movies_by_year[year].append(movie)

        except:
            continue


    # =============================
    # SCHEMA
    # =============================

    schema = pa.schema([
        ("movie_id", pa.int64()),
        ("title", pa.string()),
        ("release_date", pa.string()),
        ("budget", pa.int64()),
        ("revenue", pa.int64()),
        ("runtime", pa.int64()),
        ("popularity", pa.float64()),
        ("vote_average", pa.float64()),
        ("vote_count", pa.int64()),
        ("language", pa.string()),
        ("status", pa.string()),
        ("ingestion_timestamp", pa.string())
    ])

    # =============================
    # UPSERT NAS PARTIÇÕES
    # =============================

    for year, movies_list in movies_by_year.items():

        print(f"Salvando partição year={year} ({len(movies_list)} filmes)")

        table = pa.Table.from_pylist(
            movies_list,
            schema=schema
        )

        buffer = io.BytesIO()

        pq.write_table(
            table,
            buffer,
            compression="snappy"
        )

        buffer.seek(0)

        s3_key = f"silver/movies/year={year}/movies.parquet"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=buffer.getvalue()
        )

        print("Partição atualizada:", s3_key)


if __name__ == "__main__":
    run_silver_table()