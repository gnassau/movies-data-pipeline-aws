def run_silver_table():
    import boto3
    import json
    from datetime import datetime

    BUCKET_NAME = "movies-raw-gustavo-portfolio"

    s3 = boto3.client("s3")


    # =============================
    # FUNÇÃO PARA LER JSON DO S3
    # =============================
    def read_json_lines(prefix):

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

        data = []

        for page in pages:
            for obj in page.get("Contents", []):

                key = obj["Key"]
                
                print("LENDO:", key)   # <-- DEBUG

                response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                body = response["Body"].read().decode("utf-8")

                for line in body.splitlines():
                    data.append(json.loads(line))

        return data


    # =============================
    # LER BRONZE
    # =============================
    movies = read_json_lines("bronze/movies/")
    updates = read_json_lines("bronze/movies_updates/")

    print(f"Filmes da camada Bronze: {len(movies)}")
    print(f"Filmes atualizados na camada Bronze: {len(updates)}")

    all_movies = movies + updates

    all_movies = sorted(
        all_movies,
        key=lambda x: datetime.strptime(x["ingestion_timestamp"], "%Y-%m-%d %H:%M:%S")
    )   

    print(f"All movies: {len(all_movies)}")


    # =============================
    # DEDUPLICAÇÃO
    # =============================
    latest_movies = {}

    for movie in all_movies:

        movie_id = movie["movie_id"]
        timestamp = datetime.strptime(movie["ingestion_timestamp"], "%Y-%m-%d %H:%M:%S")

        if movie_id not in latest_movies:
            latest_movies[movie_id] = movie
        else:
            existing_timestamp = datetime.strptime(
                latest_movies[movie_id]["ingestion_timestamp"],
                "%Y-%m-%d %H:%M:%S"
            )

            if timestamp > existing_timestamp:
                latest_movies[movie_id] = movie


    silver_movies = list(latest_movies.values())
    print(f"Filmes na camada Silver: {len(silver_movies)}")

    # =============================
    # REMOVENDO VALORES NULOS NA COLUNA RELEASE_DATE
    # =============================
    silver_movies = [m for m in silver_movies if m.get("release_date") is not None and m.get("release_date") != ""]

    # =============================
    # CONVERTER PARA JSON LINES
    # =============================
    json_lines = "\n".join(json.dumps(m) for m in silver_movies)


    # =============================
    # SALVAR NA CAMADA SILVER
    # =============================
    now = datetime.utcnow()
    date_folder = now.strftime("%Y-%m-%d")

    s3_key = f"silver/movies/{date_folder}/movies.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Silver salvo em: {s3_key}")