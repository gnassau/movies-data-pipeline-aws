def run_silver_table():

    print("Iniciando processamento da camada Silver")

    import boto3
    import json
    from datetime import datetime

    BUCKET_NAME = "movies-raw-gustavo-portfolio"

    s3 = boto3.client("s3")

    # =============================
    # LER JSON LINES
    # =============================

    def read_json_from_key(key):

        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        body = response["Body"].read().decode("utf-8")

        data = []

        for line in body.splitlines():
            data.append(json.loads(line))

        return data


    # =============================
    # PEGAR ARQUIVO MAIS RECENTE
    # =============================

    def get_latest_file(prefix):

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

        latest_obj = None

        for page in pages:
            for obj in page.get("Contents", []):

                key = obj["Key"]

                if not key.endswith(".json"):
                    continue

                if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                    latest_obj = obj

        if latest_obj is None:
            return None

        return latest_obj["Key"]


    # =============================
    # LER APENAS PARTIÇÃO MAIS RECENTE
    # =============================

    latest_movies_key = get_latest_file("bronze/movies/")
    latest_updates_key = get_latest_file("bronze/movies_updates/")

    movies = []
    updates = []

    if latest_movies_key:
        print("LENDO MOVIES:", latest_movies_key)
        movies = read_json_from_key(latest_movies_key)

    if latest_updates_key:
        print("LENDO UPDATES:", latest_updates_key)
        updates = read_json_from_key(latest_updates_key)

    print("Filmes da camada Bronze:", len(movies))
    print("Filmes atualizados:", len(updates))


    # =============================
    # JUNTAR DADOS
    # =============================

    all_movies = movies + updates

    print("Total registros:", len(all_movies))

    # =============================
    # DEDUPLICAÇÃO
    # =============================

    latest_movies = {}

    for movie in all_movies:

        movie_id = movie["movie_id"]

        latest_movies[movie_id] = movie


    silver_movies = list(latest_movies.values())

    print("Filmes após deduplicar:", len(silver_movies))


    # =============================
    # LIMPEZA de release_date vazio
    # =============================

    silver_movies = [
        m for m in silver_movies
        if m.get("release_date")
    ]


    # =============================
    # CONVERTER PARA JSON LINES
    # =============================

    json_lines = "\n".join(json.dumps(m) for m in silver_movies)


    # =============================
    # SALVAR SILVER
    # =============================

    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")

    s3_key = f"silver/movies/movies.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print("Silver salva em:", s3_key)


if __name__ == "__main__":
    run_silver_table()