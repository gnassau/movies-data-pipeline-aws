def s3_to_postgres():
    import awswrangler as wr
    import pg8000

    # 1️⃣ ler parquet da silver no S3
    df = wr.s3.read_parquet(
        path="s3://movies-raw-gustavo-portfolio/silver/movies/"
    )

    print(df.head())

    # 2️⃣ conectar postgres (pg8000)
    conn = pg8000.connect(
        host="postgres_analytics",
        port=5432,
        database="movies",
        user="admin",
        password="admin"
    )

    # 3️⃣ mandar dataframe pro postgres
    wr.postgresql.to_sql(
        df=df,
        table="movies",
        schema="silver",
        con=conn,
        mode="overwrite",   # recria a tabela
        index=False
    )

    # 4️⃣ fechar conexão
    print("Dataframe successfully loaded to PostgreSQL!")
    conn.close()

if __name__ == "__main__":
    s3_to_postgres()
