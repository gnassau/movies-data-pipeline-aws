def s3_to_postgres():
    import awswrangler as wr
    import pg8000
    import pandas as pd
    import json
    import math

    # =============================
    # 1️⃣ Ler parquet da Silver
    # =============================
    print("📥 Lendo dados do S3...")

    df = wr.s3.read_parquet(
        path="s3://movies-raw-gustavo-portfolio/silver/movies/"
    )

    print("✅ Dados carregados!")
    print(df.head())

    # =============================
    # 2️⃣ Ver dtypes
    # =============================
    print("\n📊 Dtypes antes do tratamento:")
    print(df.dtypes)

    # =============================
    # 3️⃣ Corrigir coluna genres
    # =============================
    print("\n🛠️ Convertendo coluna 'genres'...")

    def convert_genres(x):
        if x is None:
            return None

        # trata NaN
        if isinstance(x, float) and math.isnan(x):
            return None

        # converte lista/array para JSON string
        try:
            return json.dumps(list(x))
        except Exception:
            return None

    df["genres"] = df["genres"].apply(convert_genres)

    print("✅ Coluna 'genres' tratada!")

    # =============================
    # 4️⃣ Conectar no PostgreSQL
    # =============================
    print("\n🔌 Conectando no PostgreSQL...")

    conn = pg8000.connect(
        host="postgres_analytics",
        port=5432,
        database="movies",
        user="admin",
        password="admin"
    )

    print("✅ Conectado!")

    # teste rápido
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    print("🧪 Teste conexão:", cursor.fetchone())

    # =============================
    # 5️⃣ Enviar para PostgreSQL
    # =============================
    print("\n🚀 Iniciando carga no PostgreSQL...")

    wr.postgresql.to_sql(
        df=df,
        table="movies",
        schema="silver",
        con=conn,
        mode="overwrite",
        index=False,
        chunksize=1000  # 🔥 ESSENCIAL para não travar
    )

    print("✅ Dataframe carregado com sucesso!")

    # =============================
    # 6️⃣ Fechar conexão
    # =============================
    conn.close()
    print("🔒 Conexão encerrada.")


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    s3_to_postgres()