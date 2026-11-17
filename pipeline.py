"""
Pipeline de ingestão de Big Data: Livros Kindle (Kaggle) -> Parquet -> MongoDB
Avaliação N3 - Desenvolvimento com Linguagens Disruptivas I

Etapas (iguais às especificadas na N2):
  1. Captura        : baixa o CSV do Kaggle e salva em landing/ (original nunca é alterado)
  2. Leitura        : Spark lê o CSV com o esquema (tipos) declarado
  3. Validação      : aplica as regras; registros ruins vão para rejected/ com o motivo
  4. Transformação  : cria os atributos novos e organiza em subdocumentos
  5. Parquet        : grava o resultado limpo em curated/books/ particionado por ano
  6. MongoDB        : carrega a coleção books (upsert pelo asin) e gera categories e authors
  7. Índices e log  : cria índices e grava a execução em ingestion_runs

Uso:  python pipeline.py
"""
import csv
import glob
import os
import shutil
import sys
import time
from datetime import datetime, timezone

# Faz o Spark usar o mesmo Python do ambiente virtual
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# No Windows o Spark precisa do winutils.exe (HADOOP_HOME) para gravar arquivos.
# Se ele estiver em C:\hadoop\bin, o pipeline configura sozinho.
if os.name == "nt" and not os.environ.get("HADOOP_HOME") and os.path.exists(r"C:\hadoop\bin\winutils.exe"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pymongo import MongoClient, UpdateOne  # noqa: E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)

BASE = os.path.dirname(os.path.abspath(__file__))
LANDING = os.path.join(BASE, "landing")
CURATED = os.path.join(BASE, "curated", "books")
REJECTED = os.path.join(BASE, "rejected")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "kindle")
KAGGLE_DATASET = "asaniczka/amazon-kindle-books-dataset-2023-130k-books"
TAMANHO_LOTE = 5000

# Esquema declarado (ordem = ordem das colunas do CSV do Kaggle)
SCHEMA = StructType([
    StructField("asin", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("soldBy", StringType()),
    StructField("imgUrl", StringType()),
    StructField("productURL", StringType()),
    StructField("stars", DoubleType()),
    StructField("reviews", LongType()),
    StructField("price", DoubleType()),
    StructField("isKindleUnlimited", BooleanType()),
    StructField("category_id", IntegerType()),
    StructField("isBestSeller", BooleanType()),
    StructField("isEditorsPick", BooleanType()),
    StructField("isGoodReadsChoice", BooleanType()),
    StructField("publishedDate", StringType()),
    StructField("category_name", StringType()),
])


def etapa(numero, texto):
    print(f"\n[{numero}/7] {texto}", flush=True)


# ---------------------------------------------------------------- 1. CAPTURA
def capturar_csv():
    os.makedirs(LANDING, exist_ok=True)
    existentes = sorted(glob.glob(os.path.join(LANDING, "*.csv")))
    if existentes:
        print(f"    CSV já está em landing/: {os.path.basename(existentes[0])}")
        return existentes[0]

    print("    Baixando o dataset do Kaggle (pode demorar um pouco)...")
    import kagglehub
    pasta = kagglehub.dataset_download(KAGGLE_DATASET)
    csvs = glob.glob(os.path.join(pasta, "**", "*.csv"), recursive=True)
    if not csvs:
        raise RuntimeError("Nenhum CSV encontrado no dataset baixado.")
    destino = os.path.join(LANDING, os.path.basename(csvs[0]))
    shutil.copy(csvs[0], destino)  # copia: o original do cache não é mexido
    print(f"    Salvo em landing/{os.path.basename(destino)}")
    return destino


def conferir_cabecalho(caminho):
    esperado = [c.name for c in SCHEMA.fields]
    with open(caminho, encoding="utf-8-sig", newline="") as f:
        encontrado = next(csv.reader(f))
    if [c.strip() for c in encontrado] != esperado:
        raise RuntimeError(
            "As colunas do CSV não batem com o esquema do pipeline.\n"
            f"  Esperado  : {esperado}\n  Encontrado: {encontrado}"
        )
    print("    Cabeçalho do CSV conferido: as 16 colunas batem com o esquema.")


# ------------------------------------------------------- funções de limpeza
def limpa(coluna, padrao=None):
    """Tira espaços sobrando; se ficar vazio, usa o padrão (ou nulo)."""
    t = F.trim(F.regexp_replace(F.col(coluna), r"\s+", " "))
    vazio = t.isNull() | (t == "")
    if padrao is not None:
        return F.when(vazio, F.lit(padrao)).otherwise(t)
    return F.when(vazio, F.lit(None)).otherwise(t)


def chave_autor_spark(col):
    """Gera a chave do autor (sem acento, minúsculas, hífens) só com funções do Spark.

    Antes era uma UDF em Python, que funciona no Mac mas derruba o worker do Spark
    no Windows (erro "Python worker exited unexpectedly"). Com funções nativas,
    roda igual nos dois sistemas e ainda fica mais rápido.
    """
    s = F.translate(col, "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ",
                    "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN")
    s = F.regexp_replace(F.lower(s), "[^a-z0-9]+", "-")
    s = F.regexp_replace(s, "^-+|-+$", "")
    return F.when(s.isNull() | (s == ""), F.lit("desconhecido")).otherwise(s)


# ------------------------------------------------------------------- MAIN
def main():
    inicio = datetime.now(timezone.utc)
    t0 = time.time()
    run_id = "run-" + inicio.strftime("%Y%m%d-%H%M%S")
    print(f"=== Pipeline Kindle -> MongoDB | execução {run_id} ===")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception:
        sys.exit(
            "ERRO: não consegui conectar no MongoDB em " + MONGO_URI + ".\n"
            "Confira se o Docker Desktop está aberto e se o contêiner 'mongo-kindle' está rodando (docker ps)."
        )
    db = client[DB_NAME]

    m = {"rowsRead": 0, "rowsAccepted": 0, "rowsRejected": 0, "duplicatesRemoved": 0}
    status = "sucesso"
    spark = None
    arquivo = ""
    try:
        # 1. Captura
        etapa(1, "Captura do CSV")
        arquivo = capturar_csv()
        conferir_cabecalho(arquivo)

        spark = (
            SparkSession.builder.appName("kindle-pipeline").master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        # 2. Leitura com esquema definido
        etapa(2, "Leitura do CSV com esquema definido (Spark)")
        df = (
            spark.read.option("header", True).option("multiLine", True)
            .option("quote", '"').option("escape", '"').option("mode", "PERMISSIVE")
            .schema(SCHEMA).csv(arquivo)
        )
        df = df.withColumn("asin", F.trim("asin")).cache()
        m["rowsRead"] = df.count()
        print(f"    Registros lidos: {m['rowsRead']:,}")

        # 3. Validação e limpeza
        etapa(3, "Validação e limpeza")
        motivo = (
            F.when(F.col("asin").isNull() | (F.col("asin") == ""), "asin vazio")
            .when(F.col("stars").isNull() | (F.col("stars") < 0) | (F.col("stars") > 5),
                  "stars fora do intervalo de 0 a 5")
            .when(F.col("reviews").isNull() | (F.col("reviews") < 0), "reviews negativo ou invalido")
            .when(F.col("price").isNull() | (F.col("price") < 0), "price negativo ou invalido")
        )
        df = df.withColumn("motivo_rejeicao", motivo)
        rejeitados = df.filter(F.col("motivo_rejeicao").isNotNull())
        validos = df.filter(F.col("motivo_rejeicao").isNull()).drop("motivo_rejeicao")

        m["rowsRejected"] = rejeitados.count()
        rejeitados.coalesce(1).write.mode("overwrite").option("header", True).csv(REJECTED)
        print(f"    Rejeitados: {m['rowsRejected']:,} (gravados em rejected/ com o motivo)")
        if m["rowsRejected"]:
            rejeitados.groupBy("motivo_rejeicao").count().show(truncate=False)

        unicos = validos.dropDuplicates(["asin"]).cache()
        m["rowsAccepted"] = unicos.count()
        m["duplicatesRemoved"] = (m["rowsRead"] - m["rowsRejected"]) - m["rowsAccepted"]
        print(f"    asin repetidos removidos: {m['duplicatesRemoved']:,}")
        print(f"    Aceitos: {m['rowsAccepted']:,}")

        # 4. Transformação
        etapa(4, "Transformação (atributos novos e subdocumentos)")
        data = F.to_date(F.trim(F.col("publishedDate")), "yyyy-MM-dd")
        faixa = (
            F.when(F.col("price") == 0, "gratuito")
            .when(F.col("price") <= 5, "ate-5")
            .when(F.col("price") <= 10, "5-10")
            .when(F.col("price") <= 20, "10-20")
            .otherwise("acima-20")
        )
        chave_autor = chave_autor_spark
        nome_autor = limpa("author", "Desconhecido")
        carga = inicio.strftime("%Y-%m-%dT%H:%M:%SZ")

        final = unicos.select(
            F.col("asin").alias("_id"),
            limpa("title").alias("title"),
            F.struct(nome_autor.alias("name"), chave_autor(nome_autor).alias("authorKey")).alias("author"),
            limpa("soldBy", "Desconhecido").alias("seller"),
            F.col("imgUrl").alias("imageUrl"),
            F.col("productURL").alias("productUrl"),
            F.struct(
                F.col("price").alias("price"),
                faixa.alias("priceRange"),
                F.coalesce("isKindleUnlimited", F.lit(False)).alias("isKindleUnlimited"),
            ).alias("pricing"),
            F.struct(
                F.col("stars").alias("stars"),
                F.col("reviews").alias("reviews"),
                (F.col("reviews") > 0).alias("hasReviews"),
            ).alias("rating"),
            F.struct(
                F.coalesce("isBestSeller", F.lit(False)).alias("isBestSeller"),
                F.coalesce("isEditorsPick", F.lit(False)).alias("isEditorsPick"),
                F.coalesce("isGoodReadsChoice", F.lit(False)).alias("isGoodReadsChoice"),
            ).alias("badges"),
            F.struct(data.alias("date"), F.year(data).alias("year")).alias("publication"),
            F.struct(F.col("category_id").alias("id"), limpa("category_name").alias("name")).alias("category"),
            F.struct(F.lit(run_id).alias("runId"), F.lit(carga).alias("loadedAt")).alias("ingestion"),
            F.year(data).alias("publication_year"),  # só para particionar o Parquet
        )
        print("    Atributos criados: publication.year, pricing.priceRange, rating.hasReviews, "
              "author.authorKey, ingestion.runId/loadedAt")

        # 5. Parquet
        etapa(5, "Gravação em Parquet (curated/books/, particionado por ano)")
        final.write.mode("overwrite").partitionBy("publication_year").parquet(CURATED)
        pastas = [p for p in os.listdir(CURATED) if p.startswith("publication_year=")]
        print(f"    {len(pastas)} partições (uma pasta por ano de publicação) em curated/books/")

        # 6. MongoDB
        etapa(6, "Carga no MongoDB (upsert pelo asin) e geração de categories e authors")
        livros = spark.read.parquet(CURATED).drop("publication_year")
        books = db["books"]
        lote, enviados = [], 0
        for row in livros.toLocalIterator():
            doc = row.asDict(recursive=True)
            _id = doc.pop("_id")
            d = doc["publication"]["date"]
            if d is not None:  # o Mongo não aceita 'date' puro do Python
                doc["publication"]["date"] = datetime(d.year, d.month, d.day)
            lote.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))
            if len(lote) >= TAMANHO_LOTE:
                books.bulk_write(lote, ordered=False)
                enviados += len(lote)
                lote = []
                print(f"    ...{enviados:,} livros enviados", flush=True)
        if lote:
            books.bulk_write(lote, ordered=False)
            enviados += len(lote)
        print(f"    Livros carregados em books: {enviados:,}")

        media_com_nota = {"$avg": {"$cond": ["$rating.hasReviews", "$rating.stars", None]}}
        books.aggregate([
            {"$group": {
                "_id": "$category.id",
                "name": {"$first": "$category.name"},
                "totalBooks": {"$sum": 1},
                "avgPrice": {"$avg": "$pricing.price"},
                "avgStars": media_com_nota,
                "totalReviews": {"$sum": "$rating.reviews"},
                "pctKindleUnlimited": {"$avg": {"$cond": ["$pricing.isKindleUnlimited", 100, 0]}},
                "pctBestSeller": {"$avg": {"$cond": ["$badges.isBestSeller", 100, 0]}},
            }},
            {"$set": {
                "category_id": "$_id",
                "avgPrice": {"$round": ["$avgPrice", 2]},
                "avgStars": {"$round": ["$avgStars", 2]},
                "pctKindleUnlimited": {"$round": ["$pctKindleUnlimited", 1]},
                "pctBestSeller": {"$round": ["$pctBestSeller", 1]},
            }},
            {"$out": "categories"},
        ], allowDiskUse=True)
        print(f"    Coleção categories gerada: {db['categories'].count_documents({}):,} categorias")

        books.aggregate([
            {"$group": {
                "_id": "$author.authorKey",
                "name": {"$first": "$author.name"},
                "totalBooks": {"$sum": 1},
                "avgStars": media_com_nota,
                "totalReviews": {"$sum": "$rating.reviews"},
                "topBooks": {"$topN": {
                    "n": 3, "sortBy": {"rating.reviews": -1},
                    "output": {"title": "$title", "reviews": "$rating.reviews"},
                }},
            }},
            {"$set": {"autor_id": "$_id", "avgStars": {"$round": ["$avgStars", 2]}}},
            {"$out": "authors"},
        ], allowDiskUse=True)
        books.aggregate([
            {"$group": {"_id": {"a": "$author.authorKey", "c": "$category.name"}, "qtd": {"$sum": 1}}},
            {"$sort": {"qtd": -1}},
            {"$group": {"_id": "$_id.a",
                        "categories": {"$push": {"name": "$_id.c", "categoryQuantity": "$qtd"}}}},
            {"$merge": {"into": "authors", "on": "_id", "whenMatched": "merge", "whenNotMatched": "discard"}},
        ], allowDiskUse=True)
        print(f"    Coleção authors gerada: {db['authors'].count_documents({}):,} autores")

        # 7. Índices
        etapa(7, "Índices e registro da execução")
        books.create_index("author.authorKey")
        books.create_index("category.id")
        books.create_index("pricing.priceRange")
        books.create_index("publication.year")
        books.create_index([("rating.reviews", -1)])
        db["categories"].create_index("category_id")
        db["authors"].create_index("totalBooks")
        db["ingestion_runs"].create_index("run_id", unique=True)
        print("    Índices criados.")

    except Exception as erro:
        status = f"falha: {erro}"
        raise
    finally:
        fim = datetime.now(timezone.utc)
        db["ingestion_runs"].insert_one({
            "run_id": run_id,
            "sourceFile": os.path.basename(arquivo),
            "rowsRead": int(m["rowsRead"]),
            "rowsAccepted": int(m["rowsAccepted"]),
            "rowsRejected": int(m["rowsRejected"]),
            "duplicatesRemoved": int(m["duplicatesRemoved"]),
            "startedAt": inicio.replace(tzinfo=None),
            "finishedAt": fim.replace(tzinfo=None),
            "durationSeconds": round(time.time() - t0, 1),
            "status": status,
        })
        if spark is not None:
            spark.stop()
        print("\n=== RESUMO DA EXECUÇÃO ===")
        print(f"run_id    : {run_id}")
        print(f"status    : {status}")
        print(f"lidos     : {m['rowsRead']:,}")
        print(f"aceitos   : {m['rowsAccepted']:,}")
        print(f"rejeitados: {m['rowsRejected']:,}")
        print(f"duplicados: {m['duplicatesRemoved']:,}")
        print(f"duração   : {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
