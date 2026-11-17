"""
Consultas de demonstração (saídas esperadas) sobre os dados carregados no MongoDB.
Uso:  python consultas.py
"""
import os
import sys

from pymongo import MongoClient

client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=5000)
try:
    client.admin.command("ping")
except Exception:
    sys.exit("ERRO: MongoDB não está acessível. Abra o Docker Desktop e confira 'docker ps'.")
db = client[os.getenv("MONGO_DB", "kindle")]


def fmt(v):
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int) and not isinstance(v, bool):
        return f"{v:,}"
    return "" if v is None else str(v)


def tabela(titulo, linhas, colunas):
    print(f"\n{titulo}\n" + "-" * len(titulo))
    if not linhas:
        print("(sem dados)")
        return
    larg = [max(len(c), *(len(fmt(l.get(c))) for l in linhas)) for c in colunas]
    print("  ".join(c.ljust(w) for c, w in zip(colunas, larg)))
    for l in linhas:
        print("  ".join(fmt(l.get(c)).ljust(w) for c, w in zip(colunas, larg)))


# 0. Visão geral
print("=== VISÃO GERAL ===")
for nome in ["books", "categories", "authors", "ingestion_runs"]:
    print(f"{nome:15s}: {db[nome].count_documents({}):,} documentos")

ultima = db["ingestion_runs"].find_one(sort=[("startedAt", -1)], projection={"_id": 0})
if ultima:
    tabela("Última execução do pipeline (ingestion_runs)", [ultima],
           ["run_id", "rowsRead", "rowsAccepted", "rowsRejected", "duplicatesRemoved",
            "durationSeconds", "status"])

# 1. Desempenho por categoria
tabela("1) Categorias com melhor nota média (mínimo de 100 livros)",
       list(db.categories.find({"totalBooks": {"$gte": 100}}, {"_id": 0})
            .sort("avgStars", -1).limit(10)),
       ["name", "totalBooks", "avgStars", "avgPrice", "totalReviews"])

# 2. Efeito do Kindle Unlimited
tabela("2) Kindle Unlimited x nota e avaliações (só livros com avaliação)",
       list(db.books.aggregate([
           {"$match": {"rating.hasReviews": True}},
           {"$group": {"_id": "$pricing.isKindleUnlimited", "livros": {"$sum": 1},
                       "notaMedia": {"$avg": "$rating.stars"},
                       "avaliacoesMedias": {"$avg": "$rating.reviews"}}},
       ])),
       ["_id", "livros", "notaMedia", "avaliacoesMedias"])

# 3. Preço e selos
ordem = {"gratuito": 0, "ate-5": 1, "5-10": 2, "10-20": 3, "acima-20": 4}
faixas = list(db.books.aggregate([
    {"$group": {"_id": "$pricing.priceRange", "livros": {"$sum": 1},
                "maisVendido": {"$sum": {"$cond": ["$badges.isBestSeller", 1, 0]}},
                "escolhaEditores": {"$sum": {"$cond": ["$badges.isEditorsPick", 1, 0]}},
                "goodreads": {"$sum": {"$cond": ["$badges.isGoodReadsChoice", 1, 0]}}}},
]))
faixas.sort(key=lambda x: ordem.get(x["_id"], 99))
tabela("3) Faixa de preço x selos", faixas,
       ["_id", "livros", "maisVendido", "escolhaEditores", "goodreads"])

# 4. Evolução das publicações
anos = list(db.books.aggregate([
    {"$match": {"publication.year": {"$ne": None}}},
    {"$group": {"_id": "$publication.year", "livros": {"$sum": 1}}},
    {"$sort": {"_id": -1}}, {"$limit": 10},
]))
anos.sort(key=lambda x: x["_id"])
tabela("4) Livros publicados por ano (últimos 10 anos com dados)", anos, ["_id", "livros"])

# 5. Ranking de autores
tabela("5) Autores com boa nota (mínimo 3 livros e 1.000 avaliações)",
       list(db.authors.find({"totalBooks": {"$gte": 3}, "totalReviews": {"$gte": 1000}}, {"_id": 0})
            .sort([("avgStars", -1), ("totalReviews", -1)]).limit(10)),
       ["name", "totalBooks", "avgStars", "totalReviews"])

print("\nFim das consultas.")
