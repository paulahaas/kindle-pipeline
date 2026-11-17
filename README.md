# Pipeline de Big Data: Livros Kindle (Kaggle) → Parquet → MongoDB

Avaliação N3 – Desenvolvimento com Linguagens Disruptivas I
Equipe: Yasmin Ferreira, Paula Haas, Gustavo Correia da Silveira, Marcella Cagliari

Dataset: [Amazon Kindle Books Dataset 2023 (130k books)](https://www.kaggle.com/datasets/asaniczka/amazon-kindle-books-dataset-2023-130k-books)

## O que o pipeline faz

1. **Captura**: baixa o CSV do Kaggle para `landing/` (o original nunca é alterado)
2. **Leitura**: Spark lê o CSV com os tipos das colunas declarados
3. **Validação**: registros inválidos vão para `rejected/` com o motivo
4. **Transformação**: cria `publication.year`, `pricing.priceRange`, `rating.hasReviews`, `author.authorKey`, `ingestion.runId/loadedAt`
5. **Parquet**: grava em `curated/books/`, particionado por ano de publicação
6. **MongoDB**: carrega `books` (upsert pelo `asin`) e gera `categories` e `authors` por agregação
7. **Índices e log**: cria índices e grava a execução em `ingestion_runs`

## Requisitos

Funciona no **Windows** e no **Mac**. Instale pelos sites oficiais:

- **Python 3.12** (ex.: 3.12.10): https://www.python.org/downloads/release/python-31210/ — o Spark não funciona com Python 3.13 ou 3.14
- **Java 17** (Temurin JDK): https://adoptium.net/temurin/releases/ (escolher o seu sistema e a versão 17)
- **Docker Desktop**: https://www.docker.com/products/docker-desktop/
- **Só no Windows: winutils.** O Spark precisa dele para gravar arquivos no Windows. Baixe o `winutils.exe` e o `hadoop.dll` da versão Hadoop 3.3 (repositório [cdarlint/winutils](https://github.com/cdarlint/winutils), pasta `hadoop-3.3.5/bin`) e coloque os dois em `C:\hadoop\bin`. O `pipeline.py` encontra essa pasta sozinho.

## Como executar

### Windows (PowerShell)

```powershell
# 1. Subir o MongoDB (Docker Desktop precisa estar aberto)
docker run -d --name mongo-kindle -p 27017:27017 -v mongo-kindle-data:/data/db mongo:7
# (se o contêiner já existir, use: docker start mongo-kindle)

# 2. Entrar na pasta do projeto
cd caminho\para\kindle-pipeline

# 3. Criar e ativar o ambiente virtual
py -3.12 -m venv .venv
.venv\Scripts\activate
# (se o PowerShell bloquear: Set-ExecutionPolicy -Scope Process Bypass)

# 4. Instalar as dependências
pip install -r requirements.txt

# 5. Rodar o pipeline (baixa o dataset sozinho na primeira vez)
python pipeline.py

# 6. Ver as saídas esperadas
python consultas.py
```

### Mac (Terminal)

```bash
docker run -d --name mongo-kindle -p 27017:27017 -v mongo-kindle-data:/data/db mongo:7
cd caminho/para/kindle-pipeline
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py
python consultas.py
```

Se o download automático falhar, baixe o CSV manualmente no Kaggle (botão **Download**), descompacte e coloque o arquivo `.csv` dentro da pasta `landing/`. Depois rode `python pipeline.py` de novo.

### Usando MongoDB Atlas em vez do Docker

```bash
export MONGO_URI="mongodb+srv://USUARIO:SENHA@cluster.mongodb.net"
python pipeline.py
```

## Saídas esperadas

| Onde | O que aparece |
|---|---|
| Terminal | Etapas 1/7 a 7/7 e o resumo (lidos, aceitos, rejeitados, duplicados, duração) |
| `landing/` | CSV original |
| `curated/books/` | Uma pasta `publication_year=AAAA` por ano, com arquivos `.parquet` |
| `rejected/` | CSV com os registros rejeitados e a coluna `motivo_rejeicao` |
| MongoDB, banco `kindle` | Coleções `books`, `categories`, `authors`, `ingestion_runs` |
| `python consultas.py` | Tabelas de categorias, Kindle Unlimited, preço x selos, publicações por ano e ranking de autores |

O pipeline pode ser executado várias vezes: o upsert pelo `asin` evita duplicar livros.

**Observação sobre Windows:** a chave do autor (`author.authorKey`) é gerada só com funções nativas do Spark, sem UDF em Python. A versão com UDF derrubava o worker do Spark no Windows (erro *Python worker exited unexpectedly*).

## Estrutura

```
kindle-pipeline/
├── pipeline.py        # pipeline completo
├── consultas.py       # consultas de demonstração
├── requirements.txt
├── evidencias/        # prints da execução (MongoDB Compass e consultas)
├── landing/           # CSV original (criada na execução)
├── curated/books/     # Parquet limpo (criada na execução)
└── rejected/          # registros rejeitados (criada na execução)
```
