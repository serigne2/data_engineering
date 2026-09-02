import os
import io
import boto3
import pandas as pd
import snowflake.connector
#from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
from botocore.client import Config
from snowflake.connector.pandas_tools import write_pandas



#BASE_DIR = Path(__file__).resolve().parent.parent
#load_dotenv(BASE_DIR / ".env")


# ============================================================
# 1. CONFIGURATION MINIO
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MINIO_BUCKET = "netflixdataset"
MINIO_PREFIX = "netflixmoviesdata/"


# ============================================================
# 2. CONFIGURATION SNOWFLAKE
# ============================================================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "NETFLIX")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")


# ============================================================
# 3. CONNEXION MINIO
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


# ============================================================
# 4. CONNEXION SNOWFLAKE
# ============================================================

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)


print("✓ Connexion à MinIO réussie")
print("✓ Connexion à Snowflake réussie")


# ============================================================
# 5. MAPPING FICHIERS → TABLES SNOWFLAKE
# ============================================================

files = {
    "movies.csv": "RAW_MOVIES",
    "ratings.csv": "RAW_RATINGS",
    "tags.csv": "RAW_TAGS",
    "genome-scores.csv": "RAW_GENOME_SCORES",
    "genome-tags.csv": "RAW_GENOME_TAGS",
    "links.csv": "RAW_LINKS",
}


# ============================================================
# 6. CRÉATION DES TABLES RAW
# ============================================================

table_definitions = {

    "RAW_MOVIES": """
        CREATE TABLE IF NOT EXISTS RAW_MOVIES (
            MOVIEID INTEGER,
            TITLE STRING,
            GENRES STRING
        )
    """,

    "RAW_RATINGS": """
        CREATE TABLE IF NOT EXISTS RAW_RATINGS (
            USERID INTEGER,
            MOVIEID INTEGER,
            RATING FLOAT,
            TIMESTAMP BIGINT
        )
    """,

    "RAW_TAGS": """
        CREATE TABLE IF NOT EXISTS RAW_TAGS (
            USERID INTEGER,
            MOVIEID INTEGER,
            TAG STRING,
            TIMESTAMP BIGINT
        )
    """,

    "RAW_GENOME_SCORES": """
        CREATE TABLE IF NOT EXISTS RAW_GENOME_SCORES (
            MOVIEID INTEGER,
            TAGID INTEGER,
            RELEVANCE FLOAT
        )
    """,

    "RAW_GENOME_TAGS": """
        CREATE TABLE IF NOT EXISTS RAW_GENOME_TAGS (
            TAGID INTEGER,
            TAG STRING
        )
    """,

    "RAW_LINKS": """
        CREATE TABLE IF NOT EXISTS RAW_LINKS (
            MOVIEID INTEGER,
            IMDBID INTEGER,
            TMDBID INTEGER
        )
    """
}


cursor = conn.cursor()

for table_name, sql in table_definitions.items():
    cursor.execute(sql)
    print(f"✓ Table {table_name} prête")


# ============================================================
# 7. CHARGEMENT DES FICHIERS MINIO → SNOWFLAKE
# ============================================================
for file_name, table_name in files.items():

    print(f"\n→ Chargement de {file_name}")
# --------------------------------------------------------
# Construire la clé complète du fichier dans MinIO
# --------------------------------------------------------
    object_key = f"{MINIO_PREFIX}{file_name}"
    print( f" Source : " f"s3://{MINIO_BUCKET}/{object_key}" )

    # Récupération du fichier depuis MinIO
    response = s3.get_object(
        Bucket=MINIO_BUCKET,
        Key=object_key
    )

    # Lecture du fichier en mémoire
    file_content = response["Body"].read()

    # Conversion CSV → DataFrame
    df = pd.read_csv(
        io.BytesIO(file_content)
    )

    print(f"  {len(df):,} lignes trouvées")

    # Nettoyage des noms de colonnes
    df.columns = [
        column.upper()
        for column in df.columns
    ]

    # Chargement dans Snowflake
    success, chunks, rows, output = write_pandas(
        conn,
        df,
        table_name,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=False,
        overwrite=True,
    )

    if success:
        print(
            f"✓ {rows:,} lignes chargées dans "
            f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
        )
    else:
        print(f"✗ Erreur lors du chargement de {file_name}")


# ============================================================
# 8. FERMETURE
# ============================================================

cursor.close()
conn.close()

print("\n======================================")
print("✓ Chargement terminé avec succès")
print("======================================")

