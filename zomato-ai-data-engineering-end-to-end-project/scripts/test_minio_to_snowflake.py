import os
import time
from pathlib import Path

import boto3
import snowflake.connector

from botocore.client import Config
from dotenv import load_dotenv


# ============================================================
# Environment
# ============================================================

load_dotenv()


# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "zomatodataset")

SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]

SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "ZOMATO_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ZOMATO")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

SNOWFLAKE_FILE_FORMAT = os.getenv(
    "SNOWFLAKE_FILE_FORMAT",
    "ZOMATO.RAW.CSV_FMT"
)

SNOWFLAKE_STAGE = os.getenv(
    "SNOWFLAKE_STAGE",
    "ZOMATO.RAW.ZOMATO_INTERNAL_STAGE"
)

TMP_DIR = Path("/tmp")


# ============================================================
# Files to load
# ============================================================

FILES = {
    "orders": {
        "minio_key": "raw/orders/orders.csv",
        "table": "ZOMATO.RAW.ORDERS",
    },
    "order_items": {
        "minio_key": "raw/order_items/order_items.csv",
        "table": "ZOMATO.RAW.ORDER_ITEMS",
    },
    "reviews": {
        "minio_key": "raw/reviews/reviews.csv",
        "table": "ZOMATO.RAW.REVIEWS",
    },
}


# ============================================================
# MinIO
# ============================================================

def create_minio_client():
    """Create and return a MinIO/S3-compatible client."""

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    return client


# ============================================================
# Snowflake
# ============================================================

def create_snowflake_connection():
    """Create and return a Snowflake connection."""

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


# ============================================================
# MinIO → Local
# ============================================================

def download_from_minio(s3_client, minio_key):
    """
    Download a file from MinIO to /tmp.

    Returns:
        Path to the downloaded local file.
    """

    filename = Path(minio_key).name
    local_path = TMP_DIR / filename

    print(f"\n→ MinIO → Local")
    print(f"  Source      : s3://{MINIO_BUCKET}/{minio_key}")
    print(f"  Destination : {local_path}")

    start = time.perf_counter()

    s3_client.download_file(
        MINIO_BUCKET,
        minio_key,
        str(local_path),
    )

    elapsed = time.perf_counter() - start

    size_mb = local_path.stat().st_size / (1024 ** 2)
    size_gb = size_mb / 1024

    print(
        f"  ✓ Téléchargement terminé : "
        f"{size_mb:,.1f} MB ({size_gb:.2f} GB)"
    )
    print(f"  ✓ Temps : {elapsed / 60:.2f} min")

    return local_path


# ============================================================
# PUT → Snowflake Internal Stage
# ============================================================

def put_to_snowflake(cursor, local_file, stage_path):
    """
    Upload a local file to a Snowflake internal stage.
    """

    print(f"\n→ PUT → Snowflake Internal Stage")
    print(f"  File  : {local_file}")
    print(f"  Stage : @{stage_path}")

    start = time.perf_counter()

    put_sql = f"""
        PUT file://{local_file}
        @{stage_path}
        AUTO_COMPRESS=TRUE
        PARALLEL=8
    """

    result = cursor.execute(put_sql).fetchall()

    elapsed = time.perf_counter() - start

    print(f"  ✓ PUT terminé en {elapsed / 60:.2f} min")

    for row in result:
        print(f"  {row}")

    return result


# ============================================================
# COPY INTO → Snowflake RAW
# ============================================================

def copy_into_snowflake(cursor, table, stage_path):
    """
    Load staged files into a Snowflake table using COPY INTO.
    """

    print(f"\n→ COPY INTO Snowflake")
    print(f"  Source      : @{stage_path}/")
    print(f"  Destination : {table}")

    start = time.perf_counter()

    copy_sql = f"""
        COPY INTO {table}
        FROM @{stage_path}/
        FILE_FORMAT = (
            FORMAT_NAME = '{SNOWFLAKE_FILE_FORMAT}'
        )
        ON_ERROR = 'ABORT_STATEMENT'
    """

    result = cursor.execute(copy_sql).fetchall()

    elapsed = time.perf_counter() - start

    print(f"  ✓ COPY terminé en {elapsed / 60:.2f} min")

    for row in result:
        print(f"  {row}")

    return result


# ============================================================
# Load one file
# ============================================================

def load_file(s3_client, cursor, file_config):
    """
    Complete loading pipeline:

        MinIO
          ↓
        Local /tmp
          ↓
        PUT
          ↓
        Snowflake Internal Stage
          ↓
        COPY INTO
          ↓
        Snowflake RAW
    """

    minio_key = file_config["minio_key"]
    table = file_config["table"]

    file_name = Path(minio_key).stem

    stage_path = (
        f"{SNOWFLAKE_STAGE}/{file_name}"
    )

    print("\n" + "=" * 70)
    print(f"CHARGEMENT : {file_name}")
    print("=" * 70)

    total_start = time.perf_counter()

    # --------------------------------------------------------
    # 1. MinIO → Local
    # --------------------------------------------------------

    local_file = download_from_minio(
        s3_client,
        minio_key,
    )

    # --------------------------------------------------------
    # 2. Local → Snowflake Stage
    # --------------------------------------------------------

    put_to_snowflake(
        cursor,
        local_file,
        stage_path,
    )

    # --------------------------------------------------------
    # 3. Snowflake Stage → RAW
    # --------------------------------------------------------

    copy_result = copy_into_snowflake(
        cursor,
        table,
        stage_path,
    )

    conn.commit()

    total_elapsed = time.perf_counter() - total_start

    # --------------------------------------------------------
    # 4. Cleanup
    # --------------------------------------------------------

    try:
        local_file.unlink()
        print(f"\n✓ Fichier temporaire supprimé : {local_file}")
    except OSError as error:
        print(
            f"\n⚠ Impossible de supprimer {local_file}: "
            f"{error}"
        )

    print(
        f"\n✓ {file_name} terminé en "
        f"{total_elapsed / 60:.2f} min"
    )

    return copy_result


# ============================================================
# Main
# ============================================================

def main():

    global conn

    s3_client = create_minio_client()

    print("✓ Connexion MinIO OK")

    conn = create_snowflake_connection()
    cursor = conn.cursor()

    print("✓ Connexion Snowflake OK")

    try:

        # ----------------------------------------------------
        # Choose the files to load
        # ----------------------------------------------------

        # Exemple : charger uniquement reviews
        load_file(
            s3_client,
            cursor,
            FILES["reviews"],
        )

        # Pour charger order_items :
        #
        # load_file(
        #     s3_client,
        #     cursor,
        #     FILES["order_items"],
        # )

        # Pour charger orders :
        #
        # load_file(
        #     s3_client,
        #     cursor,
        #     FILES["orders"],
        # )

    except Exception:
        conn.rollback()
        raise

    finally:

        cursor.close()
        conn.close()

        print("\n✓ Connexion Snowflake fermée")


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()