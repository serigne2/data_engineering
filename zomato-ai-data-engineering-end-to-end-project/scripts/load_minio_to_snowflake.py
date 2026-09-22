
import argparse
import os
import time
from pathlib import Path

import boto3
import snowflake.connector

from botocore.client import Config
from dotenv import load_dotenv
load_dotenv()
# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin",
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "zomatodataset",
)


SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]

SNOWFLAKE_WAREHOUSE = os.getenv(
    "SNOWFLAKE_WAREHOUSE",
    "ZOMATO_WH",
)

SNOWFLAKE_DATABASE = os.getenv(
    "SNOWFLAKE_DATABASE",
    "ZOMATO",
)

SNOWFLAKE_SCHEMA = os.getenv(
    "SNOWFLAKE_SCHEMA",
    "RAW",
)

SNOWFLAKE_FILE_FORMAT = os.getenv(
    "SNOWFLAKE_FILE_FORMAT",
    "ZOMATO.RAW.CSV_FMT",
)

SNOWFLAKE_STAGE = os.getenv(
    "SNOWFLAKE_STAGE",
    "ZOMATO.RAW.ZOMATO_INTERNAL_STAGE",
)

TMP_DIR = Path("/tmp")


# ============================================================
# Files to load
# ============================================================

FILES = {
    "restaurants": {
        "minio_key": "raw/restaurant/restaurant.csv",
        "table": "ZOMATO.RAW.RESTAURANTS",
    },
    "users": {
        "minio_key": "raw/users/users.csv",
        "table": "ZOMATO.RAW.USERS",
    },
    "food": {
        "minio_key": "raw/food/food.csv",
        "table": "ZOMATO.RAW.FOOD",
    },
    "menu": {
        "minio_key": "raw/menu/menu.csv",
        "table": "ZOMATO.RAW.MENU",
    },
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
    """Create an S3-compatible client for MinIO."""

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ============================================================
# Snowflake
# ============================================================

def create_snowflake_connection():
    """Create a Snowflake connection."""

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

# ============================================================
# Ingestion Log
# ============================================================

INGESTION_LOG_TABLE = "ZOMATO.RAW.INGESTION_LOG"


def check_ingestion_log(cursor, source_key, source_etag, file_size_bytes):
    """
    Check whether this exact MinIO file version has already been processed.
    """

    sql = f"""
        SELECT STATUS
        FROM {INGESTION_LOG_TABLE}
        WHERE SOURCE_KEY = %s
          AND SOURCE_ETAG = %s
          AND FILE_SIZE_BYTES = %s
          AND STATUS IN ('LOADED', 'SKIPPED')
        LIMIT 1
    """

    result = cursor.execute(
        sql,
        (
            source_key,
            source_etag,
            file_size_bytes,
        ),
    ).fetchone()

    return result is not None

def log_ingestion(
    cursor,
    source_key,
    file_name,
    target_table,
    file_size_bytes,
    source_etag,
    status,
    rows_loaded=None,
    error_message=None,
):
    sql = f"""
        MERGE INTO {INGESTION_LOG_TABLE} AS target
        USING (
            SELECT
                %s AS source_key,
                %s AS file_name,
                %s AS target_table,
                %s AS file_size_bytes,
                %s AS source_etag,
                %s AS status,
                %s AS rows_loaded,
                %s AS error_message
        ) AS source
        ON target.SOURCE_KEY = source.source_key
           AND target.SOURCE_ETAG = source.source_etag
           AND target.FILE_SIZE_BYTES = source.file_size_bytes

        WHEN MATCHED THEN UPDATE SET
            target.FILE_NAME = source.file_name,
            target.TARGET_TABLE = source.target_table,
            target.STATUS = source.status,
            target.ROWS_LOADED = source.rows_loaded,
            target.ERROR_MESSAGE = source.error_message,
            target.LOADED_AT = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT (
            SOURCE_KEY,
            FILE_NAME,
            TARGET_TABLE,
            FILE_SIZE_BYTES,
            SOURCE_ETAG,
            STATUS,
            ROWS_LOADED,
            ERROR_MESSAGE
        )
        VALUES (
            source.source_key,
            source.file_name,
            source.target_table,
            source.file_size_bytes,
            source.source_etag,
            source.status,
            source.rows_loaded,
            source.error_message
        )
    """

    cursor.execute(
        sql,
        (
            source_key,
            file_name,
            target_table,
            file_size_bytes,
            source_etag,
            status,
            rows_loaded,
            error_message,
        ),
    )
# ============================================================
# MinIO → Local
# ============================================================

def download_from_minio(s3_client, minio_key):
    """Download a MinIO object to /tmp."""

    filename = Path(minio_key).name
    local_path = TMP_DIR / filename

    print("\n→ MinIO → Local")
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
    """Upload a local file to the Snowflake internal stage."""

    print("\n→ PUT → Snowflake Internal Stage")
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
    """Load staged files into a RAW table."""

    print("\n→ COPY INTO Snowflake")
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

def load_file(s3_client, cursor, file_name, file_config):
    """
    Complete idempotent pipeline:

        MinIO
          ↓
        INGESTION_LOG check
          ↓
        /tmp
          ↓
        PUT
          ↓
        Snowflake Internal Stage
          ↓
        COPY INTO
          ↓
        RAW
          ↓
        INGESTION_LOG
    """

    minio_key = file_config["minio_key"]
    table = file_config["table"]

    stage_path = f"{SNOWFLAKE_STAGE}/{file_name}"

    print("\n" + "=" * 70)
    print(f"CHARGEMENT : {file_name}")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Check MinIO file metadata
    # --------------------------------------------------------

    print("\n→ Vérification du fichier dans MinIO")

    #head = s3_client.head_object(
    #    Bucket=MINIO_BUCKET,
    #    Key=minio_key,
    #)

    #file_size_bytes = head["ContentLength"]

    #print(f"  Source : s3://{MINIO_BUCKET}/{minio_key}")
    #print(f"  Taille : {file_size_bytes:,} bytes")
    head = s3_client.head_object(
    Bucket=MINIO_BUCKET,
    Key=minio_key,
    )

    file_size_bytes = head["ContentLength"]
    source_etag = head["ETag"].strip('"')

    print(f"  Source : s3://{MINIO_BUCKET}/{minio_key}")
    print(f"  Taille : {file_size_bytes:,} bytes")
    print(f"  ETag   : {source_etag}")

    # --------------------------------------------------------
    # 2. Check INGESTION_LOG
    # --------------------------------------------------------

    #already_processed = check_ingestion_log(
    #    cursor,
    #    minio_key,
    #)
    already_processed = check_ingestion_log(
        cursor,
        minio_key,
        source_etag,
        file_size_bytes,
    )

    if already_processed:

        print("\n✓ Fichier déjà traité → SKIP")
        print("  Aucun téléchargement effectué.")
        print("  Aucun PUT effectué.")
        print("  Aucun COPY effectué.")

        return None

    print("\n✓ Fichier non présent dans INGESTION_LOG")
    print("  → Nouveau chargement")

    total_start = time.perf_counter()

    local_file = None

    try:

        # ----------------------------------------------------
        # 3. MinIO → Local
        # ----------------------------------------------------

        local_file = download_from_minio(
            s3_client,
            minio_key,
        )

        # ----------------------------------------------------
        # 4. PUT → Snowflake Internal Stage
        # ----------------------------------------------------

        put_to_snowflake(
            cursor,
            local_file,
            stage_path,
        )

        # ----------------------------------------------------
        # 5. COPY INTO → RAW
        # ----------------------------------------------------

        copy_result = copy_into_snowflake(
            cursor,
            table,
            stage_path,
        )

        # ----------------------------------------------------
        # 6. Determine COPY result
        # ----------------------------------------------------

        rows_loaded = 0
        copy_status = "SKIPPED"

        if copy_result:

            for row in copy_result:

                print(f"  {row}")

                # Snowflake COPY result:
                # row[0] = file
                # row[1] = status
                # row[2] = rows_parsed
                # row[3] = rows_loaded

                if len(row) > 3 and row[3] is not None:
                    rows_loaded += int(row[3])

                if len(row) > 1:
                    copy_status = str(row[1])

        # ----------------------------------------------------
        # 7. Write INGESTION_LOG
        # ----------------------------------------------------

        if rows_loaded > 0:

            log_ingestion(
                cursor=cursor,
                source_key=minio_key,
                file_name=file_name,
                target_table=table,
                source_etag=source_etag,
                file_size_bytes=file_size_bytes,
                status="LOADED",
                rows_loaded=rows_loaded,
            )

            print(
                f"\n✓ INGESTION_LOG → LOADED "
                f"({rows_loaded:,} lignes)"
            )

        else:

            log_ingestion(
                cursor=cursor,
                source_key=minio_key,
                file_name=file_name,
                target_table=table,
                source_etag=source_etag,
                file_size_bytes=file_size_bytes,
                status="SKIPPED",
                rows_loaded=0,
            )

            print(
                "\n✓ INGESTION_LOG → SKIPPED "
                f"(COPY status: {copy_status})"
            )

        # ----------------------------------------------------
        # 8. Delete temporary file
        # ----------------------------------------------------

        if local_file:

            try:
                local_file.unlink()
                print(
                    f"\n✓ Fichier temporaire supprimé : "
                    f"{local_file}"
                )

            except OSError as error:

                print(
                    f"\n⚠ Impossible de supprimer "
                    f"{local_file}: {error}"
                )

        total_elapsed = time.perf_counter() - total_start

        print(
            f"\n✓ {file_name} terminé en "
            f"{total_elapsed / 60:.2f} min"
        )

        return copy_result

    except Exception as error:

        # ----------------------------------------------------
        # Error handling
        # ----------------------------------------------------

        print(f"\n✗ Erreur pendant le chargement : {error}")

        # IMPORTANT:
        # We do NOT write LOADED to INGESTION_LOG.
        # The file can therefore be retried.

        raise

# ============================================================
# Argument parsing
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Zomato data from MinIO to Snowflake RAW."
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--all",
        action="store_true",
        help="Load all Zomato source tables.",
    )

    group.add_argument(
        "--table",
        choices=FILES.keys(),
        help="Load one specific table.",
    )

    return parser.parse_args()


# ============================================================
# Main
# ============================================================

def main():

    args = parse_args()

    s3_client = create_minio_client()

    print("✓ Connexion MinIO configurée")

    conn = create_snowflake_connection()
    cursor = conn.cursor()

    print("✓ Connexion Snowflake OK")

    try:

        if args.all:
            tables_to_load = FILES.items()
        else:
            tables_to_load = [
                (args.table, FILES[args.table])
            ]

        for file_name, file_config in tables_to_load:

            load_file(
                s3_client,
                cursor,
                file_name,
                file_config,
            )

            conn.commit()

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

