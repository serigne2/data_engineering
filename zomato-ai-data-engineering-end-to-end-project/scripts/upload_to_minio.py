import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


# ============================================================
# MinIO configuration
# ============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BUCKET = "zomatodataset"


# ============================================================
# Create S3/MinIO client
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


# ============================================================
# Create bucket if it doesn't exist
# ============================================================

try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"✓ Bucket '{BUCKET}' existe déjà.")

except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"✓ Bucket '{BUCKET}' créé.")


# ============================================================
# Files to upload
#
# Local file                  -> MinIO object
# ============================================================

files = {
    "food.csv": "raw/food/food.csv",
    "menu.csv": "raw/menu/menu.csv",
    "order_items.csv": "raw/order_items/order_items.csv",
    "orders.csv": "raw/orders/orders.csv",
    "restaurant.csv": "raw/restaurant/restaurant.csv",
    "reviews.csv": "raw/reviews/reviews.csv",
    "users.csv": "raw/users/users.csv",
}


# ============================================================
# Upload files
# ============================================================

for filename, object_key in files.items():

    #file_path = os.path.join("data", filename)
    #Il lui retrouvera les données dans n'importe quel lieu très pratique
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")

    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        print(f"✗ Fichier introuvable : {file_path}")
        continue

    print(f"Upload de {file_path}...")

    s3.upload_file(
        file_path,
        BUCKET,
        object_key,
    )

    print(f"  ✓ s3://{BUCKET}/{object_key}")


# ============================================================
# Verification
# ============================================================

print("\nVérification du contenu de MinIO :\n")

response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix="raw/"
)

if "Contents" in response:
    for obj in response["Contents"]:
        print(f"✓ {obj['Key']}")
else:
    print("Aucun fichier trouvé.")

print("\nUpload terminé.")