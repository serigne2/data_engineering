import os
import boto3
from botocore.client import Config

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

bucket = "netflixdataset"

# Création du bucket s'il n'existe pas
try:
    s3.head_bucket(Bucket=bucket)
    print(f"Bucket '{bucket}' existe déjà.")
except Exception:
    s3.create_bucket(Bucket=bucket)
    print(f"Bucket '{bucket}' créé.")

files = [
    "data/genome-scores.csv",
    "data/ratings.csv",
]

for file_path in files:
    file_name = os.path.basename(file_path)

    print(f"Upload de {file_path}...")

    s3.upload_file(
        file_path,
        bucket,
        f"netflixmoviesdata/{file_name}",
    )

    print(f"✓ {file_name} envoyé dans s3://{bucket}/netflixmoviesdata/")