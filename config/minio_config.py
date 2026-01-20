"""
MinIO Configuration for S3-Compatible Storage
"""
import os
from minio import Minio
from minio.error import S3Error

# MinIO Connection Settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Bucket Configuration
BUCKET_NAME = "bigdata-ecommerce"

# Paths
RAW_DATA_PREFIX = "raw/"
PROCESSED_DATA_PREFIX = "processed/"
DELTA_LAKE_PREFIX = "delta-lake/"
STREAMING_PREFIX = "streaming/"


def get_minio_client():
    """
    Create and return MinIO client
    
    Returns:
        Minio: Configured MinIO client
    """
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def create_bucket_if_not_exists(bucket_name=BUCKET_NAME):
    """
    Create bucket if it doesn't exist
    
    Args:
        bucket_name (str): Name of the bucket to create
    """
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully")
        else:
            print(f"ℹBucket '{bucket_name}' already exists")
    except S3Error as e:
        print(f"Error creating bucket: {e}")


def get_s3a_path(prefix="", filename=""):
    """
    Generate s3a:// path for Spark
    
    Args:
        prefix (str): Path prefix (e.g., "raw/")
        filename (str): Filename
        
    Returns:
        str: Complete s3a:// path
    """
    path = f"s3a://{BUCKET_NAME}/{prefix}{filename}"
    return path.rstrip('/')


if __name__ == "__main__":
    # Test connection
    print("Testing MinIO connection...")
    create_bucket_if_not_exists()
    print(f"S3A path example: {get_s3a_path('raw/', 'test.parquet')}")