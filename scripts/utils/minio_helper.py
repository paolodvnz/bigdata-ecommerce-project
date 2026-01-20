"""
MinIO Helper Utilities
Upload and manage files on MinIO S3-compatible storage
"""
import os
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config.minio_config import (
    get_minio_client,
    create_bucket_if_not_exists,
    BUCKET_NAME
)


def upload_file(client: Minio, bucket: str, object_name: str, file_path: Path) -> bool:
    """
    Upload a single file to MinIO
    
    Args:
        client: MinIO client
        bucket: Bucket name
        object_name: Object name in bucket
        file_path: Local file path
        
    Returns:
        bool: True if successful
    """
    try:
        file_stat = file_path.stat()
        client.fput_object(
            bucket,
            object_name,
            str(file_path)
        )
        return True
    except S3Error as e:
        print(f"Error uploading {file_path.name}: {e}")
        return False


def upload_directory(
    client: Minio,
    bucket: str,
    local_dir: Path,
    prefix: str = "",
    pattern: str = "*.parquet"
) -> dict:
    """
    Upload all files from a directory to MinIO
    
    Args:
        client: MinIO client
        bucket: Bucket name
        local_dir: Local directory path
        prefix: Prefix for object names in bucket
        pattern: File pattern to match (default: *.parquet)
        
    Returns:
        dict: Statistics (success, failed, total_size)
    """
    files = list(local_dir.rglob(pattern))
    
    if not files:
        print(f"No files matching '{pattern}' found in {local_dir}")
        return {"success": 0, "failed": 0, "total_size": 0}
    
    print(f"\nUploading {len(files)} files from {local_dir}...")
    
    success_count = 0
    failed_count = 0
    total_size = 0
    
    with tqdm(total=len(files), desc="Uploading", unit="file") as pbar:
        for file_path in files:
            # Create object name preserving directory structure
            relative_path = file_path.relative_to(local_dir)
            object_name = f"{prefix}{relative_path}".replace("\\", "/")
            
            # Upload
            if upload_file(client, bucket, object_name, file_path):
                success_count += 1
                total_size += file_path.stat().st_size
            else:
                failed_count += 1
            
            pbar.update(1)
    
    total_size_mb = total_size / (1024 * 1024)
    
    print(f"Upload complete:")
    print(f"  - Success: {success_count}")
    print(f"  - Failed: {failed_count}")
    print(f"  - Total size: {total_size_mb:.2f} MB")
    
    return {
        "success": success_count,
        "failed": failed_count,
        "total_size": total_size_mb
    }


def list_bucket_objects(client: Minio, bucket: str, prefix: str = "") -> list:
    """
    List objects in bucket
    
    Args:
        client: MinIO client
        bucket: Bucket name
        prefix: Object prefix filter
        
    Returns:
        list: List of object names
    """
    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        print(f"Error listing objects: {e}")
        return []


def verify_upload(client: Minio, bucket: str, object_name: str) -> bool:
    """
    Verify if object exists in bucket
    
    Args:
        client: MinIO client
        bucket: Bucket name
        object_name: Object name to verify
        
    Returns:
        bool: True if exists
    """
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error:
        return False


def get_bucket_stats(client: Minio, bucket: str) -> dict:
    """
    Get bucket statistics
    
    Args:
        client: MinIO client
        bucket: Bucket name
        
    Returns:
        dict: Statistics (num_objects, total_size)
    """
    try:
        objects = list(client.list_objects(bucket, recursive=True))
        
        num_objects = len(objects)
        total_size = sum(obj.size for obj in objects)
        total_size_mb = total_size / (1024 * 1024)
        
        return {
            "num_objects": num_objects,
            "total_size_mb": total_size_mb
        }
    except S3Error as e:
        print(f"Error getting stats: {e}")
        return {"num_objects": 0, "total_size_mb": 0}


if __name__ == "__main__":
    # Test MinIO helper functions
    print("Testing MinIO helper functions...")
    
    # Create client
    client = get_minio_client()
    create_bucket_if_not_exists()
    
    # Get stats
    stats = get_bucket_stats(client, BUCKET_NAME)
    print(f"\nBucket stats:")
    print(f"  - Objects: {stats['num_objects']}")
    print(f"  - Total size: {stats['total_size_mb']:.2f} MB")
    
    print("\nMinIO helper test complete!")
