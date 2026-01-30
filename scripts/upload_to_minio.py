"""
Upload E-commerce Dataset to MinIO
Uploads FULL dataset (100M transactions) to S3-compatible MinIO storage.
SAMPLE dataset remains local for quick testing.
"""
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from config.minio_config import (
    get_minio_client,
    create_bucket_if_not_exists,
    BUCKET_NAME,
    RAW_DATA_PREFIX,
)
from scripts.utils.minio_helper import (
    upload_file,
    upload_directory,
    get_bucket_stats,
    list_bucket_objects
)


# ===========================
# CONFIGURATION
# ===========================

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"


# ===========================
# UPLOAD FUNCTIONS
# ===========================

def upload_dataset(client, source_dir: Path, prefix: str, dataset_name: str):
    """
    Upload dataset to MinIO
    
    Args:
        client: MinIO client
        source_dir: Source directory (RAW_DIR)
        prefix: Prefix for MinIO objects (e.g., "raw/")
        dataset_name: Name for logging (e.g., "RAW" or "SAMPLE")
    
    Returns:
        dict: Upload statistics
    """
    print("\n" + "="*60)
    print(f"UPLOADING {dataset_name} DATASET")
    print("="*60)
    print(f"  Source: {source_dir}")
    print(f"  Destination: s3a://{BUCKET_NAME}/{prefix}")
    
    if not source_dir.exists():
        print(f"{dataset_name} directory not found, skipping...")
        return {"success": 0, "failed": 0, "total_size": 0}
    
    total_stats = {"success": 0, "failed": 0, "total_size": 0}
    
    # Upload customers
    customers_file = source_dir / "customers.parquet"
    if customers_file.exists():
        print("\n[1/3] Uploading customers...")
        object_name = f"{prefix}customers.parquet"
        if upload_file(client, BUCKET_NAME, object_name, customers_file):
            print(f"   - Uploaded: {object_name}")
            total_stats["success"] += 1
            total_stats["total_size"] += customers_file.stat().st_size / (1024*1024)
        else:
            total_stats["failed"] += 1
    else:
        print("  - customers.parquet not found, skipping...")
    
    # Upload products
    products_file = source_dir / "products.parquet"
    if products_file.exists():
        print("\n[2/3] Uploading products...")
        object_name = f"{prefix}products.parquet"
        if upload_file(client, BUCKET_NAME, object_name, products_file):
            print(f"   - Uploaded: {object_name}")
            total_stats["success"] += 1
            total_stats["total_size"] += products_file.stat().st_size / (1024*1024)
        else:
            total_stats["failed"] += 1
    else:
        print("  - products.parquet not found, skipping...")
    
    # Upload transactions (directory with partitions)
    transactions_dir = source_dir / "transactions"
    if transactions_dir.exists():
        print(f"\n[3/3] Uploading transactions (partitioned)...")
        stats = upload_directory(
            client,
            BUCKET_NAME,
            transactions_dir,
            prefix=f"{prefix}transactions/",
            pattern="*.parquet"
        )
        total_stats["success"] += stats["success"]
        total_stats["failed"] += stats["failed"]
        total_stats["total_size"] += stats["total_size"]
    else:
        print("  - transactions directory not found, skipping...")
    
    print("\n" + "-"*60)
    print(f"{dataset_name} upload summary:")
    print(f"  - Files uploaded: {total_stats['success']}")
    print(f"  - Failed: {total_stats['failed']}")
    print(f"  - Total size: {total_stats['total_size']:.2f} MB")
    
    return total_stats


# ===========================
# VERIFICATION
# ===========================

def verify_uploads(client):
    """Verify uploaded files in MinIO"""
    
    print("\n" + "="*60)
    print("VERIFYING UPLOADS")
    print("="*60)
    
    # List all objects
    print("\nListing bucket contents...")
    objects = list_bucket_objects(client, BUCKET_NAME)
    
    if not objects:
        print("  - No objects found in bucket!")
        return False
    
    print(f"\n  - Found {len(objects)} objects in bucket:")
    
    # Group by prefix
    prefixes = {}
    for obj in objects:
        prefix = obj.split('/')[0]
        if prefix not in prefixes:
            prefixes[prefix] = []
        prefixes[prefix].append(obj)
    
    for prefix, files in prefixes.items():
        print(f"\n   {prefix}/")
        if len(files) <= 10:
            for file in sorted(files):
                print(f"      - {file}")
        else:
            for file in sorted(files)[:5]:
                print(f"      - {file}")
            print(f"      ... and {len(files)-5} more files")
    
    # Get bucket statistics
    print("\n" + "-"*60)
    stats = get_bucket_stats(client, BUCKET_NAME)
    print(f"\nBucket statistics:")
    print(f"  - Total objects: {stats['num_objects']:,}")
    print(f"  - Total size: {stats['total_size_mb']:.2f} MB ({stats['total_size_mb']/1024:.2f} GB)")
    
    return True


# ===========================
# MAIN
# ===========================

def main():
    """
    Main execution - uploads FULL dataset to MinIO
    
    This script automatically uploads the FULL dataset (100M transactions)
    to MinIO without user interaction. The SAMPLE dataset remains local
    for quick testing purposes.
    """
    
    print("="*60)
    print("MINIO UPLOAD SCRIPT - FULL DATASET")
    print("="*60)
    print(f"\nStart time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize MinIO
    print("\n" + "="*60)
    print("INITIALIZING MINIO")
    print("="*60)
    
    try:
        client = get_minio_client()
        print("  - MinIO client created")
        
        create_bucket_if_not_exists(BUCKET_NAME)
        print(f"  - Bucket '{BUCKET_NAME}' ready")
    except Exception as e:
        print(f"Failed to initialize MinIO: {e}")
        print("\nMake sure MinIO is running:")
        print("  - docker compose up -d")
        sys.exit(1)
    
    # Upload FULL dataset only
    print("\n" + "="*60)
    print("UPLOADING FULL DATASET")
    print("="*60)
    print("\nUploading FULL dataset (100M transactions) to MinIO...")
    print("SAMPLE dataset will remain local for quick testing.\n")
    
    start_time = datetime.now()
    
    try:
        # Upload FULL dataset (RAW_DIR -> raw/ prefix)
        stats = upload_dataset(
            client=client,
            source_dir=RAW_DIR,
            prefix=RAW_DATA_PREFIX,  # "raw/"
            dataset_name="FULL"
        )
        
        # Verify uploads
        verify_uploads(client)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print("UPLOAD COMPLETE")
        print("="*60)
        print(f"\nUpload completed in {duration:.1f} seconds")
        print(f"  - Files uploaded: {stats['success']}")
        print(f"  - Failed: {stats['failed']}")
        print(f"  - Total size: {stats['total_size']:.2f} MB")
        print(f"\nEnd time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n" + "="*60)
        print("DATASET LOCATION SUMMARY")
        print("="*60)
        print(f"\nFULL dataset:   s3a://{BUCKET_NAME}/{RAW_DATA_PREFIX}  (MinIO)")
        
    except KeyboardInterrupt:
        print("\n\nUpload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError during upload: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()