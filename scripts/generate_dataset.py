"""
Generate E-commerce Dataset
- SAMPLE: CSV only (customers.csv, products.csv, transactions.csv)
- FULL: Parquet only with partitioned transactions (year/month)
"""
import os
import sys
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from scripts.utils.data_generator import (
    generate_customers,
    generate_products,
    generate_transactions_batch,
    validate_customers,
    validate_products,
    validate_transactions
)

# ===========================
# CONFIGURATION
# ===========================

# FULL Dataset Parameters
FULL_CUSTOMERS = 1_000_000
FULL_PRODUCTS = 50_000
FULL_TRANSACTIONS = 100_000_000
BATCH_SIZE_FULL = 1_000_000   # 1M transactions per batch
NUM_BATCHES_FULL = FULL_TRANSACTIONS // BATCH_SIZE_FULL    # 100 batches

# SAMPLE Dataset Parameters (for Notebook 1 - Pandas Limits)
SAMPLE_CUSTOMERS = 20_000
SAMPLE_PRODUCTS = 1_000
SAMPLE_TRANSACTIONS = 20_000_000
BATCH_SIZE_SAMPLE = 1_000_000   # 1M transactions per batch
NUM_BATCHES_SAMPLE = SAMPLE_TRANSACTIONS // BATCH_SIZE_SAMPLE    # 20 batches

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "sample"
SCHEMAS_DIR = DATA_DIR / "schemas"

# Create directories
RAW_DIR.mkdir(parents=True, exist_ok=True)
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)

# ===========================
# HELPER FUNCTIONS
# ===========================

def save_parquet(df: pd.DataFrame, filepath: Path, compression='snappy'):
    """Save dataframe as Parquet with Spark-compatible timestamps"""
    # Convert datetime columns to microseconds (Spark compatible)
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].astype('datetime64[us]')
    
    df.to_parquet(filepath, engine='pyarrow', compression=compression, index=False)

def save_csv(df: pd.DataFrame, filepath: Path):
    """Save dataframe as CSV"""
    df.to_csv(filepath, index=False)

# ===========================
# GENERATE DATASET 
# ===========================

def generate_dataset(
    num_customers: int,
    num_products: int,
    num_transactions: int,
    batch_size: int,
    output_dir: Path,
    dataset_name: str = "dataset",
    format_type: str = "csv"  # "csv" or "parquet_partitioned"
):
    """
    Generate dataset with specified parameters
    
    Args:
        num_customers: Number of customers
        num_products: Number of products
        num_transactions: Total number of transactions
        batch_size: Transactions per batch
        output_dir: Output directory (RAW_DIR or SAMPLE_DIR)
        dataset_name: Name for logging (FULL or SAMPLE)
        format_type: Output format - "csv" for SAMPLE, "parquet_partitioned" for FULL
    """
    
    print("\n" + "="*60)
    print(f"GENERATING {dataset_name} DATASET")
    print("="*60)
    print(f"  Customers: {num_customers:,}")
    print(f"  Products: {num_products:,}")
    print(f"  Transactions: {num_transactions:,}")
    print(f"  Batch size: {batch_size:,}")
    print(f"  Format: {format_type}")
    print(f"  Output: {output_dir}")
    
    num_batches = num_transactions // batch_size
    
    # Step 1: Generate Customers
    print(f"\n[1/3] Generating {num_customers:,} customers...")
    customers_df = generate_customers(num_customers)
    validate_customers(customers_df)
    
    if format_type == "csv":
        customers_file = output_dir / "customers.csv"
        save_csv(customers_df, customers_file)
        print(f"   Saved: {customers_file.name}")
    else:  # parquet_partitioned
        customers_file = output_dir / "customers.parquet"
        save_parquet(customers_df, customers_file)
        print(f"   Saved: {customers_file.name}")
    
    # Step 2: Generate Products
    print(f"\n[2/3] Generating {num_products:,} products...")
    products_df = generate_products(num_products)
    validate_products(products_df)
    
    if format_type == "csv":
        products_file = output_dir / "products.csv"
        save_csv(products_df, products_file)
        print(f"   Saved: {products_file.name}")
    else:  # parquet_partitioned
        products_file = output_dir / "products.parquet"
        save_parquet(products_df, products_file)
        print(f"   Saved: {products_file.name}")
    
    # Step 3: Generate Transactions
    print(f"\n[3/3] Generating {num_transactions:,} transactions ({num_batches:,} batches)...")
    
    if format_type == "csv":
        # CSV: Single flat file with append mode
        generate_transactions_csv(
            num_batches=num_batches,
            batch_size=batch_size,
            customers_df=customers_df,
            products_df=products_df,
            output_dir=output_dir,
            dataset_name=dataset_name
        )
    else:  # parquet_partitioned
        # Parquet: Partitioned by year/month
        generate_transactions_parquet_partitioned(
            num_batches=num_batches,
            batch_size=batch_size,
            customers_df=customers_df,
            products_df=products_df,
            output_dir=output_dir,
            dataset_name=dataset_name
        )
    
    print(f"\n{dataset_name} dataset generation complete!")
    print(f"  - Customers: {num_customers:,} records")
    print(f"  - Products: {num_products:,} records")
    print(f"  - Transactions: {num_transactions:,} records")
    print(f"  - Location: {output_dir}")


def generate_transactions_csv(
    num_batches: int,
    batch_size: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    output_dir: Path,
    dataset_name: str
):
    """
    Generate transactions as single CSV file (for SAMPLE dataset)
    
    Args:
        num_batches: Number of batches to generate
        batch_size: Transactions per batch
        customers_df: Customer dataframe
        products_df: Product dataframe
        output_dir: Output directory
        dataset_name: Dataset name for logging
    """
    transactions_csv = output_dir / "transactions.csv"
    
    # Remove existing file if present
    if transactions_csv.exists():
        transactions_csv.unlink()
    
    all_transactions = []
    first_batch = True
    
    for batch_num in tqdm(range(num_batches), desc="Generating batches", unit="batch"):
        start_id = batch_num * batch_size + 1
        
        batch_df = generate_transactions_batch(
            num_transactions=batch_size,
            customers_df=customers_df,
            products_df=products_df,
            start_id=start_id
        )
        
        all_transactions.append(batch_df)
        
        # Save every 10 batches to manage memory
        if (batch_num + 1) % 10 == 0 or (batch_num + 1) == num_batches:
            combined = pd.concat(all_transactions, ignore_index=True)
            
            # Append to single CSV file
            combined.to_csv(
                transactions_csv,
                mode='a',  # Append mode
                header=first_batch,  # Header only for first batch
                index=False
            )
            
            first_batch = False
            all_transactions = []  # Clear memory
    
    # Validate a sample
    print("\n   Validating transaction sample...")
    sample_trans = pd.read_csv(transactions_csv, nrows=100000)
    validate_transactions(sample_trans, customers_df, products_df)
    print(f"   Saved: {transactions_csv.name}")


def generate_transactions_parquet_partitioned(
    num_batches: int,
    batch_size: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    output_dir: Path,
    dataset_name: str
):
    """
    Generate transactions as partitioned Parquet files (for FULL dataset)
    Partitioned by year and month for optimal query performance
    
    Args:
        num_batches: Number of batches to generate
        batch_size: Transactions per batch
        customers_df: Customer dataframe
        products_df: Product dataframe
        output_dir: Output directory
        dataset_name: Dataset name for logging
    """
    transactions_dir = output_dir / "transactions"
    transactions_dir.mkdir(exist_ok=True)
    
    all_transactions = []
    
    for batch_num in tqdm(range(num_batches), desc="Generating batches", unit="batch"):
        start_id = batch_num * batch_size + 1
        
        batch_df = generate_transactions_batch(
            num_transactions=batch_size,
            customers_df=customers_df,
            products_df=products_df,
            start_id=start_id
        )
        
        all_transactions.append(batch_df)
        
        # Save every 10 batches to manage memory
        if (batch_num + 1) % 10 == 0 or (batch_num + 1) == num_batches:
            combined = pd.concat(all_transactions, ignore_index=True)
            
            # Add year and month columns for partitioning
            combined['year'] = pd.to_datetime(combined['transaction_date']).dt.year
            combined['month'] = pd.to_datetime(combined['transaction_date']).dt.month
            
            # Partition and save by year/month
            for (year, month), group in combined.groupby(['year', 'month']):
                partition_dir = transactions_dir / f"year={year}" / f"month={month:02d}"
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Create unique filename with batch range
                batch_start = (batch_num // 10) * 10
                partition_file = partition_dir / f"part-{batch_start:04d}-{batch_num:04d}.parquet"
                
                # Remove year/month columns before saving (they're in the path)
                group_to_save = group.drop(columns=['year', 'month'])
                save_parquet(group_to_save, partition_file)
            
            # Clear memory
            all_transactions = []
    
    # Validate a sample
    print("\n   Validating transaction sample...")
    sample_files = list(transactions_dir.rglob("*.parquet"))[:5]
    if sample_files:
        sample_trans = pd.concat([pd.read_parquet(f) for f in sample_files], ignore_index=True)
        validate_transactions(sample_trans, customers_df, products_df)
    
    print(f"   Saved: {len(list(transactions_dir.rglob('*.parquet')))} partitioned files")


# ===========================
# MAIN
# ===========================

def main():
    """Main execution"""
    
    print("="*60)
    print("E-COMMERCE DATASET GENERATOR")
    print("="*60)
    print(f"\nStart time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Choose what to generate
    print("\nWhat would you like to generate?")
    print("  1. FULL dataset only (100M transactions, Parquet partitioned, ~10-15 min)")
    print("  2. SAMPLE dataset only (20M transactions, CSV flat, ~3-5 min)")
    print("  3. BOTH datasets (recommended, ~15-20 min)")
    
    choice = input("\nEnter choice (1/2/3) [default: 3]: ").strip() or "3"
    
    start_time = datetime.now()
    
    try:
        if choice in ["1", "3"]:
            generate_dataset(
                num_customers=FULL_CUSTOMERS,
                num_products=FULL_PRODUCTS,
                num_transactions=FULL_TRANSACTIONS,
                batch_size=BATCH_SIZE_FULL,
                output_dir=RAW_DIR,
                dataset_name="FULL",
                format_type="parquet_partitioned"
            )

        if choice in ["2", "3"]:
            generate_dataset(
                num_customers=SAMPLE_CUSTOMERS,
                num_products=SAMPLE_PRODUCTS,
                num_transactions=SAMPLE_TRANSACTIONS,
                batch_size=BATCH_SIZE_SAMPLE,
                output_dir=SAMPLE_DIR,
                dataset_name="SAMPLE",
                format_type="csv"
            )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print(f"Generation completed in {duration/60:.1f} minutes")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        print("\nDataset Summary:")
        if choice in ["2", "3"]:
            print(f"\nSAMPLE (CSV format):")
            print(f"  {SAMPLE_DIR}/customers.csv")
            print(f"  {SAMPLE_DIR}/products.csv")
            print(f"  {SAMPLE_DIR}/transactions.csv")
        
        if choice in ["1", "3"]:
            print(f"\nFULL (Parquet format):")
            print(f"  {RAW_DIR}/customers.parquet")
            print(f"  {RAW_DIR}/products.parquet")
            print(f"  {RAW_DIR}/transactions/  (partitioned by year/month)")
        
    except KeyboardInterrupt:
        print("\n\nGeneration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError during generation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()