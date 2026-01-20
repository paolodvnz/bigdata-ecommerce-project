"""
Generate E-commerce Dataset
Creates both FULL (100M transactions) and SAMPLE (100K transactions) datasets
"""
import os
import sys
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import argparse

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

# SAMPLE Dataset Parameters 
SAMPLE_CUSTOMERS = 1_000
SAMPLE_PRODUCTS = 1_000
SAMPLE_TRANSACTIONS = 1_000_000
BATCH_SIZE_SAMPLE = 10_000   # 10K transactions per batch
NUM_BATCHES_SAMPLE = SAMPLE_TRANSACTIONS // BATCH_SIZE_SAMPLE    # 100 batches

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
    """Save dataframe as Parquet with compression"""
    df.to_parquet(filepath, engine='pyarrow', compression=compression, index=False)

def save_schema(df: pd.DataFrame, filepath: Path):
    """Save dataframe schema as JSON"""
    schema = {
        "columns": df.dtypes.astype(str).to_dict(),
        "num_rows": len(df),
        "num_columns": len(df.columns)
    }
    
    import json
    with open(filepath, 'w') as f:
        json.dump(schema, f, indent=2)


# ===========================
# GENERATE DATASET 
# ===========================

def generate_dataset(
    num_customers: int,
    num_products: int,
    num_transactions: int,
    batch_size: int,
    output_dir: Path,
    dataset_name: str = "dataset"
):
    """
    Generate dataset with specified parameters (unified for FULL and SAMPLE)
    
    Args:
        num_customers: Number of customers
        num_products: Number of products
        num_transactions: Total number of transactions
        batch_size: Transactions per batch
        output_dir: Output directory (RAW_DIR or SAMPLE_DIR)
        dataset_name: Name for logging (FULL or SAMPLE)
    """
    
    print("\n" + "="*60)
    print(f"GENERATING {dataset_name} DATASET")
    print("="*60)
    print(f"  Customers: {num_customers:,}")
    print(f"  Products: {num_products:,}")
    print(f"  Transactions: {num_transactions:,}")
    print(f"  Batch size: {batch_size:,}")
    print(f"  Output: {output_dir}")
    
    num_batches = num_transactions // batch_size
    
    # Step 1: Generate Customers
    print(f"\n[1/3] Generating {num_customers:,} customers...")
    customers_df = generate_customers(num_customers)
    validate_customers(customers_df)
    
    customers_path = output_dir / "customers.parquet"
    save_parquet(customers_df, customers_path)
    save_schema(customers_df, SCHEMAS_DIR / f"{dataset_name.lower()}_customers_schema.json")
    
    # Step 2: Generate Products
    print(f"\n[2/3] Generating {num_products:,} products...")
    products_df = generate_products(num_products)
    validate_products(products_df)
    
    products_path = output_dir / "products.parquet"
    save_parquet(products_df, products_path)
    save_schema(products_df, SCHEMAS_DIR / f"{dataset_name.lower()}_products_schema.json")
    
    # Step 3: Generate Transactions (in batches with partitioning)
    print(f"\n[3/3] Generating {num_transactions:,} transactions ({num_batches:,} batches)...")
    
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
            
            # Save partitioned by year and month
            for (year, month), group in combined.groupby([
                pd.to_datetime(combined['transaction_date']).dt.year,
                pd.to_datetime(combined['transaction_date']).dt.month
            ]):
                partition_dir = transactions_dir / f"year={year}" / f"month={month:02d}"
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Use batch range in filename for uniqueness
                batch_start = (batch_num // 10) * 10
                partition_file = partition_dir / f"part-{batch_start:04d}-{batch_num:04d}.parquet"
                save_parquet(group, partition_file)
            
            # Clear memory
            all_transactions = []
    
    # Validate a sample
    print("\nValidating transaction sample...")
    # Read back a sample for validation
    sample_files = list(transactions_dir.rglob("*.parquet"))[:5]
    if sample_files:
        sample_trans = pd.concat([pd.read_parquet(f) for f in sample_files], ignore_index=True)
        validate_transactions(sample_trans, customers_df, products_df)
    
    # Save schema
    if sample_files:
        save_schema(sample_trans, SCHEMAS_DIR / f"{dataset_name.lower()}_transactions_schema.json")
    
    print(f"\n{dataset_name} dataset generation complete!")
    print(f"  - Customers: {num_customers:,} records")
    print(f"  - Products: {num_products:,} records")
    print(f"  - Transactions: {num_transactions:,} records")
    print(f"  - Location: {output_dir}")


# ===========================
# STATISTICS
# ===========================

def print_statistics():
    """Print dataset statistics"""
    
    print("\n" + "="*60)
    print("DATASET STATISTICS")
    print("="*60)
    
    for dataset_name, data_dir in [("FULL", RAW_DIR), ("SAMPLE", SAMPLE_DIR)]:
        print(f"\n{dataset_name} DATASET:")
        
        if not data_dir.exists():
            print(f"Not generated")
            continue
            
        if (data_dir / "customers.parquet").exists():
            customers = pd.read_parquet(data_dir / "customers.parquet")
            print(f"Customers: {len(customers):,}")
            print(f"  - VIP: {(customers['customer_segment']=='VIP').sum():,} ({(customers['customer_segment']=='VIP').sum()/len(customers)*100:.1f}%)")
            print(f"  - Regular: {(customers['customer_segment']=='Regular').sum():,} ({(customers['customer_segment']=='Regular').sum()/len(customers)*100:.1f}%)")
            print(f"  - Occasional: {(customers['customer_segment']=='Occasional').sum():,} ({(customers['customer_segment']=='Occasional').sum()/len(customers)*100:.1f}%)")
        
        if (data_dir / "products.parquet").exists():
            products = pd.read_parquet(data_dir / "products.parquet")
            print(f"\nProducts: {len(products):,}")
            print(f"  - Categories: {products['category'].nunique()}")
            print(f"  - Top 3 categories:")
            for cat, count in products['category'].value_counts().head(3).items():
                print(f"      - {cat}: {count:,}")
        
        # Count transaction files
        trans_dir = data_dir / "transactions"
        if trans_dir.exists():
            trans_files = list(trans_dir.rglob("*.parquet"))
            print(f"\nTransactions partition files: {len(trans_files):,}")
            
            # Sample a few files to estimate total
            if trans_files:
                sample_df = pd.read_parquet(trans_files[0])
                print(f"  - Sample file size: {len(sample_df):,} records")


# ===========================
# PARSE ARGUMENTS
# ===========================

def parse_arguments():
    """
    Parse command line arguments
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Generate BigData E-commerce dataset'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['sample', 'full', 'both'],
        default=None,  # None = interactive menu
        help='Dataset to generate: sample, full, or both'
    )
    return parser.parse_args()

# ===========================
# MAIN
# ===========================

def main():
    """Main execution"""
    
    print("="*50)
    print("E-COMMERCE DATASET GENERATOR")
    print("="*50)
    print(f"\nStart time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Parse arguments CLI
    args = parse_arguments()
    
    # Mode: CLI or interactive
    if args.mode is not None:
        # CLI Mode
        mode = args.mode
        print(f"\nModalità CLI: {mode.upper()}")
    else:
        # Interactive menu
        print("\nWhat would you like to generate?")
        print("  1. FULL dataset only (100M transactions, ~10-15 min)")
        print("  2. SAMPLE dataset only (100K transactions, ~1 min)")
        print("  3. BOTH datasets (recommended)")
        
        choice = input("\nEnter choice (1/2/3) [default: 3]: ").strip() or "3"
        mode_map = {'1': 'full', '2': 'sample', '3': 'both'}
        mode = mode_map.get(choice, 'both')
    
    start_time = datetime.now()
    
    try:
        # Generate datasets based on mode
        if mode in ["full", "both"]:
            generate_dataset(
                num_customers=FULL_CUSTOMERS,
                num_products=FULL_PRODUCTS,
                num_transactions=FULL_TRANSACTIONS,
                batch_size=BATCH_SIZE_FULL,
                output_dir=RAW_DIR,
                dataset_name="FULL"
            )
        
        if mode in ["sample", "both"]:
            generate_dataset(
                num_customers=SAMPLE_CUSTOMERS,
                num_products=SAMPLE_PRODUCTS,
                num_transactions=SAMPLE_TRANSACTIONS,
                batch_size=BATCH_SIZE_SAMPLE,
                output_dir=SAMPLE_DIR,
                dataset_name="SAMPLE"
            )
        
        print_statistics()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print(f"Generation completed in {duration/60:.1f} minutes")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
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