"""
Generate E-commerce Dataset
Creates both FULL (100M transactions) and SAMPLE (20M transactions) datasets
All files saved as CSV (flat, no partitioning) for maximum compatibility
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
BATCH_SIZE_SAMPLE = 500_000   # 500K transactions per batch
NUM_BATCHES_SAMPLE = SAMPLE_TRANSACTIONS // BATCH_SIZE_SAMPLE    # 40 batches

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

def save_csv(df: pd.DataFrame, filepath: Path):
    """Save dataframe as CSV"""
    df.to_csv(filepath, index=False)

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
    
    # Save as CSV
    customers_csv = output_dir / "customers.csv"
    save_csv(customers_df, customers_csv)
    save_schema(customers_df, SCHEMAS_DIR / f"{dataset_name.lower()}_customers_schema.json")
    
    # Step 2: Generate Products
    print(f"\n[2/3] Generating {num_products:,} products...")
    products_df = generate_products(num_products)
    validate_products(products_df)
    
    # Save as CSV
    products_csv = output_dir / "products.csv"
    save_csv(products_df, products_csv)
    save_schema(products_df, SCHEMAS_DIR / f"{dataset_name.lower()}_products_schema.json")
    
    # Step 3: Generate Transactions as single CSV file (no partitioning)
    print(f"\n[3/3] Generating {num_transactions:,} transactions ({num_batches:,} batches)...")
    
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
            
            # Append to single CSV file (flat, no partitioning)
            combined.to_csv(
                transactions_csv,
                mode='a',  # Append mode
                header=first_batch,  # Header only for first batch
                index=False
            )
            
            first_batch = False
            all_transactions = []  # Clear memory
    
    # Validate a sample
    print("\nValidating transaction sample...")
    sample_trans = pd.read_csv(transactions_csv, nrows=100000)
    validate_transactions(sample_trans, customers_df, products_df)
    
    # Save schema
    save_schema(sample_trans, SCHEMAS_DIR / f"{dataset_name.lower()}_transactions_schema.json")
    
    print(f"\n{dataset_name} dataset generation complete!")
    print(f"  - Customers: {num_customers:,} records")
    print(f"  - Products: {num_products:,} records")
    print(f"  - Transactions: {num_transactions:,} records")
    print(f"  - Location: {output_dir}")

# ===========================
# MAIN
# ===========================

def main():
    """Main execution"""
    
    print("="*50)
    print("E-COMMERCE DATASET GENERATOR")
    print("="*50)
    print(f"\nStart time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Choose what to generate
    print("\nWhat would you like to generate?")
    print("  1. FULL dataset only (100M transactions, ~15-20 min)")
    print("  2. SAMPLE dataset only (20M transactions, ~3-5 min)")
    print("  3. BOTH datasets (recommended, ~20-25 min)")
    
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
                dataset_name="FULL"
            )

        if choice in ["2", "3"]:
            generate_dataset(
                num_customers=SAMPLE_CUSTOMERS,
                num_products=SAMPLE_PRODUCTS,
                num_transactions=SAMPLE_TRANSACTIONS,
                batch_size=BATCH_SIZE_SAMPLE,
                output_dir=SAMPLE_DIR,
                dataset_name="SAMPLE"
            )
        
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