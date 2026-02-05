"""
Data Generation Utilities for E-commerce Dataset
Generates realistic customers, products, and transactions (OPTIMIZED with vectorization)
"""
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Initialize Faker with Italian locale
fake = Faker('it_IT')

# Random seed for reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
fake.seed_instance(RANDOM_SEED)


# ===========================
# CONFIGURATION
# ===========================

CUSTOMER_SEGMENTS = {
    "VIP": 0.05,        # 5% - high spending, loyal
    "Regular": 0.35,    # 35% - medium spending, regular
    "Occasional": 0.60  # 60% - low spending, sporadic
}

PRODUCT_CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Accessories"],
    "Home & Kitchen": ["Appliances", "Furniture", "Decor", "Kitchenware"],
    "Fashion": ["Clothing", "Shoes", "Accessories", "Jewelry"],
    "Books": ["Fiction", "Non-Fiction", "Educational", "Comics"],
    "Sports": ["Equipment", "Clothing", "Supplements", "Accessories"],
    "Beauty": ["Skincare", "Makeup", "Haircare", "Fragrances"],
    "Toys": ["Educational", "Action Figures", "Board Games", "Outdoor"],
    "Food": ["Snacks", "Beverages", "Organic", "Gourmet"],
    "Health": ["Supplements", "Medical", "Fitness", "Wellness"],
    "Automotive": ["Parts", "Accessories", "Tools", "Care Products"]
}

BRANDS = {
    "Electronics": ["Apple", "Samsung", "Sony", "LG", "HP", "Dell", "Asus"],
    "Home & Kitchen": ["IKEA", "Philips", "Dyson", "KitchenAid", "Bosch"],
    "Fashion": ["Nike", "Adidas", "Zara", "H&M", "Gucci", "Prada"],
    "Books": ["Penguin", "Mondadori", "Feltrinelli", "Einaudi"],
    "Sports": ["Nike", "Adidas", "Puma", "Under Armour", "Decathlon"],
    "Beauty": ["L'Oréal", "Estée Lauder", "Clinique", "MAC", "Sephora"],
    "Toys": ["LEGO", "Mattel", "Hasbro", "Fisher-Price", "Playmobil"],
    "Food": ["Barilla", "Ferrero", "Lavazza", "De Cecco", "Mulino Bianco"],
    "Health": ["GNC", "Nature's Bounty", "Optimum", "MyProtein"],
    "Automotive": ["Bosch", "Castrol", "Michelin", "3M", "Turtle Wax"]
}

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash_on_delivery"]
TRANSACTION_STATUS = ["completed", "cancelled", "refunded"]

ITALIAN_REGIONS = [
    "Lombardia", "Lazio", "Campania", "Sicilia", "Veneto",
    "Emilia-Romagna", "Piemonte", "Puglia", "Toscana", "Calabria",
    "Sardegna", "Liguria", "Marche", "Abruzzo", "Friuli-Venezia Giulia",
    "Trentino-Alto Adige", "Umbria", "Basilicata", "Molise", "Valle d'Aosta"
]


# ===========================
# CUSTOMERS GENERATION
# ===========================

def generate_customers(num_customers: int, start_id: int = 1) -> pd.DataFrame:
    """
    Generate customer dataset
    
    Args:
        num_customers (int): Number of customers to generate
        start_id (int): Starting customer ID
        
    Returns:
        pd.DataFrame: Customer dataframe
    """
    print(f"Generating {num_customers:,} customers...")
    
    # Pre-determine segment distribution
    num_vip = int(num_customers * CUSTOMER_SEGMENTS["VIP"])
    num_regular = int(num_customers * CUSTOMER_SEGMENTS["Regular"])
    num_occasional = num_customers - num_vip - num_regular
    
    segments = np.array(
        ["VIP"] * num_vip +
        ["Regular"] * num_regular +
        ["Occasional"] * num_occasional
    )
    np.random.shuffle(segments)
    
    # Generate all IDs at once
    customer_ids = [f"C{start_id + i:08d}" for i in range(num_customers)]
    
    # Generate genders
    genders = np.random.choice(["M", "F"], size=num_customers)
    
    # Generate names using list comprehension (faster than loop)
    names = [
        fake.name_male() if gender == "M" else fake.name_female()
        for gender in genders
    ]
    
    # Generate emails with unique identifiers
    clean_names = [name.lower().replace(" ", ".").replace("'", "") for name in names]
    emails = [
        f"{clean_name}.{i+1}@{fake.free_email_domain()}"
        for i, clean_name in enumerate(clean_names)
    ]
    
    # Generate other fields in batch
    phones = [fake.phone_number() for _ in range(num_customers)]
    addresses = [fake.street_address() for _ in range(num_customers)]
    cities = [fake.city() for _ in range(num_customers)]
    regions = np.random.choice(ITALIAN_REGIONS, size=num_customers)
    postal_codes = [fake.postcode() for _ in range(num_customers)]
    
    # Generate registration dates
    start_date = datetime(2024, 1, 1).timestamp()
    end_date = datetime(2024, 12, 31).timestamp()
    random_timestamps = np.random.uniform(start_date, end_date, size=num_customers)
    registration_dates = [datetime.fromtimestamp(ts).date() for ts in random_timestamps]
    
    # Generate ages
    ages = np.random.randint(18, 76, size=num_customers)
    
    # Create DataFrame all at once
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "name": names,
        "email": emails,
        "phone": phones,
        "address": addresses,
        "city": cities,
        "region": regions,
        "country": "Italy",
        "postal_code": postal_codes,
        "registration_date": registration_dates,
        "customer_segment": segments,
        "age": ages,
        "gender": genders
    })
    
    print(f"Generated {len(df):,} customers")
    return df


# ===========================
# PRODUCTS GENERATION
# ===========================

def generate_products(num_products: int, start_id: int = 1) -> pd.DataFrame:
    """
    Generate product dataset
    
    Args:
        num_products (int): Number of products to generate
        start_id (int): Starting product ID
        
    Returns:
        pd.DataFrame: Product dataframe
    """
    print(f"Generating {num_products:,} products...")
    
    # Generate all IDs at once
    product_ids = [f"P{start_id + i:08d}" for i in range(num_products)]
    
    # Select categories (uniform distribution)
    categories = np.random.choice(list(PRODUCT_CATEGORIES.keys()), size=num_products)
    
    # Select subcategories and brands based on category
    subcategories = []
    brands = []
    for cat in categories:
        subcategories.append(np.random.choice(PRODUCT_CATEGORIES[cat]))
        brands.append(np.random.choice(BRANDS[cat]))
    subcategories = np.array(subcategories)
    brands = np.array(brands)
    
    # Generate prices based on category
    base_prices = np.zeros(num_products)
    
    # Price ranges by category (vectorized)
    for i, cat in enumerate(categories):
        if cat == "Electronics":
            base_prices[i] = np.random.uniform(50, 2000)
        elif cat in ["Fashion", "Sports"]:
            base_prices[i] = np.random.uniform(20, 500)
        elif cat == "Books":
            base_prices[i] = np.random.uniform(10, 50)
        elif cat == "Food":
            base_prices[i] = np.random.uniform(5, 100)
        else:
            base_prices[i] = np.random.uniform(15, 300)
    
    prices = np.round(base_prices, 2)
    
    # Calculate costs (60-75% of price)
    cost_ratios = np.random.uniform(0.60, 0.75, size=num_products)
    costs = np.round(prices * cost_ratios, 2)
    
    # Generate product names using list comprehension
    product_names = [
        f"{brands[i]} {subcategories[i]} {fake.word().title()}"
        for i in range(num_products)
    ]
    
    # Other fields
    stock_quantities = np.random.randint(0, 1001, size=num_products)
    weights = np.round(np.random.uniform(0.1, 10.0, size=num_products), 2)
    ratings = np.round(np.random.uniform(3.0, 5.0, size=num_products), 1)
    num_reviews = np.random.randint(0, 1001, size=num_products)
    
    # Create DataFrame all at once
    df = pd.DataFrame({
        "product_id": product_ids,
        "product_name": product_names,
        "category": categories,
        "subcategory": subcategories,
        "brand": brands,
        "price": prices,
        "cost": costs,
        "stock_quantity": stock_quantities,
        "weight_kg": weights,
        "rating": ratings,
        "num_reviews": num_reviews
    })
    
    print(f"Generated {len(df):,} products")
    return df


# ===========================
# TRANSACTIONS GENERATION
# ===========================

def generate_transactions_batch(
    num_transactions: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    start_id: int = 1,
    start_date: datetime = datetime(2024, 1, 1),
    end_date: datetime = datetime(2025, 12, 31)
) -> pd.DataFrame:
    """
    Generate transaction dataset batch
    
    Args:
        num_transactions (int): Number of transactions
        customers_df (pd.DataFrame): Customer dataframe
        products_df (pd.DataFrame): Product dataframe
        start_id (int): Starting transaction ID
        start_date (datetime): Start date for transactions
        end_date (datetime): End date for transactions
        
    Returns:
        pd.DataFrame: Transaction dataframe
    """
    # Pre-compute customer weights (VIP=6x, Regular=3x, Occasional=1x)
    customer_weights = np.where(
        customers_df["customer_segment"] == "VIP", 6,
        np.where(customers_df["customer_segment"] == "Regular", 3, 1)
    )
    customer_weights = customer_weights / customer_weights.sum()
    
    # Pre-compute product weights (Zipf distribution)
    product_ranks = np.arange(1, len(products_df) + 1)
    product_weights = 1.0 / product_ranks
    product_weights = product_weights / product_weights.sum()
    
    # Select all customers and products at once
    customer_indices = np.random.choice(
        len(customers_df), 
        size=num_transactions, 
        p=customer_weights
    )
    product_indices = np.random.choice(
        len(products_df), 
        size=num_transactions, 
        p=product_weights
    )
    
    # Get customer and product data
    customers = customers_df.iloc[customer_indices].reset_index(drop=True)
    products = products_df.iloc[product_indices].reset_index(drop=True)
    
    # Generate random dates
    time_range_seconds = int((end_date - start_date).total_seconds())
    random_seconds = np.random.randint(0, time_range_seconds, size=num_transactions)
    transaction_timestamps = pd.to_datetime(
        [start_date + timedelta(seconds=int(s)) for s in random_seconds]
    )
    
    # Quantity based on segment
    quantities = np.where(
        customers["customer_segment"] == "VIP", 
        np.random.randint(1, 6, size=num_transactions),
        np.where(
            customers["customer_segment"] == "Regular",
            np.random.randint(1, 4, size=num_transactions),
            np.random.randint(1, 3, size=num_transactions)
        )
    )
    
    # Pricing
    unit_prices = products["price"].values
    total_amounts = quantities * unit_prices
    
    # Discounts based on segment
    discount_pcts = np.where(
        customers["customer_segment"] == "VIP",
        np.random.uniform(0.10, 0.30, size=num_transactions),
        np.where(
            customers["customer_segment"] == "Regular",
            np.random.uniform(0.05, 0.15, size=num_transactions),
            np.random.uniform(0.00, 0.10, size=num_transactions)
        )
    )
    
    discount_amounts = np.round(total_amounts * discount_pcts, 2)
    final_amounts = np.round(total_amounts - discount_amounts, 2)
    
    # Shipping cost
    shipping_costs = np.where(
        final_amounts > 50,
        0.0,
        np.round(products["weight_kg"].values * 2.5 + 3.99, 2)
    )
    
    # Payment methods
    payment_methods = np.random.choice(
        PAYMENT_METHODS, 
        size=num_transactions
    )
    
    # Status (95% completed, 3% cancelled, 2% refunded)
    status_rand = np.random.random(size=num_transactions)
    statuses = np.where(
        status_rand < 0.95, "completed",
        np.where(status_rand < 0.98, "cancelled", "refunded")
    )
    
    # Adjust amounts for cancelled/refunded
    final_amounts = np.where(
        (statuses == "cancelled") | (statuses == "refunded"),
        0.0,
        final_amounts
    )
    shipping_costs = np.where(
        statuses == "cancelled",
        0.0,
        shipping_costs
    )
    
    # Generate transaction IDs
    transaction_ids = [f"T{start_id + i:010d}" for i in range(num_transactions)]
    
    # Create dataframe all at once
    df = pd.DataFrame({
        "transaction_id": transaction_ids,
        "customer_id": customers["customer_id"].values,
        "product_id": products["product_id"].values,
        "quantity": quantities,
        "unit_price": np.round(unit_prices, 2),
        "total_amount": np.round(total_amounts, 2),
        "discount_pct": np.round(discount_pcts, 2),
        "discount_amount": discount_amounts,
        "final_amount": final_amounts,
        "shipping_cost": shipping_costs,
        "transaction_date": transaction_timestamps.date,
        "transaction_timestamp": transaction_timestamps,
        "payment_method": payment_methods,
        "status": statuses
    })
    
    return df


# ===========================
# VALIDATION
# ===========================

def validate_customers(df: pd.DataFrame) -> bool:
    """Validate customer dataframe"""
    print("Validating customers...")
    
    assert df["customer_id"].is_unique, "Customer IDs not unique"
    assert df["email"].is_unique, "Emails not unique"
    assert df["customer_segment"].isin(["VIP", "Regular", "Occasional"]).all(), "Invalid segments"
    assert (df["age"] >= 18).all() and (df["age"] <= 75).all(), "Invalid ages"
    
    print("Customer validation passed")
    return True


def validate_products(df: pd.DataFrame) -> bool:
    """Validate product dataframe"""
    print("Validating products...")
    
    assert df["product_id"].is_unique, "Product IDs not unique"
    assert (df["price"] > 0).all(), "Invalid prices"
    assert (df["cost"] > 0).all(), "Invalid costs"
    assert (df["cost"] < df["price"]).all(), "Cost >= Price"
    
    print("Product validation passed")
    return True


def validate_transactions(df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> bool:
    """Validate transaction dataframe"""
    print("Validating transactions...")
    
    assert df["transaction_id"].is_unique, "Transaction IDs not unique"
    assert df["customer_id"].isin(customers_df["customer_id"]).all(), "Invalid customer_id (FK violation)"
    assert df["product_id"].isin(products_df["product_id"]).all(), "Invalid product_id (FK violation)"
    assert (df["quantity"] > 0).all(), "Invalid quantity"
    assert (df["final_amount"] >= 0).all(), "Negative amounts"
    
    print("Transaction validation passed")
    return True


if __name__ == "__main__":
    # Quick test
    print("Testing data generation...")
    print("="*60)
    
    import time
    
    # Test customers
    start = time.time()
    customers = generate_customers(10000)
    print(f"Time: {time.time() - start:.2f}s\n")
    
    # Test products
    start = time.time()
    products = generate_products(1000)
    print(f"Time: {time.time() - start:.2f}s\n")
    
    # Test transactions
    start = time.time()
    transactions = generate_transactions_batch(100000, customers, products)
    print(f"Time: {time.time() - start:.2f}s\n")
    
    # Validations
    validate_customers(customers)
    validate_products(products)
    validate_transactions(transactions, customers, products)
    
    print("\n" + "="*60)
    print("All tests passed!")
    print(f"  - Customers: {len(customers):,}")
    print(f"  - Products: {len(products):,}")
    print(f"  - Transactions: {len(transactions):,}")
    print("\nSample transaction:")
    print(transactions.head(1).T)