import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
from config import CONFIG
from logger import get_logger

logger = get_logger(__name__)
logger.info("Starting data generation step")

fake = Faker()

# ---------------- CONFIG ----------------
NUM_PRODUCTS = 300
NUM_STORES = 30
BATCH1_ROWS = 120000
BATCH2_ROWS = 80000

START_DATE = datetime(2024, 1, 1)

logger.info(f"Config → products:{NUM_PRODUCTS}, stores:{NUM_STORES}, "
            f"batch1:{BATCH1_ROWS}, batch2:{BATCH2_ROWS}")

# ---------------- PRODUCTS ----------------
logger.info("Generating product dataset")
products = []
for i in range(1, NUM_PRODUCTS + 1):
    products.append({
        "product_id": i,
        "product_name": fake.word().capitalize(),
        "category": random.choice(["Electronics", "Clothing", "Home", "Sports", "Beauty"]),
        "brand": fake.company(),
        "cost_price": round(random.uniform(5, 700), 2)
    })

products_df = pd.DataFrame(products)
products_df.to_csv(CONFIG["products"], index=False)
logger.info(f"Products written → {CONFIG['products']} ({len(products_df)} rows)")

# ---------------- STORES ----------------
logger.info("Generating store dataset")
stores = []
for i in range(1, NUM_STORES + 1):
    stores.append({
        "store_id": i,
        "store_name": fake.company(),
        "city": fake.city(),
        "region": random.choice(["North", "South", "East", "West"])
    })

stores_df = pd.DataFrame(stores)
stores_df.to_csv(CONFIG["stores"], index=False)
logger.info(f"Stores written → {CONFIG['stores']} ({len(stores_df)} rows)")

# ---------------- FUNCTION ----------------
def generate_transactions(num_rows, start_offset_days, file_path, start_id):
    logger.info(f"Generating {num_rows} transactions → {file_path}")

    transactions = []
    for i in range(start_id, start_id + num_rows):
        transactions.append({
            "transaction_id": i,
            "customer_id": random.randint(1000, 9000),
            "product_id": random.randint(1, NUM_PRODUCTS),
            "store_id": random.randint(1, NUM_STORES),
            "transaction_date": START_DATE + timedelta(days=start_offset_days + random.randint(0, 180)),
            "quantity": random.randint(1, 6),
            "unit_price": round(random.uniform(10, 900), 2),
            "payment_method": random.choice(["Card", "Cash", "UPI", "NetBanking"])
        })

    df = pd.DataFrame(transactions)
    df.to_csv(file_path, index=False)

    logger.info(f"Transactions written → {file_path} ({len(df)} rows)")

# ---------------- BATCH 1 ----------------
generate_transactions(
    num_rows=BATCH1_ROWS,
    start_offset_days=0,
    file_path=CONFIG["raw_transactions_batch_1"],
    start_id=1
)

# ---------------- BATCH 2 ----------------
generate_transactions(
    num_rows=BATCH2_ROWS,
    start_offset_days=181,
    file_path=CONFIG["raw_transactions_batch_2"],
    start_id=BATCH1_ROWS + 1
)

logger.info("Synthetic datasets generated successfully")
