import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# ---------------- CONFIG ----------------
NUM_PRODUCTS = 300
NUM_STORES = 30
BATCH1_ROWS = 120000
BATCH2_ROWS = 80000

START_DATE = datetime(2024, 1, 1)

# ---------------- PRODUCTS ----------------
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
products_df.to_csv("data/raw/products.csv", index=False)

# ---------------- STORES ----------------
stores = []
for i in range(1, NUM_STORES + 1):
    stores.append({
        "store_id": i,
        "store_name": fake.company(),
        "city": fake.city(),
        "region": random.choice(["North", "South", "East", "West"])
    })

stores_df = pd.DataFrame(stores)
stores_df.to_csv("data/raw/stores.csv", index=False)

# ---------------- FUNCTION TO GENERATE TRANSACTIONS ----------------
def generate_transactions(num_rows, start_offset_days, file_name, start_id):
    transactions = []

    for i in range(start_id, start_id + num_rows):
        transactions.append({
            "transaction_id": i,
            "customer_id": random.randint(1000, 9999),
            "product_id": random.randint(1, NUM_PRODUCTS),
            "store_id": random.randint(1, NUM_STORES),
            "transaction_date": START_DATE + timedelta(days=start_offset_days + random.randint(0, 180)),
            "quantity": random.randint(1, 6),
            "unit_price": round(random.uniform(10, 900), 2),
            "payment_method": random.choice(["Card", "Cash", "UPI", "NetBanking"])
        })

    df = pd.DataFrame(transactions)
    df.to_csv(f"data/raw/{file_name}", index=False)


# ---------------- BATCH 1 (Jan → Jun) ----------------
generate_transactions(
    num_rows=BATCH1_ROWS,
    start_offset_days=0,
    file_name="sales_transactions_batch1.csv",
    start_id=1
)

# ---------------- BATCH 2 (Jul → Dec) ----------------
generate_transactions(
    num_rows=BATCH2_ROWS,
    start_offset_days=181,
    file_name="sales_transactions_batch2.csv",
    start_id=BATCH1_ROWS + 1
)

print("Incremental synthetic datasets generated successfully!")
