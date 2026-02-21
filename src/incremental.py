from pyspark.sql import SparkSession
from transformation import validateAndClean, enrichTransactions
from pyspark.sql.functions import broadcast

CURATED_PATH = "../data/curated/enriched_transactions"
BATCH2_PATH = "../data/raw/sales_transactions_batch2.csv"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("IncrementalLoadStep") \
    .getOrCreate()

# Read existing curated data
curated_df = spark.read.parquet(CURATED_PATH)
print("Existing curated count:", curated_df.count())

# Read new batch
new_transactions = spark.read.csv(BATCH2_PATH, header=True, inferSchema=True)

# Clean new batch
cleaned_new = validateAndClean(new_transactions)

# Read dimension tables
products = spark.read.csv("../data/raw/products.csv", header=True, inferSchema=True)
stores = spark.read.csv("../data/raw/stores.csv", header=True, inferSchema=True)

# Enrich new batch
enriched_new = enrichTransactions(cleaned_new, products, stores)

# Keep ONLY records that are NOT already in curated
new_records = enriched_new.join(
    curated_df.select("transaction_id"),
    on="transaction_id",
    how="left_anti"
)

print("New records to insert:", new_records.count())

# Append new records to curated
new_records.write \
    .partitionBy("transaction_year", "transaction_month") \
    .mode("append") \
    .parquet(CURATED_PATH)

# Validate final count
final_df = spark.read.parquet(CURATED_PATH)
print("Final curated count:", final_df.count())

spark.stop()
