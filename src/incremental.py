from pyspark.sql import SparkSession
from transformation import validateAndClean, enrichTransactions
from config import CONFIG
from logger import get_logger

logger = get_logger(__name__)
logger.info("🚀 Starting incremental load step")

spark = SparkSession.builder \
    .master(CONFIG["master"]) \
    .appName("IncrementalLoadStep") \
    .getOrCreate()

try:
    # ========================
    # Read existing curated
    # ========================
    logger.info("Reading existing curated dataset")
    curated_df = spark.read.parquet(CONFIG["curated"])
    existing_count = curated_df.count()
    logger.info(f"Existing curated count: {existing_count}")

    # ========================
    # Read new batch
    # ========================
    logger.info("Reading new transactions batch")
    new_transactions = spark.read.csv(
        CONFIG["raw_transactions_batch_2"],
        header=True,
        inferSchema=True
    )

    # ========================
    # Clean new batch
    # ========================
    logger.info("Cleaning new batch")
    cleaned_new = validateAndClean(new_transactions)
    logger.info(f"Cleaned batch count: {cleaned_new.count()}")

    # ========================
    # Read dimension tables
    # ========================
    logger.info("Reading dimension tables")
    products = spark.read.csv(CONFIG["products"], header=True, inferSchema=True)
    stores = spark.read.csv(CONFIG["stores"], header=True, inferSchema=True)

    # ========================
    # Enrich new batch
    # ========================
    logger.info("Enriching new batch")
    enriched_new = enrichTransactions(cleaned_new, products, stores)
    logger.info(f"Enriched batch count: {enriched_new.count()}")

    # ========================
    # Identify new records
    # ========================
    logger.info("Performing LEFT ANTI join to detect new records")
    new_records = enriched_new.join(
        curated_df.select("transaction_id"),
        on="transaction_id",
        how="left_anti"
    )

    new_records_count = new_records.count()
    logger.info(f"New records to insert: {new_records_count}")

    # ========================
    # Write incremental data
    # ========================
    logger.info("Appending new records to curated layer")
    new_records.write \
        .partitionBy("transaction_year", "transaction_month") \
        .mode("append") \
        .parquet(CONFIG["curated"])

    logger.info("Write completed")

    # ========================
    # Final validation
    # ========================
    logger.info("Validating final curated dataset")
    final_df = spark.read.parquet(CONFIG["curated"])
    final_count = final_df.count()

    logger.info(f"Final curated count: {final_count}")
    logger.info(f"Net new rows added: {final_count - existing_count}")

    logger.info("✅ Incremental load completed successfully")

except Exception as e:
    logger.exception("❌ Incremental load failed")
    raise

finally:
    spark.stop()