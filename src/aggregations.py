from pyspark.sql import SparkSession
from pyspark.sql.functions import col, day, quarter, dayofweek, date_format
from config import CONFIG
from logger import get_logger

logger = get_logger(__name__)
logger.info("Starting aggregation (warehouse modeling) step")

spark = SparkSession.builder.getOrCreate()

# ========================
# Read curated data
# ========================
logger.info("Reading curated dataset")
data = spark.read.parquet(CONFIG["curated"])
logger.info(f"Curated row count: {data.count()}")

# DIM PRODUCT
logger.info("Building dim_product")
dim_product = data \
    .select("product_id", "product_name", "category", "brand", "cost_price") \
    .dropDuplicates(["product_id"])

logger.info(f"dim_product count: {dim_product.count()}")

# DIM STORE
logger.info("Building dim_store")
dim_store = data \
    .select("store_id", "store_name", "city", "region") \
    .dropDuplicates(["store_id"])

logger.info(f"dim_store count: {dim_store.count()}")

# DIM DATE
logger.info("Building dim_date")
dim_date = data \
    .withColumn("date_id", date_format(col("transaction_date"), "yyyyMMdd").cast("int")) \
    .withColumn("full_date", col("transaction_date")) \
    .withColumn("day", day(col("transaction_date"))) \
    .withColumn("month", col("transaction_month")) \
    .withColumn("year", col("transaction_year")) \
    .withColumn("quarter", quarter(col("transaction_date"))) \
    .withColumn("day_of_week", dayofweek(col("transaction_date"))) \
    .select("date_id", "full_date", "day", "month", "year", "quarter", "day_of_week") \
    .dropDuplicates(["date_id"])

logger.info(f"dim_date count: {dim_date.count()}")

# FACT SALES
logger.info("Building fact_sales")
fact_sales = data \
    .withColumn("date_id", date_format(col("transaction_date"), "yyyyMMdd").cast("int")) \
    .select("transaction_id", "date_id", "product_id", "store_id", "quantity", "total_amount", "profit")

logger.info(f"fact_sales count: {fact_sales.count()}")

# Preview
logger.info("Previewing fact_sales")
fact_sales.show(2)

logger.info("Aggregation step completed successfully")
