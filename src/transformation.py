from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, row_number, desc, broadcast, month, year
from config import CONFIG
from logger import get_logger

logger = get_logger(__name__)
logger.info("Starting transformation step")


class ArgumentNullException(Exception):
    pass


def getFinalColumns():
    transaction_columns = [
        "transaction_id",
        "customer_id",
        "product_id",
        "store_id",
        "transaction_date",
        "quantity",
        "unit_price"
    ]

    product_columns = [
        "product_name",
        "category",
        "brand",
        "cost_price"
    ]

    store_columns = [
        "store_name",
        "city",
        "region"
    ]

    metric_columns = [
        "total_amount",
        "profit",
        "transaction_year",
        "transaction_month"
    ]

    return transaction_columns + product_columns + store_columns + metric_columns


def validateAndClean(df: DataFrame) -> DataFrame:
    logger.info("Validating and cleaning transactions")

    initial_count = df.count()
    logger.info(f"Initial row count: {initial_count}")

    df = df\
        .withColumn("transaction_id", col("transaction_id").cast("int"))\
        .withColumn("customer_id", col("customer_id").cast("int"))\
        .withColumn("product_id", col("product_id").cast("int"))\
        .withColumn("store_id", col("store_id").cast("int"))\
        .withColumn("transaction_date", col("transaction_date").cast("date"))\
        .withColumn("quantity", col("quantity").cast("int"))\
        .withColumn("unit_price", col("unit_price").cast("double"))

    df = df.na.drop(subset=[
        "transaction_id", "customer_id", "product_id",
        "store_id", "quantity", "unit_price"
    ])

    df = df\
        .filter("quantity > 0 AND unit_price > 0")\
        .withColumn("total_amount", col("quantity") * col("unit_price"))

    window_spec = Window.partitionBy("transaction_id")\
        .orderBy(desc("transaction_date"))

    df = df\
        .withColumn("rn", row_number().over(window_spec))\
        .filter(col("rn") == 1)\
        .drop("rn")

    final_count = df.count()
    logger.info(f"Final row count after cleaning: {final_count}")
    logger.info(f"Rows removed: {initial_count - final_count}")

    return df


def enrichTransactions(transactions: DataFrame,
                       products: DataFrame,
                       stores: DataFrame) -> DataFrame:

    logger.info("Enriching transactions with product and store dimensions")

    enriched = transactions\
        .join(broadcast(products), "product_id", "left")\
        .join(broadcast(stores), "store_id", "left")\
        .withColumn(
            "profit",
            col("total_amount") - (col("quantity") * col("cost_price"))
        )\
        .withColumn("transaction_year", year("transaction_date"))\
        .withColumn("transaction_month", month("transaction_date"))

    enriched = enriched.dropDuplicates(["transaction_id"])

    logger.info("Enrichment complete")

    return enriched.select(getFinalColumns())


def writeProcessed(df: DataFrame):
    logger.info("Writing processed (silver) layer")
    df.write.mode("append").parquet(CONFIG["processed"])
    logger.info("Processed layer write complete")


def writeCurated(df: DataFrame):
    logger.info("Writing curated (gold) layer with partitioning")
    df.write.partitionBy("transaction_year", "transaction_month")\
        .mode("overwrite")\
        .parquet(CONFIG["curated"])
    logger.info("Curated layer write complete")


def runPipeline(url: str):
    logger.info("Pipeline execution started")

    if not url:
        logger.error("Input URL is empty")
        raise ArgumentNullException("URL must be not empty")

    spark = SparkSession.builder \
        .master(CONFIG["master"]) \
        .appName("TransformationPipeline") \
        .getOrCreate()

    logger.info("Reading raw transactions")
    transactions = spark.read.csv(url, header=True, inferSchema=True)

    logger.info("Reading dimension data")
    products = spark.read.csv(CONFIG["products"], header=True, inferSchema=True)
    stores = spark.read.csv(CONFIG["stores"], header=True, inferSchema=True)

    cleaned = validateAndClean(transactions)
    writeProcessed(cleaned)

    enriched = enrichTransactions(cleaned, products, stores)
    writeCurated(enriched)

    logger.info("Computing row counts")

    row_counts = {
        "products": products.count(),
        "stores": stores.count(),
        "sales_transactions": enriched.count()
    }

    for name, count in row_counts.items():
        logger.info(f"{name} -> {count} rows")

    logger.info("Pipeline execution completed successfully")


if __name__ == "__main__":
    runPipeline(CONFIG["raw_transactions_batch_1"])
