from pyspark.sql import SparkSession
from config import CONFIG
from logger import get_logger

logger = get_logger(__name__)

def run_ingestion():

    logger.info("Starting ingestion step")

    spark = SparkSession.builder \
        .master(CONFIG["master"]) \
        .appName("IngestionStep") \
        .getOrCreate()

    try:
        logger.info("Reading products data")
        products = spark.read.csv(
            CONFIG["products"],
            header=True,
            inferSchema=True
        )

        logger.info("Reading stores data")
        stores = spark.read.csv(
            CONFIG["stores"],
            header=True,
            inferSchema=True
        )

        logger.info("Reading transactions data")
        sales_transactions = spark.read.csv(
            CONFIG["raw_transactions"],
            header=True,
            inferSchema=True
        )

        logger.info("Printing schemas")
        products.printSchema()
        stores.printSchema()
        sales_transactions.printSchema()

        logger.info("Calculating row counts")

        row_counts = {
            "products": products.count(),
            "stores": stores.count(),
            "sales_transactions": sales_transactions.count()
        }

        for name, count in row_counts.items():
            logger.info(f"{name} -> {count} rows")

        logger.info("Ingestion completed successfully")

        return products, stores, sales_transactions

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise


if __name__ == "__main__":
    run_ingestion()
