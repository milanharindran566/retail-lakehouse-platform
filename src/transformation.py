from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, row_number, desc, broadcast, month, year

BATCH1_URL = "../data/raw/sales_transactions_batch1.csv"

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

    return df

def enrichTransactions(
    transactions: DataFrame,
    products: DataFrame,
    stores: DataFrame
) -> DataFrame:

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

    return enriched.select(getFinalColumns())

def writeProcessed(df: DataFrame):
    df.write.mode("append")\
        .parquet("../data/processed/sales_transactions_cleaned")


def writeCurated(df: DataFrame):
    df.write.partitionBy("transaction_year", "transaction_month")\
        .mode("overwrite")\
        .parquet("../data/curated/enriched_transactions")
        
def runPipeline(url: str):
    if (not url):
        raise ArgumentNullException("URL must be not empty")

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TransformationPipeline") \
        .getOrCreate()

    transactions = spark.read.csv(
        url,
        header=True,
        inferSchema=True
    )

    products = spark.read.csv("../data/raw/products.csv", header=True, inferSchema=True)
    stores = spark.read.csv("../data/raw/stores.csv", header=True, inferSchema=True)

    cleaned = validateAndClean(transactions)
    writeProcessed(cleaned)

    enriched = enrichTransactions(cleaned, products, stores)
    writeCurated(enriched)
    
    row_counts = {
    "products" : products.count(),
    "stores" : stores.count(),
    "sales_transactions" : enriched.count()
    }
    
    for x in row_counts:
        print(f"{x} -> {row_counts[x]} rows\n")

if __name__ == "__main__":
    runPipeline(BATCH1_URL)
