from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()

products = spark.read.csv("../data/raw/products.csv", header= True, inferSchema=True)
stores = spark.read.csv("../data/raw/stores.csv", header=True, inferSchema=True)
sales_transactions = spark.read.csv("../data/raw/sales_transactions_batch1.csv", header=True, inferSchema=True)

products.printSchema()
stores.printSchema()
sales_transactions.printSchema()

row_counts = {
    "products" : products.count(),
    "stores" : stores.count(),
    "sales_transactions" : sales_transactions.count()
}

for x in row_counts:
    print(f"{x} -> {row_counts[x]} rows\n")