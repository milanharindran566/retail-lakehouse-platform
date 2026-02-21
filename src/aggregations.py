from pyspark.sql import SparkSession
from pyspark.sql.functions import col, day, quarter, dayofweek, date_format

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet("../data/curated/enriched_transactions")

#dim_product
dim_product = data\
    .select("product_id", "product_name", "category", "brand","cost_price")\
    .dropDuplicates(subset=["product_id"])

#dim_store
dim_store = data\
    .select("store_id", "store_name", "city", "region")\
    .dropDuplicates(subset=["store_id"])

#dim_date
dim_date = data\
    .withColumn("date_id", date_format(col("transaction_date"), "yyyyMMdd").cast("int"))\
    .withColumn("full_date", col("transaction_date"))\
    .withColumn("day", day(col("transaction_date")))\
    .withColumn("month", col("transaction_month"))\
    .withColumn("year", col("transaction_year"))\
    .withColumn("quarter", quarter(col("transaction_date")))\
    .withColumn("day_of_week", dayofweek(col("transaction_date")))\
    .select("date_id", "full_date","day","month","year","quarter","day_of_week")
    
#fact_sales
fact_sales = data\
    .withColumn("date_id", date_format(col("transaction_date"), "yyyyMMdd").cast("int"))\
    .select("transaction_id","date_id","product_id","store_id","quantity","total_amount","profit")

fact_sales.show(2)