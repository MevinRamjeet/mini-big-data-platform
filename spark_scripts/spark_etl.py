from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_timestamp, when, month, year, sum as _sum
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import IntegerType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Olist_End_to_End_ETL") \
    .getOrCreate()

# 2. Load Raw Data from HDFS
base_path = "hdfs://namenode:9000/raw/olist/"
orders = spark.read.csv(base_path + "olist_orders_dataset.csv", header=True, inferSchema=True)
items = spark.read.csv(base_path + "olist_order_items_dataset.csv", header=True, inferSchema=True)
customers = spark.read.csv(base_path + "olist_customers_dataset.csv", header=True, inferSchema=True)
products = spark.read.csv(base_path + "olist_products_dataset.csv", header=True, inferSchema=True)
reviews = spark.read.csv(base_path + "olist_order_reviews_dataset.csv", header=True, inferSchema=True)\
               .withColumn("review_score", col("review_score").cast(IntegerType()))

# 3. Clean & Engineer Features
# Convert dates and calculate delivery delay
orders = orders.withColumn("estimated_date", to_timestamp("order_estimated_delivery_date")) \
               .withColumn("delivered_date", to_timestamp("order_delivered_customer_date")) \
               .withColumn("purchase_date", to_timestamp("order_purchase_timestamp")) \
               .withColumn("purchase_year", year("purchase_date")) \
               .withColumn("purchase_month", month("purchase_date"))

orders = orders.withColumn("delivery_delay_days", datediff(col("delivered_date"), col("estimated_date")))
orders = orders.withColumn("is_late", when(col("delivery_delay_days") > 0, 1).otherwise(0))

# Calculate product volume (proxy for size)
products = products.withColumn("product_volume_cm3", 
                               col("product_length_cm") * col("product_height_cm") * col("product_width_cm"))

# 4. Join Tables
master_df = orders.filter(col("order_status") == "delivered") \
    .join(items, "order_id", "left") \
    .join(customers, "customer_id", "left") \
    .join(products, "product_id", "left") \
    .join(reviews, "order_id", "left") \
    .fillna({"product_weight_g": 0, "product_volume_cm3": 0, "freight_value": 0, "review_score": 3})

# 5. Deeper Insight: Predictive Baseline (Logistic Regression)
# Predict 'is_late' using freight, weight, and volume
assembler = VectorAssembler(
    inputCols=["freight_value", "product_weight_g", "product_volume_cm3"], 
    outputCol="features", handleInvalid="skip")

ml_data = assembler.transform(master_df.select("freight_value", "product_weight_g", "product_volume_cm3", "is_late").na.drop())
train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="is_late", maxIter=10)
lr_model = lr.fit(train_data)
predictions = lr_model.transform(test_data)

# Join predictions back to master_df (simplified for the project)
master_df = master_df.withColumn("risk_of_late_delivery", col("is_late")) # In a real flow, you'd append the prediction probability here

# 6. Write to HDFS (Curated)
master_df.write.mode("overwrite").parquet("hdfs://namenode:9000/curated/olist/master_table")

# 7. Write to MongoDB (Serving Layer) using v10 Syntax
master_df.write.format("mongodb") \
    .option("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
    .option("spark.mongodb.write.database", "olist_db") \
    .option("spark.mongodb.write.collection", "master_table") \
    .mode("overwrite") \
    .save()

print("PySpark ETL & ML Pipeline Complete!")