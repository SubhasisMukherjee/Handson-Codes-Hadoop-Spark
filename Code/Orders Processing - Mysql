from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# Connect to Mysql database and fetch orders table data into dataframe
orders = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://ip-172-31-20-247/retail_db") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "orders") \
        .option("user", "sqoopuser") \
        .option("password", "NHkkP876rp") \
        .load()

ord_schema = StructType(
    [StructField("order_id", IntegerType(), True),
     StructField("order_date", StringType(), True),
     StructField("order_customer_id", IntegerType(), True),
     StructField("order_status", StringType(), True)])
    
orders = spark.read \
        .format('csv') \
        .options(schema=ord_schema) \
        .load("datasets/retail_db/orders/part-00000")
orders.show()        

orders = orders.withColumnRenamed("_c0","order_id")
orders = orders.withColumnRenamed("_c1","order_date")
orders = orders.withColumnRenamed("_c2","order_customer_id")
orders = orders.withColumnRenamed("_c3","order_status")

# Save the orders as parquet file in HDFS
orders.write.format("parquet").mode("overwrite").save("orders")

# Define customer schema
cust_schema = StructType(
    [StructField("customer_id", IntegerType(), True),
     StructField("customer_fname", StringType(), True),
     StructField("customer_lname", StringType(), True),
     StructField("customer_email", StringType(), True),
     StructField("customer_password", StringType(), True),
     StructField("customer_street", StringType(), True),
     StructField("customer_city", StringType(), True),
     StructField("customer_state", StringType(), True),
     StructField("customer_zipcode", StringType(), True)])
     
# Create a dataframe customers, based on the customers table in retail_db in mysql database.
customers = spark.read \
        .format('csv') \
        .options(schema=cust_schema) \
        .load("datasets/retail_db/customers/part-00000")
        
customers = customers.withColumnRenamed("_c0","customer_id")
customers = customers.withColumnRenamed("_c1","customer_fname")
customers = customers.withColumnRenamed("_c2","customer_lname")
customers = customers.withColumnRenamed("_c3","customer_email")
customers = customers.withColumnRenamed("_c4","customer_password")
customers = customers.withColumnRenamed("_c5","customer_street")
customers = customers.withColumnRenamed("_c6","customer_city")
customers = customers.withColumnRenamed("_c7","customer_state")
customers = customers.withColumnRenamed("_c8","customer_zipcode")
customers.show()

# Create an orders based on the orders parquet file.
orders = spark.read.load("orders")
orders.show(6)

# Write a query joining customers table in mysql and orders parquet file in HDFS to find most number of complet orders.
orders.alias('o') \
.join(customers.alias('c'), col("o.order_customer_id") == col("c.customer_id")) \
.filter(col("o.order_status") == "COMPLETE") \
.groupBy("order_customer_id", "customer_fname", "customer_lname") \
.count() \
.orderBy(col("count").desc()) \
.show()

orders.write.mode("overwrite").saveAsTable("orders")


