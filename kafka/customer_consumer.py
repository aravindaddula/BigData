''' console based consumer
/opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --topic customer-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
import datetime
spark = SparkSession.builder \
    .appName("Write kafka into Deltalake")\
    .config("spark.drive.host","127.0.0.1") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f'spark session created {spark} at {datetime.datetime.now()}')

from pyspark.sql.types import *

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ])),
    StructField("transaction_type", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])


# spark.readStream.format('kafka')
try:
    cust_df = (spark.readStream.format('kafka')
                    .option('kafka.bootstrap.servers','localhost:9092')
                    .option('subscribe','customer-topic')
                    .option('startingOffsets','latest')
                    .load())

    # cust_df = cust_df.selectExpr("cast(value as string) as message","timestamp as batch_time")
    cust_df = cust_df.selectExpr("cast(value as string) as msg")
    cust_df=cust_df.select(from_json(col("msg"),schema).alias("message")).select("message.*")


except Exception as e:
    print(e)

''' Display results in console '''



results = cust_df.writeStream.format("console").option("truncate",False).start()
results.awaitTermination()


''' write the results into deltalake '''

# delta_tbl = '/home/jarvis/BigData/DeltaLake'

# try:
#     cust_df.writeStream \
#            .format("delta") \
#            .outputMode("append") \
#            .option("checkpointLocation", "/tmp/chkpt_fraud_alerts") \
#            .start(f"{delta_tbl}/numbers") \
#            .awaitTermination()
    
#     print(f"Write success into ../DeltaLake/numbers at {datetime.datetime.now()}")
# except Exception as e:
#     print(e)
