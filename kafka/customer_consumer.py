''' console based consumer
/opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --topic customer-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
'''

from pyspark.sql import SparkSession
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

# spark.readStream.format('kafka')

cust_df = (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers','localhost:9092')
                .option('subscribe','customer-topic')
                .option('startingOffsets','latest')
                .load())

cust_df = cust_df.selectExpr("cast(value as string) as message","timestamp as batch_time")


''' Display results in console '''

# results = cust_df.writeStream.format("console").option("truncate",False).start()
# results.awaitTermination()


''' write the results into deltalake '''

delta_tbl = '/home/jarvis/BigData/DeltaLake'

try:
    cust_df.writeStream \
           .format("delta") \
           .outputMode("append") \
           .option("checkpointLocation", "/tmp/chkpt_fraud_alerts") \
           .start(f"{delta_tbl}/numbers") \
           .awaitTermination()
    
    print(f"Write success into ../DeltaLake/numbers at {datetime.datetime.now()}")
except Exception as e:
    print(e)