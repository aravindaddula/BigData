from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Read Parquet Example") \
    .getOrCreate()

# Read Parquet file
df = spark.read.parquet("D:/BigData/empfiles/empl appl detail.parquet")

# Display data
df.select("application_id","reporting_manager","recomended_ctc","ctc_approved","credit_report","n6").show(1000)
# df.printSchema()
