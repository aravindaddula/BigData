from pyspark.sql import SparkSession
from configs import SPARK_JARS

spark = SparkSession.builder \
        .appName("Template Generation App") \
        .master("local[*]") \
        .config("spark.driver.memory","10g") \
        .config("spark.executor.memory","12g") \
        .config("spark.memory.fraction","0.8") \
        .config("spark.sql.files.maxPartitionBytes",'128MB') \
        .config("spark.sql.parquet.compression.codec","snappy") \
        .config("spark.sql.shuffle.partitions", "24") \
        .config("spark.default.parallelism", "24") \
        .config("spark.sql.legacy.charVarcharAsString","true") \
        .config("spark.jars", SPARK_JARS) \
        .getOrCreate()

spark.conf.set("spark.sql.crossJoin.enabled", True)
spark.conf.set("spark.sql.debug.maxToStringFields", 2000)
spark.SparkContext.setLogLevel("ERROR")
# .config("spark.sql.catalog.glue_catalog.warehouse", dmd_coll_path) \

# spark


    #    .config("spark.hadoop.fs.s3a.access.key", mis_access_key) \
        # .config("spark.hadoop.fs.s3a.secret.key", mis_secret_key) \
        # .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        # .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        # .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        # .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        # .config("spark.sql.defaultCatalog", "glue_catalog") \