'''
test DataFrame to check whether the code is working or not
'''

from pyspark.sql import SparkSession
import datetime
# Alternative Configuration
    # .config("spark.python.worker.reuse", "false") \
    # .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    # .config("spark.network.timeout", "600s") \
    # .config("spark.executor.heartbeatInterval", "120s") \
spark = SparkSession.builder \
    .appName("Spark Interview Preparation")\
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


data = [
    (101, "Alice", "HR", 50000),
    (102, "Bob", "IT", 60000),
    (103, "Charlie", "Finance", 55000),
    (104, "David", "IT", 70000),
    (105, "Eva", "HR", 52000),
    (106, "Frank", "Finance", 58000)
]

# Column names
columns = "Emp_ID int, Name string, Department string, Salary int"

# Create DataFrame
df = spark.createDataFrame(data, columns)

df.printSchema()
# Show data
df.show()


# 3,12.3s
