from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define the data
structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), None, "F", -1)
]

# Define the schema
structSchema = StructType([
    StructField("fullname",
                StructType([
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True)
                ])),
    StructField("regno", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data=structureData, schema=structSchema)

# Select columns and show the DataFrame
empdata = df.select("fullname.firstname", "fullname.middlename", "fullname.lastname", "regno", "sex")
empdata.show(truncate=False)

# Write the DataFrame to ORC format
empdata.write.format("orc").save("emp_details")

# Stop Spark session
spark.stop()

