from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

#spark session institation and spark context
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext
file_1 = "E:\\Pyspark POC - Data Sources\\CSV\\constructors.json"
file_2 = "E:\\Pyspark POC - Data Sources\\CSV\\drivers.json"
path_1 = "E:\Pyspark POC - Data Sources\CSV\output_json"

#reading a JSON file:
df = spark.read.json(file_1)
df.printSchema()  # by default JSON DATA SOURCES inferSchema from the input files
df.show(5,truncate=False)

#reading a JSON File:
df1 = spark.read \
    .format("org.apache.spark.sql.json") \
     .load(file_2)
df1.printSchema() # by default JSON DATA SOURCES inferSchema from the input files
df1.show(5, truncate=False)

#user defined complex Schema for input JSON File:
name_schema = StructType([ \
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

schema = StructType([ \
    StructField("driverId",IntegerType(),True),
    StructField("driverRef", StringType(), True),
    StructField("number",IntegerType(), True),
    StructField("code",StringType(),True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)])

df2 = spark.read \
    .format("json") \
    .load(file_2,schema=schema)


df2.printSchema()
df2.show(5, truncate=False)

#Write operations in DF:
#using Append mode:
df2.write \
    .format("json") \
    .mode("append") \
    .save(path_1)

#using Overwrite mode:
df2.write \
    .save(path_1,format="json",mode="Overwrite")

#mode -> Ignore
df2.write \
    .mode("ignore") \
    .save(path_1)

#mode -> Error or error if exists:
df2.write \
    .save(path_1, format="json", mode="error")

#partition by Nationality:
df2.write \
    .format("json") \
    .mode("overwrite") \
    .partitionBy("nationality") \
    .save(path_1)


