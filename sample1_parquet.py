from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

#instiating Spark Session and Spark context:
spark = SparkSession.builder.master("local[*]").appName("Parquet examples").getOrCreate()
sc = spark.sparkContext

file_1 = "E:\\Pyspark POC - Data Sources\\CSV\\races.csv"
path = "E:\Pyspark POC - Data Sources\CSV\output_parquet"

#reading a csv file into a DF:

df = spark.read \
    .option("Header", True) \
    .option("InferSchema", True) \
    .load(file_1,format="csv")

df.printSchema()
df.show(3, truncate=False)

#Writing a df without mentioning the format - by default writes it as parquet file:
df.write \
    .save(path)

#writing a df as parquet format and mode is append:
df.write \
    .format("parquet") \
    .mode("append") \
    .save(path)

#Using mode - OVerwrite:
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(path)

#using mode - Ignore:
df.write \
    .format("parquet") \
    .mode("ignore") \
    .save(path)

#using mode - error:
df.write \
    .format("parquet") \
    .mode("errorifexists") \
    .save(path)

#using partition by with overwrite:

df.write \
    .save(path, format="parquet", partitionBy="year",mode="overwrite")

#reading a parquet file:
df1 = spark.read \
    .format("parquet") \
    .load(path)

df1.printSchema()
df1.show(3,truncate=False)

#reading from a specfic partition:
df2 = spark.read \
    .format("parquet") \
    .load("E:\Pyspark POC - Data Sources\CSV\output_parquet\year=1950")

df2.printSchema()
df2.show()