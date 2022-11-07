from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

#spark session institation and spark context
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext
path = "E:\\Pyspark POC - Data Sources\\CSV\\races.csv"
path_1 = "E:\Pyspark POC - Data Sources\CSV\output_csv"

#reading a CSV file:
df = spark.read.csv(path)
df.printSchema()
df.show(5,truncate=False)

#2. Reading a CSV file with inferSchema and Header option is True:
df1 = spark.read \
      .option("Header", True) \
      .option("inferSchema" , True) \
      .format("csv") \
      .load(path)

df1.printSchema()
df1.show(5,truncate=False)

#3. User defined Schema:
schema = StructType([
    StructField("race_id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuit_id", IntegerType(), True),
    StructField("circuit_name", StringType(), True),
    StructField("race_date", DateType(), True),
    StructField("race_time", StringType(),True),
    StructField("url",StringType(),True)])

df2 = spark.read \
    .option("header", True) \
    .format("csv") \
    .schema(schema) \
    .load(path)

df2.printSchema()
df2.show(3,truncate=False)

#Write operations in DF:
#mode -> Overwrite
df2.write\
    .options(header='True', delimiter="|") \
    .save(path_1,format='csv',mode="overwrite")

#mode -> append
df2.write \
    .options(header='True') \
    .options(delimiter='|') \
    .format("csv") \
    .mode("append") \
    .save(path_1)

#mode -> Ignore
df2.write \
    .mode("ignore") \
    .save(path_1)

#mode -> error
df2.write \
    .mode("error") \
    .save(path_1,format="csv")

#using partition-by:
df2.write \
    .options(header='True', delimiter=",") \
    .partitionBy("year") \
    .save(path_1,format='csv',mode='overwrite')

