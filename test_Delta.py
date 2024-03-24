import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.range(0, 5)
data.show()

# generate new data
data = spark.range(5, 10)
data.show()

# updating the data
print("updating the data...")
data.write.format("delta")\
    .mode("overwrite")\
    .save("delta-table")

print("print the values updated:")
data.show()

# time travel..
print("printing the first version of data:")
df = spark.read.format("delta")\
    .option("versionAsOf", 0)\
    .load("delta-table")

# printing the table...
df.show()
