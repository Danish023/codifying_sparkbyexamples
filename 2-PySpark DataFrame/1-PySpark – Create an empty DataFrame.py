# 1. Create Empty RDD in PySpark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

# Diplays
# EmptyRDD[188] at emptyRDD

# Creates Empty RDD using parallelize
rdd2 = spark.sparkContext.parallelize([])
print(rdd2)

# EmptyRDD[205] at emptyRDD at NativeMethodAccessorImpl.java:0
# ParallelCollectionRDD[206] at readRDDFromFile at PythonRDD.scala:262


# 2. Create Empty DataFrame with Schema (StructType)

# Create Schema

from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

# Create empty DataFrame from empty RDD
df = spark.createDataFrame(emptyRDD, schema)
df.printSchema()

# 3. Convert Empty RDD to DataFrame

# Convert empty RDD to Dataframe
df1 = emptyRDD.toDF(schema)
df1.printSchema()

# 4. Create Empty DataFrame with Schema.

# Create empty DataFrame directly.
df2 = spark.createDataFrame([], schema)
df2.printSchema()

# 5. Create Empty DataFrame without Schema (no columns)

# Create empty DatFrame with no schema (no columns)
df3 = spark.createDataFrame([], StructType([]))
df3.printSchema()

# print below empty schema
# root

