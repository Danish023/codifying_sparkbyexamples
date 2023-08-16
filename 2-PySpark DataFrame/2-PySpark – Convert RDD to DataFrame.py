# 1. Create PySpark RDD

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]

rdd = spark.sparkContext.parallelize(dept)

# 2. Convert PySpark RDD to DataFrame

# 2.1 Using rdd.toDF() function
df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

deptColumns = ["dept_name", "dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

# 2.2 Using PySpark createDataFrame() function
deptDF = spark.createDataFrame(rdd, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# 2.3 Using createDataFrame() with StructType schema
deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
