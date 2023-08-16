# Similar to SQL GROUP BY clause, PySpark groupBy() function is used to collect the identical data into groups
# on DataFrame and perform count, sum, avg, min, max functions on the grouped data.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000),
              ("Raman", "Finance", "CA", 99000, 40, 24000),
              ("Scott", "Finance", "NY", 83000, 36, 19000),
              ("Jen", "Finance", "NY", 79000, 53, 15000),
              ("Jeff", "Marketing", "CA", 80000, 25, 18000),
              ("Kumar", "Marketing", "NY", 91000, 50, 21000)
              ]

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count().show(truncate=False)

df.groupBy("department", "state") \
    .sum("salary", "bonus") \
    .avg("salary", "bonus") \
    .show(truncate=False)

# 4. Running more aggregates at a time
# Using agg() aggregate function we can calculate many aggregations at a time on a single statement using SQL functions
# sum(), avg(), min(), max() mean() e.t.c.
# In order to use these, we should import "from pyspark.sql.functions import sum,avg,max,min,mean,count"
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"),
         avg("salary").alias("avg_salary"),
         sum("bonus").alias("sum_bonus"),
         max("bonus").alias("max_bonus")) \
    .show(truncate=False)

# 5. Using filter on aggregate data
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"),
         avg("salary").alias("avg_salary"),
         sum("bonus").alias("sum_bonus"),
         max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)
