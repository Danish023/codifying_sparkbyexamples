# PySpark foreach() is an action operation that is available in RDD, DataFram to iterate/loop over each element
# in the DataFrmae, It is similar to for with advanced concepts. This is different than other actions as foreach()
# function doesn’t return a value instead it executes the input function on each element of an RDD, DataFrame

# 1.2 PySpark foreach() Usage
# When foreach() applied on 2-PySpark DataFrame, it executes a function specified in foreach element of DataFrame.
# This operation is mainly used if you wanted to manipulate accumulators, save the DataFrame results to RDBMS tables,
# Kafka topics, and other external sources.

# Import
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare Data
columns = ["Seqno", "Name"]
data = [("1", "john jones"),
        ("2", "tracey smith"),
        ("3", "amy sanders")]

# Create DataFrame
df = spark.createDataFrame(data=data, schema=columns)
df.show()

# foreach() Example


def f(df):
    print(df.Seqno)


df.foreach(f)

# foreach() with accumulator Example
accum = spark.sparkContext.accumulator(0)

df.foreach(lambda x: accum.add(int(x.Seqno)))
print(accum.value)  # Accessed by driver

