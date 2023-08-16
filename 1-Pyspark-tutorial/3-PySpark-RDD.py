
from pyspark.sql import SparkSession

spark = SparkSession.builder()\
      .master("local[1]")\
      .appName("SparkByExamples.com")\
      .getOrCreate()


# Create RDD from parallelize
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = spark.sparkContext.parallelize(data)


# Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("/path/textFile.txt")


# Reads entire file into a RDD as single record.
# wholeTextFiles() function returns a PairRDD with the key being the file path and value being file content.
rdd3 = spark.sparkContext.wholeTextFiles("/path/textFile.txt")

# Set parallelize manually –
# We can also set a number of partitions manually, all, we need is,
# to pass a number of partitions as the second parameter to these functions
# for example  sparkContext.parallelize([1,2,3,4,56,7,8,9,12,3], 10)

# RDD Transformations with example


rdd = spark.sparkContext.textFile("/tmp/test.txt")

rdd2 = rdd.flatMap(lambda x: x.split(" "))
# flatMap – flatMap() transformation flattens the RDD after applying the function and returns a new RDD.
# On the below example, first, it splits each record by space in an RDD and finally flattens it.
# Resulting RDD consists of a single word on each record.

rdd3 = rdd2.map(lambda x: (x, 1))

rdd4 = rdd3.reduceByKey(lambda a, b: a+b)

rdd5 = rdd4.map(lambda x: (x[1], x[0])).sortByKey()
# Print rdd5 result to console
print(rdd5.collect())

rdd4 = rdd3.filter(lambda x: 'an' in x[1])
print(rdd4.collect())

rdd5 = rdd4.map(lambda x: (x[1], x[0])).sortByKey()


# package com.sparkbyexamples.spark.rdd

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.textFile("/apps/sparkbyexamples/src/pyspark-examples/data.txt")

for element in rdd.collect():
    print(element)

#Flatmap
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)
#map
rdd3=rdd2.map(lambda x: (x,1))
for element in rdd3.collect():
    print(element)
#reduceByKey
rdd4=rdd3.reduceByKey(lambda a,b: a+b)
for element in rdd4.collect():
    print(element)
#map
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
for element in rdd5.collect():
    print(element)
#filter
rdd6 = rdd5.filter(lambda x : 'a' in x[1])
for element in rdd6.collect():
    print(element)


# RDD: List of transformations and Actions
# https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations







