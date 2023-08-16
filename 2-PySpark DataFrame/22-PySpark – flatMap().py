# PySpark flatMap() is a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns)
# after applying the function on every element and returns a new PySpark RDD/DataFrame.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd = spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)

# Flatmap
rdd2 = rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

# Using flatMap() transformation on DataFrame

# Unfortunately, PySpark DataFame doesn’t have flatMap() transformation however, DataFrame has explode() SQL function
# that is used to flatten the column. Below is a complete example.

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]
df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])

from pyspark.sql.functions import explode

df2 = df.select(df.name, explode(df.knownLanguages))
df2.printSchema()
df2.show()

# root
#  |-- name: string (nullable = true)
#  |-- col: string (nullable = true)
#
# +---------+------+
# |     name|   col|
# +---------+------+
# |    James|  Java|
# |    James| Scala|
# |  Michael| Spark|
# |  Michael|  Java|
# |  Michael|  null|
# |   Robert|CSharp|
# |   Robert|      |
# |Jefferson|     1|
# |Jefferson|     2|
# +---------+------+

