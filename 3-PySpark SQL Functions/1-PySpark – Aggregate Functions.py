# PySpark provides built-in standard Aggregate functions defined in DataFrame API, these come in handy when we need
# to make aggregate operations on DataFrame columns. Aggregate functions operate on a group of rows and calculate a
# single return value for every group.

# All these aggregate functions accept input as, Column type or column name in a string and several other arguments based
# on the function and return Column type.
#
# When possible try to leverage standard library as they are little-bit more compile-time safety, handles null and perform
# better when compared to UDF’s. If your application is critical on performance try to avoid using custom UDF at all costs
# as these are not guarantee on performance.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set, sum, avg, max, countDistinct, count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance, var_samp, var_pop

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]
schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |        James|     Sales|  3000|
# |      Michael|     Sales|  4600|
# |       Robert|     Sales|  4100|
# |        Maria|   Finance|  3000|
# |        James|     Sales|  3000|
# |        Scott|   Finance|  3300|
# |          Jen|   Finance|  3900|
# |         Jeff| Marketing|  3000|
# |        Kumar| Marketing|  2000|
# |         Saif|     Sales|  4100|
# +-------------+----------+------+

# 1. In PySpark approx_count_distinct() function returns the count of distinct items in a group.

# //approx_count_distinct()
print("approx_count_distinct: " + str(df.select(approx_count_distinct("salary")).collect()[0][0]))

# //Prints approx_count_distinct: 6

# 2. avg() function returns the average of values in the input column.

# //avg
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))

# //Prints avg: 3400.0

# 3. collect_list() function returns all values from an input column with duplicates.

# //collect_list
df.select(collect_list("salary")).show(truncate=False)

# +------------------------------------------------------------+
# |collect_list(salary)                                        |
# +------------------------------------------------------------+
# |[3000, 4600, 4100, 3000, 3000, 3300, 3900, 3000, 2000, 4100]|
# +------------------------------------------------------------+

# 4. collect_set() function returns all values from an input column with duplicate values eliminated.

# //collect_set
df.select(collect_set("salary")).show(truncate=False)

# +------------------------------------+
# |collect_set(salary)                 |
# +------------------------------------+
# |[4600, 3000, 3900, 4100, 3300, 2000]|
# +------------------------------------+

# 5. countDistinct() function returns the number of distinct elements in a columns

df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: " + str(df2.collect()[0][0]))

print("count: " + str(df.select(count("salary")).collect()[0]))
df.select(first("salary")).show(truncate=False)
df.select(last("salary")).show(truncate=False)
df.select(kurtosis("salary")).show(truncate=False)
df.select(max("salary")).show(truncate=False)
df.select(min("salary")).show(truncate=False)
df.select(mean("salary")).show(truncate=False)
df.select(skewness("salary")).show(truncate=False)
df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(truncate=False)
df.select(sum("salary")).show(truncate=False)
df.select(sumDistinct("salary")).show(truncate=False)
df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show(truncate=False)

