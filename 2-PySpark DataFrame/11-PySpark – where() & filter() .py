# PySpark filter() function is used to filter the rows from RDD/DataFrame based on the given condition or
# SQL expression, you can also use where() clause instead of the filter() if you are coming from an SQL background,
# both these functions operate exactly the same.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

arrayStructureData = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

arrayStructureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=arrayStructureData, schema=arrayStructureSchema)
df.printSchema()
df.show(truncate=False)

# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- languages: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- state: string (nullable = true)
#  |-- gender: string (nullable = true)
#
# +----------------------+------------------+-----+------+
# |name                  |languages         |state|gender|
# +----------------------+------------------+-----+------+
# |[James, , Smith]      |[Java, Scala, C++]|OH   |M     |
# |[Anna, Rose, ]        |[Spark, Java, C++]|NY   |F     |
# |[Julia, , Williams]   |[CSharp, VB]      |OH   |F     |
# |[Maria, Anne, Jones]  |[CSharp, VB]      |NY   |M     |
# |[Jen, Mary, Brown]    |[CSharp, VB]      |NY   |M     |
# |[Mike, Mary, Williams]|[Python, VB]      |OH   |M     |
# +----------------------+------------------+-----+------+

# 2. DataFrame filter() with Column Condition
df.filter(df.state == "OH") \
    .show(truncate=False)

# not equals condition
df.filter(df.state != "OH") \
    .show(truncate=False)
df.filter(~(df.state == "OH")) \
    .show(truncate=False)

df.filter(col("state") == "OH") \
    .show(truncate=False)

# 3. DataFrame filter() with SQL Expression
df.filter("gender  == 'M'") \
    .show(truncate=False)

# 4. PySpark Filter with Multiple Conditions
df.filter((df.state == "OH") & (df.gender == "M")) \
    .show(truncate=False)

df.filter(array_contains(df.languages, "Java")) \
    .show(truncate=False)

df.filter(df.name.lastname == "Williams") \
    .show(truncate=False)

# 6. Filter Based on Starts With, Ends With, Contains

# Using startswith
df.filter(df.state.startswith("N")).show()
# +--------------------+------------------+-----+------+
# |                name|         languages|state|gender|
# +--------------------+------------------+-----+------+
# |      [Anna, Rose, ]|[Spark, Java, C++]|   NY|     F|
# |[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
# |  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
# +--------------------+------------------+-----+------+

# using endswith
df.filter(df.state.endswith("H")).show()

# contains
df.filter(df.state.contains("H")).show()

# 7. PySpark Filter like and rlike

data2 = [(2, "Michael Rose"), (3, "Robert Williams"),
         (4, "Rames Rose"), (5, "Rames rose")
         ]
df2 = spark.createDataFrame(data=data2, schema=["id", "name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()
# +---+----------+
# | id|      name|
# +---+----------+
# |  5|Rames rose|
# +---+----------+

# rlike - SQL RLIKE pattern (LIKE with Regex)
# This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
# +---+------------+
# | id|        name|
# +---+------------+
# |  2|Michael Rose|
# |  4|  Rames Rose|
# |  5|  Rames rose|
