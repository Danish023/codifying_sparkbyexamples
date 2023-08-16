# pyspark.sql.Column class provides several functions to work with DataFrame to manipulate the Column values,
# evaluate the boolean expression to filter rows, retrieve a value or part of a value from a DataFrame column,
# and to work with list, map & struct columns.

# Key Points:

# PySpark Column class represents a single Column in a DataFrame.
# It provides functions that are most used to manipulate DataFrame Columns & Rows.
# Some of these Column functions evaluate a Boolean expression that can be used with filter() transformation to filter the DataFrame Rows.
# Provides functions to get a value from a list column by index, map value by key & index, and finally struct nested column.
# PySpark also provides additional functions pyspark.sql.functions that take Column object and return a Column type.

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkConf
from pyspark.sql.functions import col

# conf = SparkConf()
# conf.setAppName('PySpark Column Class - Operators & Functions')
# conf.setMaster('local[*]')
# spark = SparkSession.builder.appName("PySpark Column Class - Operators & Functions").master("local[1]").getOrCreate()

spark = SparkSession.builder.master("local[1]") \
                    .appName('PySpark Column Class - Operators & Functions') \
                    .getOrCreate()

# 1. Create Column Class Object

data = [("James", 23), ("Ann", 40)]
# df = spark.createDataFrame(data, schema='fname, gender')
df = spark.createDataFrame(data).toDF("name.fname", "gender")
df.printSchema()

#root
# |-- name.fname: string (nullable = true)
# |-- gender: long (nullable = true)

# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df["gender"]).show()

# Accessing column name with dot (with backticks)
df.select(df["`name.fname`"]).show()

# Using SQL col() function
df.select(col("gender")).show()

# Accessing column name with dot (with backticks)
df.select(col("`name.fname`")).show()

# Create DataFrame with struct using Row class

data = [
    Row(name="James", prop=Row(hair="black", eye="blue")),
    Row(name="Ann", prop=Row(hair="grey", eye="black"))
]

df = spark.createDataFrame(data)
df.printSchema()
#root
# |-- name: string (nullable = true)
# |-- prop: struct (nullable = true)
# |    |-- hair: string (nullable = true)
# |    |-- eye: string (nullable = true)

# Access struct column
df.select(df.prop.hair).show()
df.select(df["prop.hair"]).show()
df.select(col("prop.hair")).show()

# Access all columns from struct
df.select(col("prop.*")).show()

# 2. PySpark Column Operators

data = [(100, 2, 1), (200, 3, 4), (300, 4, 4)]
df = spark.createDataFrame(data).toDF("col1", "col2", "col3")

# Arthmetic operations
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()

# 3. PySpark Column Functions
# refer: https://github.com/Danish023/Big-data/blob/main/spark-DF.md
# https://sparkbyexamples.com/pyspark/pyspark-column-functions/

# IMP ones

# 1. alias(*alias, **kwargs)
# 2. asc() / desc()
# 3. cast(dataType)
# 4. between(lowerBound, upperBound)
# 5. startswith(other) / endswith(other)
# 6. substr(startPos, length)
# 7. when(condition, value)
# 8.  getField()/ dropFields(*fieldNames)
# 9. withField(fieldName, col)
