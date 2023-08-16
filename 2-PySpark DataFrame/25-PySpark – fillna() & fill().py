# In PySpark, DataFrame.fillna() or DataFrameNaFunctions.fill() is used to replace NULL/None values on all or
# selected multiple DataFrame columns with either zero(0), empty string, space, or any constant literal values.
#
# While working on 2-PySpark DataFrame we often need to replace null values since certain operations on null value
# return error hence, we need to graciously handle nulls as the first step before processing.
# Also, while writing to a file, it’s always best practice to replace null values, not doing this result nulls on
# the output file.
#
# As part of the cleanup, sometimes you may need to Drop Rows with NULL/None Values in 2-PySpark DataFrame and Filter
# Rows by checking IS NULL/NOT NULL conditions.

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

filePath = "resources/small_zipcode.csv"
df = spark.read.options(header='true', inferSchema='true') \
    .csv(filePath)

df.printSchema()
df.show(truncate=False)

# +---+-------+--------+-------------------+-----+----------+
# |id |zipcode|type    |city               |state|population|
# +---+-------+--------+-------------------+-----+----------+
# |1  |704    |STANDARD|null               |PR   |30100     |
# |2  |704    |null    |PASEO COSTA DEL SUR|PR   |null      |
# |3  |709    |null    |BDA SAN LUIS       |PR   |3700      |
# |4  |76166  |UNIQUE  |CINGULAR WIRELESS  |TX   |84000     |
# |5  |76177  |STANDARD|null               |TX   |null      |
# +---+-------+--------+-------------------+-----+----------+

# Replace 0 for null for all integer columns
df.fillna(value=0).show()
df.na.fill(value=0).show()

# Replace 0 for null on only population column
df.fillna(value=0, subset=["population"]).show()
df.na.fill(value=0, subset=["population"]).show()

df.fillna(value="").show()
df.na.fill(value="").show()

df.fillna("unknown", ["city"]).fillna("", ["type"]).show()

df.fillna({"city": "unknown", "type": ""}).show()

df.na.fill("unknown", ["city"]).na.fill("", ["type"]).show()

df.na.fill({"city": "unknown", "type": ""}).show()
