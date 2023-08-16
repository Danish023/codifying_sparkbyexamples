# In PySpark, select() function is used to select single, multiple, column by index, all columns from the list
# and the nested columns from a DataFrame.
# PySpark select() is a transformation function hence it returns a new DataFrame with the selected columns.

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [
    ("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL")
]
columns = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)

# 1. Select Single & Multiple Columns From PySpark

df.select("firstname", "lastname").show()
df.select(df.firstname, df.lastname).show()
df.select(df["firstname"], df["lastname"]).show()

# By using col() function
from pyspark.sql.functions import col

df.select(col("firstname"), col("lastname")).show()

# Select columns by regular expression
df.select(df.colRegex("`^.*name*`")).show()

# 2. Select All Columns From List

# Select All columns from List
df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()

# 3. Select Columns by Index

# Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)

# Selects columns 2 to 4  and top 3 rows
df.select(df.columns[2:4]).show(3)

# 4. Select Nested Struct Columns from PySpark

data = [
    (("James", None, "Smith"), "OH", "M"),
    (("Anna", "Rose", ""), "NY", "F"),
    (("Julia", "", "Williams"), "OH", "F"),
    (("Maria", "Anne", "Jones"), "NY", "M"),
    (("Jen", "Mary", "Brown"), "NY", "M"),
    (("Mike", "Mary", "Williams"), "OH", "M")
]

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])
df2 = spark.createDataFrame(data=data, schema=schema)
df2.printSchema()
df2.show(truncate=False)  # shows all columns

# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- state: string (nullable = true)
#  |-- gender: string (nullable = true)
#
# +----------------------+-----+------+
# |name                  |state|gender|
# +----------------------+-----+------+
# |[James,, Smith]       |OH   |M     |
# |[Anna, Rose, ]        |NY   |F     |
# |[Julia, , Williams]   |OH   |F     |
# |[Maria, Anne, Jones]  |NY   |M     |
# |[Jen, Mary, Brown]    |NY   |M     |
# |[Mike, Mary, Williams]|OH   |M     |
# +----------------------+-----+------+

df2.select("name").show(truncate=False)

# +----------------------+
# |name                  |
# +----------------------+
# |[James,, Smith]       |
# |[Anna, Rose, ]        |
# |[Julia, , Williams]   |
# |[Maria, Anne, Jones]  |
# |[Jen, Mary, Brown]    |
# |[Mike, Mary, Williams]|
# +----------------------+

df2.select("name.firstname", "name.lastname").show(truncate=False)

# +---------+--------+
# |firstname|lastname|
# +---------+--------+
# |James    |Smith   |
# |Anna     |        |
# |Julia    |Williams|
# |Maria    |Jones   |
# |Jen      |Brown   |
# |Mike     |Williams|
# +---------+--------+

df2.select("name.*").show(truncate=False)

# +---------+----------+--------+
# |firstname|middlename|lastname|
# +---------+----------+--------+
# |James    |null      |Smith   |
# |Anna     |Rose      |        |
# |Julia    |          |Williams|
# |Maria    |Anne      |Jones   |
# |Jen      |Mary      |Brown   |
# |Mike     |Mary      |Williams|
# +---------+----------+--------+

