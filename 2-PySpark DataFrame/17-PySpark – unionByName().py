# Difference between PySpark uionByName() vs union()
# The difference between unionByName() function and union() is that this function
# resolves columns by name (not by position). In other words, unionByName() is used to merge two DataFrames by
# column names instead of by position.
# 
# unionByName() also provides an argument allowMissingColumns to specify if you have a different column counts.
# In case you are using an older than Spark 3.1 version, use the below approach to merge DataFrames with different column names.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create DataFrame df1 with columns name, and id
data = [("James", 34), ("Michael", 56),
        ("Robert", 30), ("Maria", 24)]

df1 = spark.createDataFrame(data=data, schema=["name", "id"])
df1.printSchema()

# Create DataFrame df2 with columns name and id
data2 = [(34, "James"), (45, "Maria"), (45, "Jen"), (34, "Jeff")]

df2 = spark.createDataFrame(data=data2, schema=["id", "name"])
df2.printSchema()

# Using unionByName()
df3 = df1.unionByName(df2)
df3.printSchema()
df3.show()

# 4. Use unionByName() with Different Number of Columns
# In the above example we have two DataFrames with the same column names but in different order.
# If you have a different number of columns then use allowMissingColumns=True.
# When using this, the result of the DataFrmae contains null values for the columns that are missing on the DataFrame.
# Using allowMissingColumns
df1 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])
df3 = df1.unionByName(df2, allowMissingColumns=True)
df3.printSchema()
df3.show()
