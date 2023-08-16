# In order to connect to SQL-server (mssql) from PySpark, you would need the following. Make sure you have these details
# before you read or write to the SQL server. The driver I am going to use in this article is
# com.microsoft.sqlserver.jdbc.spark

# Driver to use (I will provide this)
# SQL server address & port
# Database name
# Table name
# User name and
# Password
# Steps to connect PySpark to SQL Server and Read and write Table.

# Step 1 – Identify the PySpark SQL Connector version to use
# Step 2 – Add the dependency
# Step 3 – Create SparkSession & Dataframe
# Step 4 – Save PySpark DataFrame to SQL Server Table
# Step 5 – Read SQL Table to PySpark Dataframe


# 1. PySpark Connector for SQL Server (mssql)

# PySpark connector for SQL server works with both:
# SQL Server on-prem
# Azure SQL


# 3. Create SparkSession & DataFrame

# Creating a SparkSession is a basic step to work with PySpark hence, first, let’s create a SparkSession and construct a
# sample DataFrame with columns id, name, age and gender.
# In the below sections, I will use this DataFrame to write into SQL Server table and read from it.

# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .config("spark.jars", "mysql-connector-java-6.0.6.jar") \
    .getOrCreate()

# Create DataFrame
columns = ["id", "name", "age", "gender"]
data = [(1, "James", 30, "M"), (2, "Ann", 40, "F"),
        (3, "Jeff", 41, "M"), (4, "Jennifer", 20, "F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)


# 4. Write PySpark DataFrame to SQL Server Table

# You would be required to provide the SQL Server details while writing PySpark DataFrame to SQL table.
# By using the format() specify the driver, this driver is defined in the connector dependency.

# Some points to note while writing.

# The mode("overwrite") drops the table if already exists by default and re-creates a new one without indexes.
# Use option("truncate","true") to retain the index.
# This connector by default uses READ_COMMITTED isolation level. You can change this using option
# ("mssqlIsolationLevel", "READ_UNCOMMITTED").

# Write to SQL Table
sampleDF.write \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .mode("overwrite") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .save()

# Here, used mode("overwrite") means if the table already exists with rows, overwrite the table with the rows from the
# DataFrame. The overwrite mode first drops the table if it already exists in the database.


# 5. Read SQL Server Table to PySpark DataFrame

# Similar to writing, with read() also you need to provide the driver and the SQL connection details.
# In the below example, I am reading a table employee from the database emp to the DataFrame.

# Read from SQL Table
df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .load()

df.show()


# 6. Select Specific Columns to Read

# In the above example, it reads the entire table into PySpark DataFrame. Sometimes you may not be required to select the
# entire table, so to select the specific columns, specify the query you wanted to select with dbtable option.

# Read from SQL Table
df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "select id,age from employee where gender='M'") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .load()

df.show()


# 7. Append Table
# Use spark.write.mode("append") to append the rows to the existing SQL Server table.

# Write to SQL Table
sampleDF.write \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .mode("append") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .save()
