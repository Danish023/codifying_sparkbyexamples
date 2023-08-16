# How to perform a SQL query on a database table by using JDBC in PySpark? In order to query the database table using jdbc()
# you need to have a database server running, the database java connector, and connection details.

# By using an option dbtable or query with jdbc() method you can do the SQL query on the database table into PySpark DataFrame.

# Steps to query the database table using JDBC

# Step 1 – Identify the Database Java Connector version to use
# Step 2 – Add the dependency
# Step 3 – Query JDBC Table to PySpark Dataframe


# 1. PySpark Query JDBC Database Table

# To query a database table using jdbc() method, you would need the following:
# Server IP or Host name and Port,
# Database name,
# Table name,
# User and Password.

# JDBC is a Java standard to connect to any database as long as you provide the right JDBC connector jar in the classpath
# and provide a JDBC driver using the JDBC API. PySpark also leverages the same JDBC standard when using jdbc() method.


# 2. PySpark Query JDBC Table Example

# I have MySQL database emp and table employee with columns id, name, age and gender.
# I will use this JDBC table to run SQL queries and store the output in PySpark DataFrame. The below example extracts the
# complete table into DataFrame


# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "mysql-connector-java-8.0.13.jar") \
           .getOrCreate()

# Query table using jdbc()
df = spark.read \
    .jdbc("jdbc:mysql://localhost:3306/emp", "employee",
          properties={"user": "root", "password": "root", "driver":"com.mysql.cj.jdbc.Driver"})

# show DataFrame
df.show()

# Alternatively, you can also use the DataFrameReader.format("jdbc").load() to query the table. When you use this, you need
# to provide the database details with the option() method.
# Query from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/emp") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .load()


# 3. SQL Query Specific Columns of JDBC Table
# In the above example, it extracts the entire JDBC table into PySpark DataFrame. Sometimes you may be required to query
# specific columns with where condition. You can achieve this by using either dbtable or query options.

# Query from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/emp") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("query", "select id,age from employee where gender='M'") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df.show()


