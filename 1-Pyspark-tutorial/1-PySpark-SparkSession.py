# Create SparkSession from builder

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()


# Create new SparkSession
spark2 = SparkSession.newSession
print(spark2)

# Underlying SparkContext will be the same for both sessions as you can have only one context per PySpark application


# Get Existing SparkSession
spark3 = SparkSession.builder.getOrCreate
print(spark3)


# Usage of config()
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("spark.some.config.option", "config-value") \
      .getOrCreate()


# Enabling Hive to use in Spark
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("spark.sql.warehouse.dir", "<path>/spark-warehouse") \
      .enableHiveSupport() \
      .getOrCreate()


# Set Config
spark.conf.set("spark.executor.memory", "5g")

# Get a Spark Config
partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(partitions)


# Get metadata from the Catalog
# List databases
dbs = spark.catalog.listDatabases()
print(dbs)

# Output
#[Database(name='default', description='default database',
#locationUri='file:/Users/admin/.spyder-py3/spark-warehouse')]

# List Tables
tbls = spark.catalog.listTables()
print(tbls)

#Output
#[Table(name='sample_hive_table', database='default', description=None, tableType='MANAGED', #isTemporary=False),
# Table(name='sample_hive_table1', database='default', description=None, #tableType='MANAGED', isTemporary=False),
# Table(name='sample_hive_table121', database='default', #description=None, tableType='MANAGED', isTemporary=False),
# Table(name='sample_table', database=None, #description=None, tableType='TEMPORARY', isTemporary=True)]

# Notice the two tables we have created, Spark table is considered a temporary table and Hive table as managed table.

