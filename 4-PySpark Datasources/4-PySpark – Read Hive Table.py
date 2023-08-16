# PySpark SQL supports reading a Hive table to DataFrame in two ways: the SparkSesseion.read.table() method and the
# SparkSession.sql() statement.

# In order to read a Hive table, you need to create a SparkSession with enableHiveSupport().
# This method is available at pyspark.sql.SparkSession.builder.enableHiveSupport() which is used to enable Hive support,
# including connectivity to a persistent Hive metastore, support for Hive SerDes, and Hive user-defined functions.

# Step 1 – Import PySpark
# Step 2 – Create SparkSession with Hive enabled

from os.path import abspath
from pyspark.sql import SparkSession

# enableHiveSupport() -> enables sparkSession to connect with Hive
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Step 3 – Read Hive table into Spark DataFrame using spark.sql()

df = spark.sql("select * from emp.employee")
df.show()

# Step 4 – Read using spark.read.table()

df = spark.read.table("employee")
df.show()

# Step 5 – Connect to remote Hive.

from os.path import abspath
from pyspark.sql import SparkSession

# enableHiveSupport() -> enables sparkSession to connect with Hive
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", "/hive/warehouse/dir") \
    .config("hive.metastore.uris", "thrift://remote-host:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# or Use the below approach
# Change using conf
spark.sparkContext().conf().set("spark.sql.warehouse.dir", "/user/hive/warehouse")
spark.sparkContext().conf().set("hive.metastore.uris", "thrift://localhost:9083")
