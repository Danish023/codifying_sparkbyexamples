# 1. Create SparkSession with Hive Enabled

from os.path import abspath
from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


# 2. PySpark Save DataFrame to Hive Table

# 2.1 Save DataFrame as Internal Table from PySpark
# By default saveAsTable() method saves PySpark DataFrame as a managed Hive table. Managed tables are also known as internal
# tables that are owned and managed by Hive. By default, Hive creates a table as an Internal table and owned the table structure
# and the files. When you drop an internal table, it drops the data and also drops the metadata of the table.

columns = ["id", "name", "age", "gender"]

# Create DataFrame
data = [(1, "James", 30, "M"), (2, "Ann", 40, "F"),
        (3, "Jeff", 41, "M"), (4, "Jennifer", 20, "F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Create Hive Internal table
sampleDF.write.mode('overwrite').saveAsTable("employee")

# Read Hive table
df = spark.read.table("employee")
df.show()

# As described above, it creates the Hive metastore metastore_db and Hive warehouse location spark-warehouse in the current
# directory (you can see this in IntelliJ). The employee table is created inside the warehouse directory.

# Also, note that by default it creates files in parquet format with snappy compression.

# If you wanted to create a table within a Database, use the prefix database name. If you don’t have the database,
# you can create one.

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS emp")

# Create Hive Internal table
sampleDF.write.mode('overwrite').saveAsTable("emp.employee")

# 2.2 Save as External Table
# To create an external table use the path of your choice using option(). The data in External tables are not owned or
# managed by Hive. Dropping an external table just drops the metadata but not the actual data. The actual data is still
# accessible outside of Hive.

# Create Hive External table
sampleDF.write.mode("Overwrite").option("path", "/path/to/external/table").saveAsTable("emp.employee")


# 3. Using PySpark SQL Temporary View to Save Hive Table

# Use SparkSession.sql() method and CREATE TABLE statement to create a table in Hive from PySpark temporary view.
# Above we have created a temporary view “sampleView“. Now we shall create a Database and Table using SQL in Hive Metastore
# and insert data into the Hive table using the view we created above.

# Create temporary view
sampleDF.createOrReplaceTempView("sampleView")

# Create a Database CT
spark.sql("CREATE DATABASE IF NOT EXISTS ct")

# Create a Table naming as sampleTable under CT database.
spark.sql("CREATE TABLE ct.sampleTable (id Int, name String, age Int, gender String)")

# Insert into sampleTable using the sampleView.
spark.sql("INSERT INTO TABLE ct.sampleTable  SELECT * FROM sampleView")

# Lets view the data in the table
spark.sql("SELECT * FROM ct.sampleTable").show()
