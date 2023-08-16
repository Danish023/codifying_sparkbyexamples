# PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset
# (from all nodes) to the driver node. We should use the collect() on smaller dataset usually after filter(),
# group() e.t.c. Retrieving larger datasets results in OutOfMemory error.

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

print("Results")
dataCollect = deptDF.collect()
print(dataCollect)

# deptDF.collect() retrieves all elements in a DataFrame as an Array of Row type to the driver node.
# printing a resultant array yields the below output.
# [Row(dept_name='Finance', dept_id=10),
# Row(dept_name='Marketing', dept_id=20),
# Row(dept_name='Sales', dept_id=30),
# Row(dept_name='IT', dept_id=40)]


dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)

for row in dataCollect:
    print(row['dept_name'] + "," + str(row['dept_id']))

# deptDF.collect() returns Array of Row type.
# deptDF.collect()[0] returns the first element in an array (1st row).
# deptDF.collect[0][0] returns the value of the first row & first column.


# collect () vs select ()
# select() is a transformation that returns a new DataFrame and holds the columns that are selected
# whereas collect() is an action that returns the entire data set in an Array to the driver.
