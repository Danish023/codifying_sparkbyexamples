# How to apply a function to a column in PySpark? By using withColumn(), sql(), select() you can apply a
# built-in function or custom function to a column. In order to apply a custom function, first you need to
# create a function and register the function as a UDF. Recent versions of PySpark provide a way to use Pandas API hence,
# you can also use pyspark.pandas.DataFrame.apply().

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

# Apply function using withColumn
from pyspark.sql.functions import upper
df.withColumn("Upper_Name", upper(df.Name)) \
  .show()

# Apply function using select
df.select("Seqno","Name", upper(df.Name)) \
  .show()

# Apply function using sql()
df.createOrReplaceTempView("TAB")
spark.sql("select Seqno, Name, UPPER(Name) from TAB") \
     .show()

# Create custom function
def upperCase(str):
    return str.upper()

# Convert function to udf
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
upperCaseUDF = udf(lambda x:upperCase(x),StringType())

# Custom UDF with withColumn()
df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)

# Custom UDF with select()
df.select(col("Seqno"), \
    upperCaseUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)

# Custom UDF with sql()
spark.udf.register("upperCaseUDF", upperCaseUDF)
df.createOrReplaceTempView("TAB")
spark.sql("select Seqno, Name, upperCaseUDF(Name) from TAB") \
     .show()
