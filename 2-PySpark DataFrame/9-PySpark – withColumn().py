import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

# 1. Change DataType using PySpark withColumn()
df2 = df.withColumn("salary", col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

# 2. Update The Value of an Existing Column
df3 = df.withColumn("salary", col("salary") * 100)
df3.printSchema()
df3.show(truncate=False)

# 3. Create a Column from an Existing
df4 = df.withColumn("CopiedColumn", col("salary") * -1)
df4.printSchema()

# 4. Add a New Column using withColumn()
df5 = df.withColumn("Country", lit("USA"))
df5.printSchema()

df6 = df.withColumn("Country", lit("USA")) \
    .withColumn("anotherColumn", lit("anotherValue"))
df6.printSchema()

# 5. Rename Column Name
df.withColumnRenamed("gender", "sex") \
    .show(truncate=False)

# 6. Drop Column From 2-PySpark DataFrame
df4.drop("CopiedColumn") \
    .show(truncate=False)
