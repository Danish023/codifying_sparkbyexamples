# PySpark StructType & StructField classes are used to programmatically specify the schema to the DataFrame
# and create complex columns like nested struct, array, and map columns.
# StructType is a collection of StructField’s that defines column name, column data type, boolean to specify
# if the field can be nullable or not and metadata.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, struct, when
import json
from msilib import schema

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
]

structureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), False)
])

df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)

# Using PySpark SQL function struct(), we can change the struct of the existing DataFrame and add a new StructType to it
# The below example demonstrates how to copy the columns from one structure to another and adding a new column.
# PySpark Column Class also provides some functions to work with the StructType column.

updatedDF = df2.withColumn("OtherInfo",
                           struct(
                               col("id").alias("identifier"),
                               col("gender").alias("gender"),
                               col("salary").alias("salary"),
                               when(col("salary").cast(IntegerType()) < 2000, "Low")
                                   .when(col("salary").cast(IntegerType()) < 4000, "Medium")
                                   .otherwise("High").alias("Salary_Grade")
                           )
                           ).drop("id", "gender", "salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)

print(df2.schema.json())

# import schema from a json file
schemaFromJson = StructType.fromJson(json.loads(schema.json))
df3 = spark.createDataFrame(spark.sparkContext.parallelize(structureData), schemaFromJson)
df3.printSchema()


# Checking if a Column Exists in a DataFrame
print('Checking if a column exists in a DF')
print(df2.schema.fieldNames())
print(df2.schema.fieldNames.contains("firstname"))
print(df2.schema.contains(StructField("firstname", StringType(), True)))
