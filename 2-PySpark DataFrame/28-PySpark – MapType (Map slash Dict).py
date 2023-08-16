# PySpark MapType (also called map type) is a data type to represent Python Dictionary (dict) to store key-value pair,
# a MapType object comprises three fields, keyType (a DataType), valueType (a DataType) and valueContainsNull (a BooleanType).
#
# What is PySpark MapType
# PySpark MapType is used to represent map key-value pair similar to python Dictionary (Dict), it extends DataType class
# which is a superclass of all types in PySpark and takes two mandatory arguments keyType and valueType of type DataType
# and one optional boolean argument valueContainsNull. keyType and valueType can be any type that extends the DataType class
# for e.g StringType, IntegerType, ArrayType, MapType, StructType (struct) e.t.c.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataDictionary = [
    ('James', {'hair': 'black', 'eye': 'brown'}),
    ('Michael', {'hair': 'brown', 'eye': None}),
    ('Robert', {'hair': 'red', 'eye': 'black'}),
    ('Washington', {'hair': 'grey', 'eye': 'grey'}),
    ('Jefferson', {'hair': 'brown', 'eye': ''})
]

schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(), StringType()), True)
])

df = spark.createDataFrame(data=dataDictionary, schema=schema)
df.printSchema()
df.show(truncate=False)

# root
#  |-- Name: string (nullable = true)
#  |-- properties: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
#
# +----------+-----------------------------+
# |Name      |properties                   |
# +----------+-----------------------------+
# |James     |[eye -> brown, hair -> black]|
# |Michael   |[eye ->, hair -> brown]      |
# |Robert    |[eye -> black, hair -> red]  |
# |Washington|[eye -> grey, hair -> grey]  |
# |Jefferson |[eye -> , hair -> brown]     |
# +----------+-----------------------------+

# 3. Access PySpark MapType Elements

df3 = df.rdd.map(lambda x: (x.name, x.properties["hair"], x.properties["eye"])).toDF(["name", "hair", "eye"])
df3.printSchema()
df3.show()

# root
#  |-- name: string (nullable = true)
#  |-- hair: string (nullable = true)
#  |-- eye: string (nullable = true)
#
# +----------+-----+-----+
# |      name| hair|  eye|
# +----------+-----+-----+
# |     James|black|brown|
# |   Michael|brown| null|
# |    Robert|  red|black|
# |Washington| grey| grey|
# | Jefferson|brown|     |
# +----------+-----+-----+

# 4. Functions

# 4.1 – explode

from pyspark.sql.functions import explode

df.select(df.name, explode(df.properties)).show()

# +----------+----+-----+
# |      name| key|value|
# +----------+----+-----+
# |     James| eye|brown|
# |     James|hair|black|
# |   Michael| eye| null|
# |   Michael|hair|brown|
# |    Robert| eye|black|
# |    Robert|hair|  red|
# |Washington| eye| grey|
# |Washington|hair| grey|
# | Jefferson| eye|     |
# | Jefferson|hair|brown|
# +----------+----+-----+
