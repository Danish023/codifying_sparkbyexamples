# PySpark distinct() function is used to drop/remove the duplicate rows (all columns) from DataFrame and
# dropDuplicates() is used to drop rows based on selected (one or multiple) columns.

# Import pySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)
        ]

# Create DataFrame
columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |James        |Sales     |3000  |
# |Michael      |Sales     |4600  |
# |Robert       |Sales     |4100  |
# |Maria        |Finance   |3000  |
# |James        |Sales     |3000  |
# |Scott        |Finance   |3300  |
# |Jen          |Finance   |3900  |
# |Jeff         |Marketing |3000  |
# |Kumar        |Marketing |2000  |
# |Saif         |Sales     |4100  |
# +-------------+----------+------+

# 1. Get Distinct Rows (By Comparing All Columns)

distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

# Distinct count: 9
# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |James        |Sales     |3000  |
# |Michael      |Sales     |4600  |
# |Maria        |Finance   |3000  |
# |Robert       |Sales     |4100  |
# |Saif         |Sales     |4100  |
# |Scott        |Finance   |3300  |
# |Jeff         |Marketing |3000  |
# |Jen          |Finance   |3900  |
# |Kumar        |Marketing |2000  |
# +-------------+----------+------+

df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)

# 2. PySpark Distinct of Selected Multiple Columns

dropDisDF = df.dropDuplicates(["department", "salary"])
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)

# Distinct count of department & salary : 8
# +-------------+----------+------+
# |employee_name|department|salary|
# +-------------+----------+------+
# |Jen          |Finance   |3900  |
# |Maria        |Finance   |3000  |
# |Scott        |Finance   |3300  |
# |Michael      |Sales     |4600  |
# |Kumar        |Marketing |2000  |
# |Robert       |Sales     |4100  |
# |James        |Sales     |3000  |
# |Jeff         |Marketing |3000  |
# +-------------+----------+------+
