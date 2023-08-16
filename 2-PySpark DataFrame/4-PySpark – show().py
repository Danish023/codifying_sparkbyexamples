# 2-PySpark DataFrame show() is used to display the contents of the DataFrame in a Table Row and Column Format.
# By default, it shows only 20 Rows, and the column values are truncated at 20 characters.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]

rdd = spark.sparkContext.parallelize(dept)

# Using rdd.toDF() function
df = rdd.toDF()

# 1. Quick Example of show()

# Default - displays 20 rows and
# 20 charactes from column value
df.show()

# Display full column contents
df.show(truncate=False)

# Display 2 rows and full column contents
df.show(2, truncate=False)

# Display 2 rows & column values 25 characters
df.show(2, truncate=25)

# Display DataFrame rows & columns vertically
df.show(n=3, truncate=25, vertical=True)


# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno", "Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]

df = spark.createDataFrame(data, columns)
df.show()

# Output
#+-----+--------------------+
#|Seqno|               Quote|
#+-----+--------------------+
#|    1|Be the change tha...|
#|    2|Everyone thinks o...|
#|    3|The purpose of ou...|
#|    4|            Be cool.|
#+-----+--------------------+


# Display full column contents
df.show(truncate=False)

#+-----+-----------------------------------------------------------------------------+
#|Seqno|Quote                                                                        |
#+-----+-----------------------------------------------------------------------------+
#|1    |Be the change that you wish to see in the world                              |
#|2    |Everyone thinks of changing the world, but no one thinks of changing himself.|
#|3    |The purpose of our lives is to be happy.                                     |
#|4    |Be cool.                                                                     |
#+-----+-----------------------------------------------------------------------------+


# Display 2 rows and full column contents
df.show(2, truncate=False)

#+-----+-----------------------------------------------------------------------------+
#|Seqno|Quote                                                                        |
#+-----+-----------------------------------------------------------------------------+
#|1    |Be the change that you wish to see in the world                              |
#|2    |Everyone thinks of changing the world, but no one thinks of changing himself.|
#+-----+-----------------------------------------------------------------------------+


# Display 2 rows & column values 25 characters
df.show(2, truncate=25)

#+-----+-------------------------+
#|Seqno|                    Quote|
#+-----+-------------------------+
#|    1|Be the change that you...|
#|    2|Everyone thinks of cha...|
#+-----+-------------------------+
#only showing top 2 rows


# Display DataFrame rows & columns vertically
df.show(n=3, truncate=25, vertical=True)

#-RECORD 0--------------------------
# Seqno | 1
# Quote | Be the change that you...
#-RECORD 1--------------------------
# Seqno | 2
# Quote | Everyone thinks of cha...
#-RECORD 2--------------------------
# Seqno | 3
# Quote | The purpose of our liv...
