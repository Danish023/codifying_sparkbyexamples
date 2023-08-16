# PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every
# element of RDD/DataFrame and returns a new RDD. In this article, you will learn the syntax and usage of the RDD map()
# transformation with an example and how to use it with DataFrame.
#
# RDD map() transformation is used to apply any complex operations like adding a column, updating a column, transforming the
# data e.t.c, the output of map transformations would always have the same number of records as input.
#
# Note1: DataFrame doesn’t have map() transformation to use with DataFrame hence you need to DataFrame to RDD first.
# Note2: If you have a heavy initialization use PySpark mapPartitions() transformation instead of map(), as with
# mapPartitions() heavy initialization executes only once for each partition instead of every record.


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project",
        "Gutenberg’s",
        "Alice’s",
        "Adventures",
        "in",
        "Wonderland",
        "Project",
        "Gutenberg’s",
        "Adventures",
        "in",
        "Wonderland",
        "Project",
        "Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)

rdd2 = rdd.map(lambda x: (x, 1))
for element in rdd2.collect():
    print(element)

data = [('James', 'Smith', 'M', 30),
        ('Anna', 'Rose', 'F', 41),
        ('Robert', 'Williams', 'M', 62),
        ]

columns = ["firstname", "lastname", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()

rdd2 = df.rdd.map(lambda x:
                  (x[0] + "," + x[1], x[2], x[3] * 2)
                  )
df2 = rdd2.toDF(["name", "gender", "new_salary"])
df2.show()

# Referring Column Names
rdd2 = df.rdd.map(lambda x:
                  (x["firstname"] + "," + x["lastname"], x["gender"], x["salary"] * 2)
                  )

# Referring Column Names
rdd2 = df.rdd.map(lambda x:
                  (x.firstname + "," + x.lastname, x.gender, x.salary * 2)
                  )


def func1(x):
    firstName = x.firstname
    lastName = x.lastname
    name = firstName + "," + lastName
    gender = x.gender.lower()
    salary = x.salary * 2
    return (name, gender, salary)


rdd2 = df.rdd.map(lambda x: func1(x))
