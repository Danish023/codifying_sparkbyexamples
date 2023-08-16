from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf = SparkConf()
conf.setAppName('wc')
conf.setMaster('local[*]')

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

lst = ['danish', 'sadruddin', 'ansari', 'is', 'a', 'data', 'engineer']

rdd = sc.parallelize(lst)

t1 = rdd.map(lambda x: (x, 1))  # -----------> (danish, 1) (sadruddin, 1)
t2 = t1.reduceByKey(lambda x, y: x + y)
t3 = t2.collect()

for i in t3:
    print(i)
    