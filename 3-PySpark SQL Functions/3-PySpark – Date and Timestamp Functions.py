# PySpark Date and Timestamp Functions are supported on DataFrame and SQL queries and they work similarly to
# traditional SQL, Date and Time are very important if you are using PySpark for ETL. Most of all these functions
# accept input as, Date type, Timestamp type, or String. If a String used, it should be in a default format that can
# be cast to date.

# The default format of the PySpark Date is yyyy-MM-dd.

# DateType default format is yyyy-MM-dd
# TimestampType default format is yyyy-MM-dd HH:mm:ss.SSSS
# Returns null if the input is a string that can not be cast to Date or Timestamp.

# PySpark SQL provides several Date & Timestamp functions hence keep an eye on and understand these.
# Always you should choose these functions instead of writing your own functions (UDF) as these functions are
# compile-time safe, handles null, and perform better when compared to PySpark UDF.
# If your PySpark application is critical on performance try to avoid using custom UDF at all costs as these are
# not guarantee performance.

# https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [["1", "2020-02-01"], ["2", "2019-03-01"], ["3", "2021-03-01"]]
df = spark.createDataFrame(data, ["id", "input"])
df.show()

# Result
# +---+----------+
# | id|input     |
# +---+----------+
# |  1|2020-02-01|
# |  2|2019-03-01|
# |  3|2021-03-01|
# +---+----------+

########### PySpark SQL Date Functions ###########

# 1. current_date()
# Use current_date() to get the current system date. By default, the data will be returned in yyyy-dd-mm format.

# current_date()
df.select(current_date().alias("current_date")).show(1)

# Result
# +------------+
# |current_date|
# +------------+
# |  2021-02-22|
# +------------+

# IMP
# 2. date_format()
# The below example uses date_format() to parse the date and converts from yyyy-dd-mm to MM-dd-yyyy format.

# date_format()
df.select(col("input"), date_format(col("input"), "MM-dd-yyyy").alias("date_format")).show()

# Result
# +----------+-----------+
# |input     |date_format|
# +----------+-----------+
# |2020-02-01| 02-01-2020|
# |2019-03-01| 03-01-2019|
# |2021-03-01| 03-01-2021|
# +----------+-----------+

# IMP
# 3. to_date()
# Below example converts string in date format yyyy-MM-dd to a DateType yyyy-MM-dd using to_date().
# You can also use this to convert into any specific format. PySpark supports all patterns supports on Java DateTimeFormatter.

# to_date()
df.select(col("input"), to_date(col("input"), "yyy-MM-dd").alias("to_date")).show()

# Result
# +----------+----------+
# |     input|   to_date|
# +----------+----------+
# |2020-02-01|2020-02-01|
# |2019-03-01|2019-03-01|
# |2021-03-01|2021-03-01|
# +----------+----------+

# 4. add_months() , date_add(), date_sub()

# add_months() , date_add(), date_sub()
df.select(col("input"),
          add_months(col("input"), 3).alias("add_months"),
          add_months(col("input"), -3).alias("sub_months"),
          date_add(col("input"), 4).alias("date_add"),
          date_sub(col("input"), 4).alias("date_sub")
          ).show()

# Result
# +----------+----------+----------+----------+----------+
# |     input|add_months|sub_months|  date_add|  date_sub|
# +----------+----------+----------+----------+----------+
# |2020-02-01|2020-05-01|2019-11-01|2020-02-05|2020-01-28|
# |2019-03-01|2019-06-01|2018-12-01|2019-03-05|2019-02-25|
# |2021-03-01|2021-06-01|2020-12-01|2021-03-05|2021-02-25|
# +----------+----------+----------+----------+----------+

# 5. datediff()
# The below example returns the difference between two dates using datediff().

# datediff()
df.select(col("input"), datediff(current_date(), col("input")).alias("datediff")).show()

# Result
# +----------+--------+
# |     input|datediff|
# +----------+--------+
# |2020-02-01|     387|
# |2019-03-01|     724|
# |2021-03-01|      -7|
# +----------+--------+

# 6. months_between()
# The below example returns the months between two dates using months_between().

# months_between()
df.select(col("input"), months_between(current_date(), col("input")).alias("months_between")).show()

# Result
# +----------+--------------+
# |     input|months_between|
# +----------+--------------+
# |2020-02-01|   12.67741935|
# |2019-03-01|   23.67741935|
# |2021-03-01|   -0.32258065|
# +----------+--------------+

#  7. year(), month(), month(),next_day(), weekofyear(), dayofweek(), dayofmonth(), dayofyear()

df.select(col("input"),
          year(col("input")).alias("year"),
          month(col("input")).alias("month"),
          next_day(col("input"), "Sunday").alias("next_day"),
          weekofyear(col("input")).alias("weekofyear")
          ).show()

df.select(col("input"),
          dayofweek(col("input")).alias("dayofweek"),
          dayofmonth(col("input")).alias("dayofmonth"),
          dayofyear(col("input")).alias("dayofyear"),
          ).show()

# Result
# +----------+---------+----------+---------+
# |     input|dayofweek|dayofmonth|dayofyear|
# +----------+---------+----------+---------+
# |2020-02-01|        7|         1|       32|
# |2019-03-01|        6|         1|       60|
# |2021-03-01|        2|         1|       60|
# +----------+---------+----------+---------+


# Result
# +----------+----+-----+----------+----------+
# |     input|year|month|  next_day|weekofyear|
# +----------+----+-----+----------+----------+
# |2020-02-01|2020|    2|2020-02-02|         5|
# |2019-03-01|2019|    3|2019-03-03|         9|
# |2021-03-01|2021|    3|2021-03-07|         9|
# +----------+----+-----+----------+----------+

# 8. trunc()
# The below example truncates the date at a specified unit using trunc().

# trunc()
df.select(col("input"),
          trunc(col("input"), "Month").alias("Month_Trunc"),
          trunc(col("input"), "Year").alias("Month_Year"),
          trunc(col("input"), "Month").alias("Month_Trunc")
          ).show()

# Result
# +----------+-----------+----------+-----------+
# |     input|Month_Trunc|Month_Year|Month_Trunc|
# +----------+-----------+----------+-----------+
# |2020-02-01| 2020-02-01|2020-01-01| 2020-02-01|
# |2019-03-01| 2019-03-01|2019-01-01| 2019-03-01|
# |2021-03-01| 2021-03-01|2021-01-01| 2021-03-01|
# +----------+-----------+----------+-----------+


# 8. from_unixtime(column)
# Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment
# in the current system time zone in the yyyy-MM-dd HH:mm:ss format.


# 9. from_unixtime(column, f)
# Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment
# in the current system time zone in the given format.


# IMP
# 10. unix_timestamp()
# Returns the current Unix timestamp (in seconds) as a long


# IMP
# 11. unix_timestamp(column)
# Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the default timezone and the
# default locale.


# 12. unix_timestamp(column, p)
# Converts time string with given pattern to Unix timestamp (in seconds).


#################### PySpark SQL Timestamp Functions ####################

# Below are some of the PySpark SQL Timestamp functions, these functions operate on both date and timestamp values.
# The default format of the Spark Timestamp is yyyy-MM-dd HH:mm:ss.SSSS

data = [
    ["1", "02-01-2020 11 01 19 06"],
    ["2", "03-01-2019 12 01 19 406"],
    ["3", "03-01-2021 12 01 19 406"]
]
df2 = spark.createDataFrame(data, ["id", "input"])
df2.show(truncate=False)

# Result
# +---+-----------------------+
# |id |input                  |
# +---+-----------------------+
# |1  |02-01-2020 11 01 19 06 |
# |2  |03-01-2019 12 01 19 406|
# |3  |03-01-2021 12 01 19 406|
# +---+-----------------------+

# 1. current_timestamp()

# current_timestamp()
df2.select(current_timestamp().alias("current_timestamp")).show(1, truncate=False)

# Result
# +-----------------------+
# |current_timestamp      |
# +-----------------------+
# |2021-02-22 20:13:29.673|
# +-----------------------+

# 2. hour(), Minute() and second()

# hour, minute,second
data = [["1", "2020-02-01 11:01:19.06"], ["2", "2019-03-01 12:01:19.406"], ["3", "2021-03-01 12:01:19.406"]]
df3 = spark.createDataFrame(data, ["id", "input"])

df3.select(col("input"),
           hour(col("input")).alias("hour"),
           minute(col("input")).alias("minute"),
           second(col("input")).alias("second")
           ).show(truncate=False)

# Result
# +-----------------------+----+------+------+
# |input                  |hour|minute|second|
# +-----------------------+----+------+------+
# |2020-02-01 11:01:19.06 |11  |1     |19    |
# |2019-03-01 12:01:19.406|12  |1     |19    |
# |2021-03-01 12:01:19.406|12  |1     |19    |
# +-----------------------+----+------+------+

# 3. to_timestamp()
# Converts string timestamp to Timestamp type format.

# to_timestamp()
df2.select(col("input"), to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")).show(truncate=False)

# Result
# +-----------------------+-----------------------+
# |input                  |to_timestamp           |
# +-----------------------+-----------------------+
# |02-01-2020 11 01 19 06 |2020-02-01 11:01:19.06 |
# |03-01-2019 12 01 19 406|2019-03-01 12:01:19.406|
# |03-01-2021 12 01 19 406|2021-03-01 12:01:19.406|
# +-----------------------+-----------------------+


#################### Date and Timestamp Window Functions ####################

# 1. window(timeColumn: Column, windowDuration: String, slideDuration: String, startTime: String): Column

# > Bucketize rows into one or more time windows given a timestamp specifying column. Window starts are inclusive but the
# window ends are exclusive, e.g. 12:05 will be in the window [12:05, 12:10) but not in [12:00,12:05). Windows can support
# microsecond precision. Windows in the order of months are not supported.


# 2. window(timeColumn: Column, windowDuration: String, slideDuration: String): Column

# > Bucketize rows into one or more time windows given a timestamp specifying column. Window starts are inclusive but the window
# ends are exclusive, e.g. 12:05 will be in the window [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond
# precision. Windows in the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC


# 3. window(timeColumn: Column, windowDuration: String): Column
# > Generates tumbling time windows given a timestamp specifying column. Window starts are inclusive but the window ends are
# exclusive, e.g. 12:05 will be in the window [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision.
# Windows in the order of months are not supported. The windows start beginning at 1970-01-01 00:00:00 UTC.








