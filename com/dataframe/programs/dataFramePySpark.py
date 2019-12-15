# from pyspark.sql import SparkSession
# from pyspark.sql import *
#
# spark=SparkSession.builder.appName("Data frame creation").config("spark.some.config.option", "same-value").getOrCreate()
#
# Student=Row("firstName","lstName","age", "phone")
#
# s1=Student("Rakesh","Sahoo",25,86556)
# s2=Student("Ramesh","Chndra",58,12369)
#
# StudentData=[s1,s2]
#
# df=spark.createDataFrame(StudentData)
#
# df.show()
#
# spark.stop()
#
#


from pyspark.sql import SparkSession
from pyspark.sql import *

spark=SparkSession.builder.appName("Practice Data frame").config("spark.some.config.option","same-value").getOrCreate()

Student=Row("name","age")

s1=Student("ram",18)
s2=Student("Sita",24)

StudentData=[s1,s2]

df=spark.createDataFrame(StudentData)

df.show()

