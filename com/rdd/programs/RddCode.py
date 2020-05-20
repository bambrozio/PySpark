from pyspark import SparkContext
print("Pyspark import succefully imported")

sc=SparkContext('local[2]','BasicExample')
print("Spark Context object created")

l=range(0,100)

print("type of data entered:-",type(l))

lRDD=sc.parallelize(l)

print(type(lRDD))

#COLLECT is an action performed on RDD to print on output
print(lRDD.collect())

print(lRDD.first())

#MAP: map is a lazy function
lRDD_map=lRDD.map(lambda x: x+13)
print(lRDD_map.collect())

lRDD_map_2=lRDD.map(lambda y: y>=23)
print(lRDD_map_2.collect())

lRDD_map_3=lRDD.map(lambda z: z **10)
print(lRDD_map_3.collect())

#Distinct
print("Distinct",lRDD.distinct().collect())


# from pyspark import SparkContext
# import pyspark.sql.dataframe
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# # spark = SparkSession \
# #        .builder \
# #        .appName("RandomForest") \
# #        .config("spark.executor.heartbeatInterval","60s")\
# #        .getOrCreate()
# spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
# sc = spark.sparkContext
# lines = sc.textFile("people.txt")
# parts = lines.map(lambda l: l.split(","))
# people = parts.map(lambda p: Row(name=p[0],age=int(p[1])))
# peopledf = spark.createDataFrame(people)
# people = parts.map(lambda p: Row(name=p[0],age=int(p[1].strip())))
# schemaString = "name age"
# fields = [StructField(field_name, StringType(), True) for
# field_name in schemaString.split()]
# schema = StructType(fields)
# spark.createDataFrame(people, schema).show()


# df = spark.read.json("customer.json")
# df.show()
#
# sc = spark.sparkContext
# import json
# j = {'abc':1, 'def':2, 'ghi':3}
# a=[json.dumps(j)]
# jsonRDD = sc.parallelize(a)
# df = spark.read.json(jsonRDD)
# df.show()










