#print("rak")

# from pyspark import SparkContext
# print("Pyspark import succefully imported")
#
# sc=SparkContext('local[2]', 'RddCode')
# print("Spark Context object created")

# from pyspark import SparkContext
# print("Pyspark import succefully imported")
#
# from pyspark.sql import SparkSession
# sparkSession = SparkSession.builder.appName("example").getOrCreate()
# print("SparkSession object is created successfully")

# from pyspark import SparkContext
#
# sc=SparkContext('local[2]', 'RddCode')
#
# l=[1,2,3,4]
#
# #RDD creation in below step
# listrdd=sc.parallelize(l)
#
# print(listrdd)
#
# #COLLECT is an action performed on RDD to print on output
# print(listrdd.collect())

# from pyspark import SparkContext
#
# sc=SparkContext('local[2]','Rdd Mapp code')
#
# l=[100,200,300,400]
#
# l_rdd=sc.parallelize(l)
#
# l_output_rdd=l_rdd.map(lambda x : x+50).collect()
#
# print(l_output_rdd)
#

# from pyspark import SparkContext
#
# sc=SparkContext('local[2]','Rdd filter CODE') #local means Spark run on locally and creating 2 cored in standalone cluster
#
# l=[10,20,30,40,50,60]
#
# l_rdd=sc.parallelize(l)
#
# l_output_rdd=l_rdd.filter(lambda x:x>20).collect()
#
# print(l_output_rdd)

# from pyspark import SparkContext
# sc=SparkContext('local[2]','Rdd distinct code')
# mylist=[1,2,3,3,4,5,4,6]
# mylist_rdd=sc.parallelize(mylist)
#
# mylist_trans_rdd=mylist_rdd.distinct()
#
# mylist_action_rdd=mylist_trans_rdd.collect()
#
# print(mylist_action_rdd)

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










