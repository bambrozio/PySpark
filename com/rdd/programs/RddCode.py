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










