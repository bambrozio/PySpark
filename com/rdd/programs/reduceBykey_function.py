from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.appName("reduce by key").getOrCreate()

orderItem = spark.sparkContext.textFile(r'C:\work\datasets\order_item.txt')

orderItemMap = orderItem.map(lambda x:(int(x.split(",")[1]),float(x.split(",")[4])))

#revenuePerId = orderItemMap.reduceByKey(add)

revenuePerId = orderItemMap.reduceByKey(lambda x,y:x+y)

for i in revenuePerId.take(20):
    print(i)

#get minimum revenue of each ID

dataMap = orderItem.map(lambda x:int(x.split(",")[1]),x)

minSubtotalPerID = dataMap.reduceByKey( lambda x,y: x if (float(x.split(",")[4])< float(y.split(",")[4])) else y)

for i in minSubtotalPerID.take(20):
    print(i)
