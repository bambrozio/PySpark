from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("couny by key code").getOrCreate()

orders = spark.sparkContext.textFile(r"C:\work\datasets\orders.txt")

# for i in orders.take(30):
#     print(i)

ordersStatus = orders.map(lambda i:(i.split(",")[3],1))
#here 1 is just a number to satify the input format for countByKey
print(type(ordersStatus))
# for i in ordersStatus.take(30):
#     print(i)

#print(ordersStatus.countByKey())




