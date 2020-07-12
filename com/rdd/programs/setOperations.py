from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("set operation").getOrCreate()

orders = spark.sparkContext.textFile(r'C:\work\datasets\orders.txt')

ordersItem = spark.sparkContext.textFile(r'C:\work\datasets\order_item.txt')

orders201312 = orders.\
    filter(lambda o:o.split(",")[1][:7] == "2013-12").\
    map(lambda o:(o.split(",")[0],o))

for i in orders201312.take(20):
    print(i)

orders201401 = orders.filter(lambda o:o.split(",")[1][0:7] == '2014-01').\
    map(lambda o:(o.split(",")[0],o))

for i in orders201401.take(10):
    print(i)

ordersItemMap = ordersItem.\
    map(lambda o:(int(o.split(",")[1]),o))

for i in ordersItemMap.take(10):
    print(i)


orders201312Join = orders201312.join(ordersItemMap)
orders201401Join = orders201401.join(ordersItemMap)

for i in orders201312Join.take(20):
    print(i)














