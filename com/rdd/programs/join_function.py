from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("JOIN code")\
    .getOrCreate()

orders = spark.sparkContext.textFile(r'C:\work\datasets\orders.txt')
orderItems = spark.sparkContext.textFile(r'C:\work\datasets\order_item.txt')

orderMap = orders.map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))

orderItemsMap = orderItems.map(lambda x: (int(x.split(',')[1]),float(x.split(",")[4])))

# for i in orderItemsMap.take(10):
#     print(i)

ordersJoin = orderMap.join(orderItemsMap)

ordersJoinLeft = orderMap.leftOuterJoin(orderItemsMap)

# print(ordersJoinLeft.collect())
#
# for i in ordersJoinLeft.take(100):
#      print(i[1][1])

# ordersJoinLeftFilter = ordersJoinLeft.filter(lambda x: x[1][1] == None)
#
# for i in ordersJoinLeftFilter.take(100):
#     print(i)

#orderJoinLeftNoneValues= orderJoinLeft.filter(lambda x : x.split(",")[1][1])

# orderJoinLeftNoneValues= orderJoinLeft.map(lambda x : " ".join(str(x[1])))
#
# for j in orderJoinLeftNoneValues.take(100):
#     print(j)
#

ordersOuterJoin = orderMap.fullOuterJoin(orderItems)

for i in ordersOuterJoin.take(100):
    print(i)

print(" -------------------------------------")

for i in ordersJoin.take(100):
    print(i)


