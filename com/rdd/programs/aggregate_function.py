from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("aggregate function codes")\
    .getOrCreate()

orderItems = spark.sparkContext.textFile(r"C:\work\datasets\order_item.txt")

print(orderItems.count())

#get revenue for given order id
#
# for i in orderItems.take(10):
#     print(i)

orderItemsFilter = orderItems.\
    filter(lambda i,j : int(i.split(",")[1])== 2)

# for i in orderItemsFilter.take(20):
#     print(i)
#
orderItemsSubtotals = orderItemsFilter.map(lambda i :float(i.split(",")[4]))
#
for i in orderItemsSubtotals.take(10):
    print(i)
print("    ")

#
from operator import add
print(orderItemsSubtotals.reduce(add))
#
# print(orderItemsSubtotals.reduce(lambda x,y : x+y))


