from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("set operation").getOrCreate()

orders = spark.sparkContext.textFile(r'C:\work\datasets\orders.txt')

ordersItem = spark.sparkContext.textFile(r'C:\work\datasets\order_item.txt')

orders201312map = orders.\
    filter(lambda o:o.split(",")[1][:7] == "2013-12").\
    map(lambda o:(int(o.split(",")[0]),o))

# for i in orders201312.take(20):
#     print(i)

orders201401map = orders.filter(lambda o:o.split(",")[1][0:7] == '2014-01').\
    map(lambda o:(int(o.split(",")[0]),o))

# for i in orders201401.take(10):
#     print(i)

ordersItemMap = ordersItem.\
    map(lambda o:(int(o.split(",")[1]),o))

# for i in ordersItemMap.take(10):
#     print(i)


orders201312Join = orders201312map.join(ordersItemMap)
orders201401Join = orders201401map.join(ordersItemMap)
#
for i in orders201312Join.take(20):
    print(i)
print("-------------------------------------")
# print("2014 data as below")

# for i in orders201401Join.take(20):
#     print(i)
print("-------------------------------------")
#perform set operation on product_id of both the dataset

orderItems201312 = orders201312Join.map(lambda x: x[1][1])

for i in orderItems201312.take(10):
    print(i)
print("-------------------------------------")
orderItems201401 = orders201401Join.map(lambda x:x[1][1])

# for i in orderItems201401.take(10):
#     print(i)

##Set operation Union - GET PRODUCT ID SOLD IN 2013-12 and 2014-01

product201312 = orderItems201312.map(lambda p:p.split(",")[2])

product201401 = orderItems201401.map(lambda p:p.split(",")[2])

for i in product201312.take(10):
    print(i)
print("-------------------------------------")
for i in product201401.take(10):
    print(i)
print("-------------------------------------")

allproducts = product201312.union(product201401)

for i in allproducts.take(10):
    print(i)

# for i in allproducts.distinct().take(20):
#     print(i)

print("-------------------------------------")
#get the prduct ids which are sold in both 201312 and 201401

commonproducts = product201312.intersection(product201401)

for i in commonproducts.take(10):
    print(i)




