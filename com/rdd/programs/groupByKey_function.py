#GET REVENUE FOR EACH ORDER ID

from  pyspark.sql import SparkSession

spark = SparkSession.builder.appName("revenue of each order id").getOrCreate()

data = spark.sparkContext.textFile(r'C:\work\datasets\order_item.txt')

data_map = data.map(lambda x: (int(x.split(",")[1]),x))
#data_map = data.map(lambda x: (int(x.split(",")[1]),float(x.split(",")[4])))
#above [4] is to get the sum of the sales
# for i in data_map.take(10):
#     print(i)

data_grp_by_key = data_map.groupByKey()

# for i in data_grp_by_key.take(10):
#     #print(i)
#     #print(i[1])
#     #print(list(i))
#     print(list(i[1]))

data_revenue = data_grp_by_key.map(lambda x:(x[0],round(sum(x[1]), 2)))

# for i in data_revenue.take(20):
#     print(i)


'''get order items details in descending order by revenue '''

#l=data_grp_by_key.first()

#print(sorted(l[1],key  = lambda k: float(k.split(",")[4]),reverse=True))

orderItemssortedBySubtotalPerOrder = data_grp_by_key.\
    flatMap(lambda i:
    sorted(i[1], key= lambda k: float(k.split(",")[4]), reverse=True))

for i in orderItemssortedBySubtotalPerOrder.take(20):
    print(i)



