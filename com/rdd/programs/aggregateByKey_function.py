from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("aggregate funcion").getOrCreate()

orderItem =  spark.sparkContext.textFile(r'C:\work\datasets\order_item.txt')

orderItemMap = orderItem.map(lambda x:(int(x.split(",")[1]),float(x.split(",")[4])))

for i in orderItemMap.take(30):
    print(i)

revenuePerOrder = orderItemMap.aggregateByKey((0.0,0),
                  lambda x,y: (x[0] + y,x[1]+1),
                         lambda x,y:(x[0]+y[0], x[1]+y[1]))

for i in revenuePerOrder.take(20):
    print(i)







