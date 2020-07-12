from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("top function and takeOrdered").getOrCreate()

product = spark.sparkContext.textFile(r'C:\work\datasets\products.txt')

productFilter= product.filter(lambda x:x.split(",")[4] != "")

topProducts = productFilter.top(5,key = lambda k:float(k.split(",")[4]))

# for i in topProducts:
#     print(i)

takeOrderedProduct = productFilter.takeOrdered(5,lambda x:-float(x.split(",")[4]))
# negation helps to get in desc order
# for i in takeOrderedProduct:
#     print(i)

productMap = productFilter.map(lambda x: (int(x.split(",")[1]),x))

productGroupByKey = productMap.groupByKey()

# for i in productGroupByKey.take(10):
#     print(i)

var = productGroupByKey.first()

#print(list(var[1]))

topNProductByCategory = productGroupByKey.flatMap(lambda p: sorted(p[1],key=lambda k:float(k.split(",")[4]),reverse=True)[:3])
#we will get top 3 highest

for i in topNProductByCategory.take(10):
    print(i)



