from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("global ranking").getOrCreate()

product= spark.sparkContext.textFile(r'C:\work\datasets\products.txt')

productMap = product.filter(lambda x:x.split(",")[4] != "").\
    map(lambda p:(float(p.split(",")[4]),p))

productSortByPrice  = productMap.sortByKey(False)

for i in productSortByPrice.\
    map(lambda p:p[1]).take(5):
    print(i)
