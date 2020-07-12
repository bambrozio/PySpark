from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('sort by key ').getOrCreate()

#sort the data by price from products

products = spark.sparkContext.textFile(r'C:\work\datasets\products.txt')

productsMap = products.filter(lambda x:(float(x.split(",")[4] != ""),x))

# for i in productsMap.take(30):
#     print(i)
productsSortbyPrice = productsMap.sortByKey()

productsSortbyPriceMap = productsSortbyPrice.map(lambda x:x[1])

# for i in productsSortbyPriceMap.take(20):
#     print(i)

#sort data by product category and then product price desc
productsMap = products.filter(lambda p:p.split(",")[4] != "").\
    map(lambda p:((int(p.split(",")[1]),float(p.split(",")[4])),p))

for i in productsMap.take(30):
    print(i)

for i in productsMap.sortByKey().take(10):
    print(i)

