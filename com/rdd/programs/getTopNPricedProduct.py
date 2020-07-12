from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("get top N priced").getOrCreate()

product = spark.sparkContext.textFile(r'C:\work\datasets\products.txt')

productFilter = product.filter(lambda x: x.split(",")[4] != "")

#print(productFilter.collect())
#
productMap = productFilter.\
    map(lambda p:(int(p.split(",")[1]),p))

# for i in productMap.take(5):
#     print(i)

productGroupBy = productMap.groupByKey()

# for i in productGroupBy.take(10):
#     print(list(i[1]))

t = productGroupBy.first()

#print(list(t[1]))

f = productGroupBy.filter(lambda p:p[0] == 59).first()

#print(list(f[1]))

l = sorted(f[1],key=lambda k:k.split(",")[4],reverse = False)

for i in l:
    print(i)

l_map = map(lambda x:float(x.split(",")[4]),l)

#
#print(list(l_map))
print(sorted(l_map,reverse=True))
print(set(sorted(l_map,reverse=False)))
#
#print(productSorted)

