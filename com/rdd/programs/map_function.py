from pyspark import SparkContext

sc = SparkContext('local[2]','practice')

path=r'C:\Users\RAKA\Desktop\orders.txt'

dataRdd = sc.textFile(path)

#print(dataRdd.collect())

print(dataRdd.first())

print(dataRdd.map(lambda x : int(x.split(",")[1].split(" ")[0].replace("-"))).first())













