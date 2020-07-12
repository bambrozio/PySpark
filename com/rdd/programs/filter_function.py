from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("filter function").getOrCreate()

data = r'C:\work\datasets\orders.txt'

data_rdd = spark.sparkContext.textFile(data)

print(data_rdd.count())
#print(data_rdd.take(15))

for i in data_rdd.take(15):
    print(i)


# data_rdd_filter = data_rdd.filter(lambda x: x.split(",")[3] == 'COMPLETE')
#
# for i in data_rdd_filter.take(10):
#     print(i)

#print("filter's count is -",data_rdd_filter.count())

#data_rdd_only_complete = data_rdd.filter(lambda x: x.split(",")[3] == 'COMPLETE').map(lambda x:x.split(",")[3])

# for i in data_rdd_only_complete.take(10):
#     print(i)

'''both in , or is working '''
#complete_closed_rdd = data_rdd.filter(lambda x : x.split(",")[3] in ('COMPLETE',"CLOSED"))
# complete_closed_rdd = data_rdd.filter(lambda x:x.split(",")[3] == 'COMPLETE' or x.split(",")[3] == "CLOSED")
# for i in complete_closed_rdd.take(20):
#     print(i)


#data_rdd_14_jan = data_rdd.filter(lambda x:x.split(",")[1] >= '2013-12-01').filter(lambda x:x.split(",")[3] in ("COMPLETE","CLOSED"))
data_rdd_14_jan = data_rdd.filter(lambda x:(x.split(",")[3] in ("COMPLETE","CLOSED")) and (x.split(",")[1] >= '2013-12-01'))
for i in data_rdd_14_jan.take(20):
    print(i)
