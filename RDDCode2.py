from pyspark import SparkContext

sc=SparkContext('local[2]','RDD Code')

data_file='C:\\work\\datasets\\matches.csv'

data_rdd=sc.parallelize(data_file)

print(data_rdd)

data_rdd2=data_rdd.map(lambda s:len(s))

print(data_rdd2)
