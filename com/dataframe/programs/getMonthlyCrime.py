#from pyspark.sql import SparkSession
from pyspark import SparkContext
#spark=SparkSession.builder.appName("GetCrimeName").master('local').getOrCreate()

#print(spark)

sc=SparkContext('local[2]','getCrimeMonthly')

print(sc)

crimeData='C:\\work\\datasets\\crime_incidents_2013_CSV.csv'

rddCrimeData=sc.textFile(crimeData)

print(rddCrimeData)

for i in rddCrimeData.take(10):
    print(i)


