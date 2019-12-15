from pyspark.sql import SparkSession

sc=SparkSession.builder.appName("Airbnb_Dataframe").config("spark.some.config.option").getOrCreate()

#dfAirbnb=sc.createDataFrame("hdfs://user/dataframe/airbnb.xlsx")

airbnbDF=sc.read.csv("C:\\work\\BIG_DATA_DOCS\\airbnb.csv",inferSchema=True,header=True)

print("type of the variable: ",type(airbnbDF))

airbnbDF.show()

print(type(airbnbDF.collect()))
