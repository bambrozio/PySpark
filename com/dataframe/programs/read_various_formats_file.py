from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("practice").getOrCreate()

data = spark.read.format("json").load(r"C:\work\datasets\world_bank.json")

#data1 = spark.read.format("xml").load(r"C:\work\datasets\books.xml")
#data1.show()

data.show()








