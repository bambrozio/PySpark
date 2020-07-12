from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName("venu_sir_quest")\
    .getOrCreate()
#
data = spark.read.format("csv").option("header",True).option("inferSchema",True).load(r"C:\work\datasets\movies.csv")
#
# print(data.show())
#
# res = data.withColumn("newcol",expr("substring(title,1,length(title)-6)"))\
#     .withColumn("newYear",expr("substring(title,-6,6)"))\
#     .withColumn("newYear",regexp_replace("newYear","[\\(,\\)]","" ))\
#     .withColumn("newgenres",split("genres","\\|")).select(col("")+ (0 until 5).map(x : col("newgenres").getItem(x).as("col$x")):_)\
#     .select("movieid","newcol","newYear","genres", "col0","col1","col2","col3","col4")


#
res = data.withColumn("newcol",f.expr("substring(title,1,length(title)-6)"))\
    .withColumn("newYear",f.expr("substring(title,-6,6)"))\
    .withColumn("newYear",f.regexp_replace("newYear","[\\(,\\)]","" ))\
    .withColumn("newgenres",f.split("genres","\\|"))\
    .select(f.split(data.genres,":")).rdd.flatMap(lambda x:x ).toDF()

print(res.show())
#schema = ["col1","col2","col3","col4","col5"]
#print(type(res))
