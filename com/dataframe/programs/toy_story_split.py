from pyspark.sql import SparkSession
import pyspark.sql.functions as f
spark = SparkSession.builder.appName("split_fcnt").getOrCreate()

data = spark.read.format("csv").option("header",False).load(r'C:\work\datasets\toy_story.csv')

#data.show()

data.select(f.split('_c0'," ")).rdd.flatMap(lambda x :x)\
    .toDF().drop('_4')\
    .withColumn("col_new",f.concat(f.col('_1'),f.lit(" "),f.col('_2')))\
    .drop('_1','_2')\
    .withColumnRenamed("_5",'type')\
    .withColumnRenamed('_3','year').withColumnRenamed("col_new",'movie').show()
data.selectExpr()

