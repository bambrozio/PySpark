from pyspark.sql import SparkSession
from pyspark import *

sc=SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

data=sc.textFile()
