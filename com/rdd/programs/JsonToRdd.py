'''Below Code is to Show JSON data'''


from pyspark import SparkContext
import json
from pyspark.sql import SparkSession

sc=SparkContext('local[2]','JSON to RDD')

spark = SparkSession \
       .builder \
       .appName("JsonToRdd") \
       .config("spark.executor.heartbeatInterval","60s")\
       .getOrCreate()
spark = SparkSession.builder.master("local").appName("JSON to RDD").config("spark.some.config.option", "some-value").getOrCreate()


j={'abc':1,'def':2}

a=[json.dumps(j)]

jsonRDD=sc.parallelize(a)

df=spark.read.json(jsonRDD)

df.show()
