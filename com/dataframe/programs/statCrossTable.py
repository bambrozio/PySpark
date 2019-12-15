from pyspark.shell import sqlContext
from pyspark.sql import *
names=["Alice","Bob","Mike"]
items=["milk","bread","butter","apples","oranges"]

df = sqlContext.createDataFrame([(names[i % 1], items[i % 3]) for i in range(100)], ["name", "item"])

df.stat.crosstab("name","item").show()
