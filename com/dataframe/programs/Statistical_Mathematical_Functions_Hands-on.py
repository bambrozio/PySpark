from pyspark.sql import SparkSession

from pyspark.sql.functions import rand
from pyspark.sql.functions import min, mean,max
from pyspark.sql import SQLContext

spark=SparkSession.builder.appName("prac").getOrCreate()

df=spark.range(0,20).withColumn('r1',rand(seed=10)).withColumn('r2',rand(seed=20))

df.show()

df.write.format('csv').save("C:\\Users\\RAKA\\Desktop\\hadoop\\output.txt")








