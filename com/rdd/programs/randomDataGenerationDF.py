from pyspark.shell import sqlContext
from pyspark.sql.functions import rand, randn
from pyspark.sql import *
from pyspark.sql.functions import mean, min, max


df=sqlContext.range(0,7)


df.show()

df.select("id", rand(seed=10).alias("uniform"), randn(seed=27).alias("normal")).show()

df.describe("uniform", "normal").show()

dfNew=df.describe("uniform", "normal").show()

dfNew.select([mean("uniform"),min("uniform"),max("uniform")]).show()

