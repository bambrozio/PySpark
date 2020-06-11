from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql import Window
#w = Window.orderBy(monotonically_increasing_id())

spark= SparkSession.builder.appName("practice").getOrCreate()
sqlContext = SQLContext(spark)
df = sqlContext.read.format('csv').option('inferSchema',True).option('header',True).load('C:\\work\\datasets\\test_stackOverflow_data.csv')

# df.withColumn("tmp_rn",row_number().over(Window.orderBy(monotonically_increasing_id()))-1).\
# withColumn("tmp",add_months(to_date(col("Date").cast("string"),"yyyyMM"),-1)).\
# withColumn("Decrement",expr("date_format(add_months(tmp,-(tmp_rn)),'yyyyMM')")).\
# withColumn("tmp2",to_date(lit("201908").cast("string"),"yyyyMM")).\
# withColumn("Increment",expr("date_format(add_months(tmp2,-(tmp_rn)),'yyyyMM')")).\
# drop(*["tmp_rn","tmp","tmp2"]).\
# show()


df.withColumn("tmp2",to_date(lit("201908").cast("string"),"yyyyMM")).show()


'''lit take the argument as column, so we can apply on various function to use as a temporary column '''
#df.withColumn("rak",lit("rakesh_sahoo")).show()
