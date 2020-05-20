from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row

#warehouse_location points to the default location for managed databases and tables

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


spark.sql("CREATE TABLE IF NOT EXISTS Employee(emp_name string,emp_id Decimal) USING hive")

#df.write.insertInto("Employee",overwrite = True)
