from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType

spark=SparkSession.builder.appName("json_read").getOrCreate()

addressSchema = [StructField("city", StringType(), True),
                 StructField("postalCode", StringType(), True),
                 StructField("state", StringType(), True),
                StructField("streetAddress", StringType(), True)]

elementSchema=[StructField("number",StringType(),True),StructField("type",StringType(),True)]

#phoneSchema = [StructField("element", StructType(elementSchema),True)]

schemaFields = [StructField("address",StructType(addressSchema),True),
                StructField("age", StringType(), True),
                StructField("firstName", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("lastName", StructType(), True),
                StructField("phoneNumbers", ArrayType(StructType(elementSchema)), True)]

personSchema = StructType(schemaFields)

df_json=spark.read.format('json').option('header',True).option('Multiline',True).load('/FileStore/tables/example_3.json',personSchema)

df_json.show()
