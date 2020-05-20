from pyspark.sql import *
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import Row

import pyspark.sql.functions as F

spark=SparkSession.builder.appName("readCSV").getOrCreate()
#sc=SparkContext('local[2]','assignment')

data='C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\input1.csv'

dataDF=spark.read.format('csv').option('header',True).option('inferSchema',True).load(data)

columnsList = dataDF.toDF(*['id','priority','status','datetime','data_As_of_date','amount'])


getColumn=udf(lambda x : x-x ,StringType())
newColumn1=dataDF.withColumn("temp",F.col("amount"))
newColumn2=columnsList.withColumn("AmountDiff" ,F.col("amount"))
newColumn3=columnsList.withColumn("temp2",columnsList.amount - 20).groupBy('id').max('temp2')
newColumn4=columnsList.withColumn("temp3",getColumn('amount'))
newColumn5=columnsList.withColumn("temp4",F.col('amount')-F.col('amount'))
print("type of columnList is:-",type(columnsList))
print("---------------------------------------------------------------------------------")
print(newColumn1.show())
# print(newColumn2.show())
#print(newColumn3.show())
print("---------------------------------------------------------------------------------")
print(newColumn4.show())
print("----------------------------------------column5-----------------------------------------")
print(newColumn5.show())
print("---------------------------------------------------------------------------------")
print(type(columnsList['amount']))
var=dataDF.select('amount')

print("type of VAR is :-",type(var))

print(var.show())
dataDict=var.toPandas().to_string
print(type(dataDict))

print(dataDict)

dataDF.toPandas()

var2=newColumn1.select('amount').collect()
var3=newColumn1.select('temp').collect()
print(var2)
print(var3)

substraction=var2[0]-var3[1]
print("minus of 2 rows",substraction)
#print(type(var2))
#print(var2[10])
















#newColumn.createOrReplaceTempView('tempTable')
# dataDF.createOrReplaceTempView('tempTable')
# newDF = spark.sql('select *, amount as newCol from tempTable')
# newDF.show()
#
# newDF.createOrReplaceTempView('temp2')
# spark.sql('select id,priority from temp2 group by id').show()



