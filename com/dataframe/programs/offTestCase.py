from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

from pyspark.sql.types import Row
'''spark=SparkSession.builder.master('local').appName("DF").getOrCreate()

sc=SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

rdd=sc.textFile('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\offTestCase.csv')'''

'''dff=spark.createDataFrame(rdd)'''
'''
df=spark.read.format('csv').option("header","true").option("inferSchema","true").schema('Employee String').load('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\offTestCase.csv')

df.show()'''

#df.filter(df.Employee == 'R').show()

'''df.select('Employee').map(lambda x:(x,1)).take(5)'''

def disply():
    spark=SparkSession.builder.master('local').appName("DF").getOrCreate()

    df=spark.read.format('csv').option("header","true").option("inferSchema","true").schema('Employee String').load('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\offTestCase.csv')
    #df.show()

    #df.filter(df.Employee == 'integer').show()
    df.printSchema()
    for x in df.printSchema():
        print(x)

    '''for x in df.columns:
        
        print(x)
        if type(x) == 'integer':
            print(x)
            print("it is an interger")
            print("{col} is integer type".format(col=x))
        else:
            x.count()
        #print(x.count())'''

disply()





