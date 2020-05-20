'''from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext


spark=SparkSession.builder.master('local').appName("DF").getOrCreate()

sc=SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

rdd=sc.textFile('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\offTestCase.csv')'''

'''dff=spark.createDataFrame(rdd)'''

'''rdd.map(lambda x:x).collect()'''

'''import pandas as pd

data= pd.read_csv('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\offTestCase.csv')

for i in data.iterrows():
    print(i)
    print(data.)'''

    #print(type(x))

import string
import pandas as pd
df=pd.read_csv('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\offTestCase.csv',header=None,names=['rakesh'])

print(df)

df=df[df.rakesh.isin(list(string.ascii_letters))]

print("number of non intger are",df.rakesh.count())










