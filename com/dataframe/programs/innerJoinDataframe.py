from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('innnerJoin').getOrCreate()
sc=spark.sparkContext

tab1=[
    {
        'employee':'abc',
        'val11': 1.1,
        'val12' : 2.3
    },
    {
        'employee':'def',
        'val11':5.5,
        'val12': 5.6
    }
]
tab1Rdd=sc.parallelize(tab1)
tab1DataFrame=spark.createDataFrame(tab1)
print(type(tab1DataFrame))

tab1DataFrame.show()
tab2=[
    {
       'employee':'abc',
        'val21':8.9,
        'val22':7.1
    },
    {
        'employee':'xyz',
        'val21': 2.56,
        'val22':1.0
    }
]
'''try with 3 variable is LIST'''

tab2Rdd=sc.parallelize(tab2)

tab2Dataframe=spark.createDataFrame(tab2Rdd)

tab2Dataframe.show()

'''below code is for JOIN both table'''
innerJoinDf=tab1DataFrame.join(tab2Dataframe,on=['employee'],how='outer')
innerJoinDf.show()
'''
tempTab=innerJoinDf.createGlobalTempView('tempEmpTab')
print(type(tempTab))

tempTabDF=spark.read.schema('employee, val11,val12,val21,val22').load(tempTab).select('employee').show()

print(type(tempTabDF))'''

'''https://stackoverflow.com/questions/36019847/pyspark-forward-fill-with-last-observation-for-a-dataframe'''
innerJoinDf.na.fill(198.0).show()






