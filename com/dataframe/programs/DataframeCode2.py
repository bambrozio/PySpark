from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

sc=SparkSession.builder.appName('Dataframe code').config("spark.some.config.option", "some-value").getOrCreate()

#'''data_file='C:\\work\\datasets\\matches.csv''''

'''df_data_file=sc.createDataFrame(data_file)'''

'''df_data_file.show()'''

df=sc.read.csv('C:\\work\\datasets\\matches.csv',inferSchema=True,header=True)

df.show()

print("only columns name will display",df.columns)


print("count of the data file",df.count())


print("Below reult will display of only team1 and team2")
df.select('team1','team2').show()


print("<<<<<<below result will display on CSK team as winner>>>>>>>>")
df.filter(df.winner=='Chennai Super Kings').show(5)

print("the below code is for displaying multple column names in display")
df.filter((df.winner=='Chennai Super Kings') & (df.team1=='Delhi Daredevils')).show(5)


print("i am using order by clause")
df.filter((df.team1=='Kings XI Punjab')&(df.winner=='Kings XI Punjab')).orderBy('win_by_runs').show(4)














