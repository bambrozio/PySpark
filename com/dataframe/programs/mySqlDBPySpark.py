from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark=SparkSession.builder.appName("DBConnection").master('local').getOrCreate()

#dfMySqlDb=sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark").option("driver","com.mysql.jdbc.Driver").option("dbtable", "city_info").option("user", "root").option("password", "mysql").load()
#print(type(dfMySqlDb))
'''
            ORACLE DB CONNECTION
            
jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(hostname, port, db)
connectionProperties = {
  "user" : 'XXXXXX',
  "password" : 'XXXXXX',
  "driver" : "oracle.jdbc.driver.OracleDriver",
  "oracle.jdbc.timezoneAsRegion" : "false"
}
parallel_df = spark.read.jdbc(url=jdbcUrl,
                          table=table_name, 
                          column='rownum', 
                          lowerBound=1, 
                          upperBound=200000, 
                          numPartitions=20,
                          properties=connectionProperties)'''

tables=['city_info','emp','supermarket_tab']
def spark_calling():

    for tables_name in tables:
        #df=sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark").option("driver","com.mysql.jdbc.Driver").option("dbtable", tables_name).option("user", "root").option("password", "mysql").option("useSSL","false").load()
        df=spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable", tables_name).option("user", "root").option("password", "mysql").option("useSSL","false").load()
        #df.select(df.serial_no).groupBy(df.total).count().alias('total_serial_no').show()
        #df.select(df.serial_no).show()
        '''^^^^^^^WORKING AS EXPECTED^^^^^^^^^^^'''
        '''df_count=df.select(df.serial_no).count()
        print('Count of the DF',df_count)''' # WORKING AS EXPECTED
        #df_groupby=df.select(df.serial_no).agg(min(df.serial_no)).show()
        #print(df_groupby)


        '''df2=df.dtypes
        print(type(df2))
        print(df2[0][1])'''
spark_calling()

