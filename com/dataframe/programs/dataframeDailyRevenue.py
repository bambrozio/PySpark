from pyspark.sql import SparkSession
from pyspark.sql.functions import substring

from pyspark.sql.functions import min
from pyspark.sql.functions import max
from pyspark.sql.functions import date_format
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import sum
from pyspark.sql.functions import count
from pyspark.sql.functions import round

spark=SparkSession.builder.appName('dataframe practice').master('local').getOrCreate()

orderItems=spark.read.format('CSV').schema('order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, order_item_subtotal float ,order_item_product_price float').load('C:\\Users\\RAKA\\Desktop\\order_item.txt')

#print(type(orderItems))

'''Below statement is for showing specific column name using SELECT function '''

#orderItems.select('order_item_id','order_item_order_id').show()

#orderItems.select(orderItems.order_item_id).show()

#orderItems.select(orderItems.order_item_product_id, 'order_item_quantity').show()

orders=spark.read.format('CSV').option('inferSchema','true').schema('order_id int , order_date string, order_customer_id int,order_status string').load('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\orders.txt')

#orders.select('order_date').show(truncate= False)

#orders.select(substring('order_date',1,7)).show()

#orders.select(substring('order_date',1,7).alias('order_year')).show()

#orders.selectExpr('substring(order_date,1,7) as order_year').show()

#orders.selectExpr('order_id','substring(order_date,1,7) as order_year').show()

#orders.filter(orders.order_status=='COMPLETE').show()

#orders.filter("order_status=='COMPLETE'").show()

#orders.filter((orders.order_status == 'COMPLETE')|(orders.order_status == 'CLOSED')).show()

#orders.filter("order_status == 'COMPLETE' or order_status == 'CLOSED'").show()

# WRONG-----orders.filter(orders.order_status == 'COMPLETED'| substring('order_date',1,7).alias('order_year')== '2013-07').show()

#orders.filter((orders.order_status == 'COMPLETE')| (substring('order_date',1,7).alias('order_year')== '2013-07') ).show()

#orders.filter(substring('order_date',1,7).alias("order_yr")=='2013-07').show(truncate=False)

#orders.filter(substring('order_date',1,7).alias("order_yr")=='2013-07').count()

#orders.select(substring('order_date',1,7).alias("order_yr")).count()

#orders.filter(orders.order_status.isin())

#isin is a function where we can pass multiple values
#orders.filter ((orders.order_status.isin('COMPLETE','CLOSED') )&(substring('order_date',1,7).alias('order_year')=='2013-08')).show()

#WRONG---------orders.filter((orders.order_status =='COMPLETE'|orders.order_status == 'CLOSED')&(substring('ORDER_DATE',1,7).alias('ORDER_YEAR')=='2013-08'))

#WRONG----orders.filter(orders.order_status == 'COMPLETE' |orders.order_status == 'CLOSED').show()
#CORRECT--orders.filter((orders.order_status == 'COMPLETE') |(orders.order_status == 'CLOSED')).show() ---->>Each condition should be in close bracket

#orders.filter(((orders.order_status == 'COMPLETE') |(orders.order_status == 'CLOSED')) &(substring('order_status',1,7).alias('order_year')=='2013-08')).show()

#orders.filter("order_status == 'COMPLETE' or order_status == 'CLOSED'").show()


#orders.filter((orders.order_status.isin('COMPLETE', 'CLOSED'))&(orders.order_date.like('2013-08%'))).show()

#syntax with python and function with typical dataframe approach
#orders.filter((orders.order_status.isin('COMPLETE','CLOSED')).__and__(orders.order_date.like('2013-09%'))).show()

#syntax with SQL style dataframe
#orders.filter('''order_status in  ('COMPLETE','CLOSED') and order_date like '2013-09%' ''').show()


#orderItems.show()

#orderItems.select('order_item_id','order_item_order_id').show()

#orderItems.select('order_item_subtotal','order_item_quantity','order_item_product_price').filter(orderItems.order_item_subtotal == orderItems.order_item_quantity * orderItems.order_item_product_price ).show()


#orderItems.select('order_item_subtotal','order_item_quantity','order_item_product_price').filter(orderItems.order_item_subtotal != round(orderItems.order_item_quantity * orderItems.order_item_product_price)).show()


'''import MAX and MIN module to use the respcetive functions'''
#orders.select(max(orders.order_date).alias('maximum_order_date')).show()

'''Import date_format module to fetch info related to date, month and year from functions package of pyspark.sql'''
#orders.select(date_format(orders.order_date,'yyyy')).show()

#orders.select(date_format(orders.order_date,'MM')).show()
#orders.filter(date_format(orders.order_date, 'MM') == '01').show()

#print(orders.filter((date_format(orders.order_date,'dd') == '01')).select('order_date').distinct().count())

#orders.select('countDistinct(order_date)').show()

#orders.select(countDistinct('order_date').alias('distinct_order_date_count')).show()

#orders.show()

#orders.groupBy('order_status').agg(count('order_Status').alias('count_status')).show()

'''JOIN'''

#ordersJoin=orders.join(orderItems, orders.order_id == orderItems.order_item_order_id)

#ordersJoin.groupBy('order_date','order_item_product_id').agg(round(sum('order_item_subtotal'),2).alias('prduct_revenue')).show()


'''OFF PRACTICE PURPOSE'''

#orderItems.select('order_item_id').show(5)
#orderItems.show(2)
#orderItems.filter(orderItems.order_item_id == 1).show()
'''for x in orderItems.select('order_date'):
    print(x)'''

#orderItems.select(orderItems.oder_date.isin())

'''for x in orderItems.collect():
    print(x)'''

'''for x in orderItems.select(orderItems.order_item_id).show(5):
    print("rakesh")
'''

mvv_list = orderItems.select('order_item_id').collect()

print(type(mvv_list))

for x in mvv_list:
    print(x[0])
    c=0
    if x[0] == None:
        print("not interger")
        c=c+1
        print("COUNT OF NON INTERGER is",c)
    else:
        print("INTERGER")

print(orderItems.dtypes)

dataTypeOfDF=orderItems.dtypes

print(type(dataTypeOfDF))

for x in dataTypeOfDF:
    print(x[1])


'''for x in mvv_list:
     #print(type(x))
     print(x)
     print("ROW is a tuple",type(x[0]))
     print(x[0])
     '''
#print(mvv_list[3])

 #mvv_array = [int(row.order_item_id) for row in mvv_list.collect()]
'''
i=orderItems.rdd.toLocalIterator()

print(i)'''
