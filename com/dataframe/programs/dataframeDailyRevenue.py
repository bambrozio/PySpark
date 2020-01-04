from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
spark=SparkSession.builder.appName('dataframe practice').master('local').getOrCreate()

orderItems=spark.read.format('CSV').schema('order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, order_item_subtotal float ,order_item_product_price float').load('C:\\Users\\RAKA\\Desktop\\order_item.txt').show()

#print(type(orderItems))

'''Below statement is for showing specific column name using SELECT function '''

#orderItems.select('order_item_id','order_item_order_id').show()

#orderItems.select(orderItems.order_item_id).show()

#orderItems.select(orderItems.order_item_product_id, 'order_item_quantity').show()

orders=spark.read.format('CSV').schema('order_id int , order_date string, order_customer_id int,order_status string').load('C:\\Users\\RAKA\\PycharmProjects\\PySparkPracticeCodes\\TestData\\orders.txt')

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







