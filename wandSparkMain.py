import os
from pyspark.sql import SparkSession
from Reader import *
import sys


def event_filter(eventId,sample):
    eventFilterSql='select member_name,group_name,event_name from meetup_flat where lower(response)="yes" and ' \
                   ' event_id like "%s" group by member_name,group_name,event_name' \
                   ' limit %s' % (eventId, sample)
    return rdr.read_df_data(eventFilterSql)


if __name__ == '__main__':

    try:
        filePath = sys.argv[1]
                
        print ("Files are present in "+filePath)
        
        'Creating a spark object'
        spark = SparkSession.builder.master("local").appName("NikeAssignment").getOrCreate()
        
        'Reader is a separate class to perform read operations. Creating Object here'
        rdr = Reader(filePath, sparkSession)
         
        'Read all soure files'
        calendarDf = rdr.read_csv("calendar.csv")
        salesDf = rdr.read_csv("sales.csv")
        storeDf = rdr.read_csv("store.csv")
        productDf = rdr.read_csv("product.csv")
         
        'get date on which sales happened'
		salCalDf = salesDf.join(calendarDf,trim(salesDf.dateId)==trim(calendarDf.datekey),"left").drop(calendarDf.datekey)
		 
		'get product details of the item sold'
		salPrdDf = salCalDf.join(productDf , trim(salCalDf.productId)==trim(productDf.productid),"left").drop(productDf.productid)
		 
		'get store details of the item sold'
		salPrdStrDf = salPrdDf.join(storeDf , trim(salPrdDf.storeId)==trim(storeDf.storeid),"left").drop(storeDf.storeid)
		   .withColumn('uniqueKey',concat(lit('RY'),salPrdStrDf.datecalendaryear.substr(3,2),lit('_'),salPrdStrDf.channel,lit('_'),salPrdStrDf.division,lit('_'),salPrdStrDf.gender,lit('_'),salPrdStrDf.category))
        
		
		weeklyAgg = salPrdStrDf.groupBy('uniqueKey',concat(lit('RY'),salPrdStrDf.datecalendaryear.substr(3,2)).alias('year'),'channel','division','gender','category','weeknumberofseason').agg(sum('netSales').alias('weeklyNetSales'), sum('salesUnits').alias('weeklySalesUnits'))
		
		
		w1 =  Window.orderBy('datekey')
        window = Window.orderBy("indicator")
        orderwindow = Window.orderBy('WeekNumber')
        final_week=calendarDf.withColumn('indicator',row_number().over(w1)).withColumn("WeekNumber",concat(lit('W'),ntile(53).over(window).cast(StringType()))).select('WeekNumber').distinct().withColumn("row_id",row_number().over(Window.orderBy(monotonically_increasing_id())))

		sparkSession.stop
    except Exception as e:
        print ('Error')
        print (e)
