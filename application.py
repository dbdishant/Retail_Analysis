import sys
from lib import dataManipulation, dataReader, utils, logger
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__ == '__main__':

    if(len(sys.argv) < 2):
        print("Please provide the environment as an argument, either LOCAL or TEST or PROD")
        sys.exit(-1)
    
    job_run_env = sys.argv[1]

    print("creating spark session")

    spark = utils.get_spark_session(job_run_env)
    
    logger = Log4j(spark)

    logger.warn("created spark session")
   
    # print("created spark session")

    # read data orders and customer data

    # orders_df = spark.dataReader.read_orders(spark, job_run_env)
    orders_df = dataReader.read_orders(spark, job_run_env)
    # print('orders count:', orders_df.count())

    # customers_df = spark.datareader.read_customers(spark, job_run_env)
    customers_df = dataReader.read_customers(spark, job_run_env)
    # print('customers_df count:', customers_df.count())
    # filter orders with order_status = 'CLOSED'  
    order_filtered_df = dataManipulation.filter_closed_orders(orders_df)

    joined_df = dataManipulation.join_orders_customers(order_filtered_df, customers_df)

    # do a group by and do the aggregation on the joined data

    result_df = dataManipulation.count_orders_state(joined_df)
    
    result_df.show(50)
    
    spark.stop()

    logger.info("This is the end of main")

# To run the application, use the following command:
# python application.py LOCAL