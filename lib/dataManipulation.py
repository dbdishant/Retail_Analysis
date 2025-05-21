from pyspark.sql.functions import *

# Function to filter orders with status 'CLOSED'
def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")

# Function to join orders DataFrame with customers DataFrame on 'customer_id'
def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, orders_df.order_customer_id == customers_df.customer_id, 'full_outer') \

# Function to group the joined DataFrame by 'state' and count the occurrences
def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

# This function takes generic filters
def  filter_orders_generic(orders_df, status):
    return orders_df.filter("order_status = '{}'".format(status))