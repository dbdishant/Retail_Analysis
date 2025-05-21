import pytest
from lib.dataReader import read_customers, read_orders
from lib.dataManipulation import filter_closed_orders, count_orders_state , filter_orders_generic
from lib.configReader import get_app_config


def test_read_customers_df(spark):
    customers_df = read_customers(spark, "LOCAL")
    assert customers_df.count() == 12435

def test_read_orders_df(spark):
    orders_df = read_orders(spark, "LOCAL")
    assert orders_df.count() == 68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_orders = filter_closed_orders(orders_df)
    assert filtered_orders.count() == 7556

@pytest.mark.skip("work in progress")
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

@pytest.mark.skip("work in progress")
def test_count_order_state(spark, expected_result):
    # here spark, expected_result are fixtures defined in conftest.py
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    # actual_results is a datafrae so we are able to apply .collect() on it
    assert actual_results.collect() == expected_result.collect()

####################### Parametrized generic test case ##############################

@pytest.mark.skip()
def test_check_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_orders = filter_orders_generic(orders_df, "CLOSED")
    assert filtered_orders.count() == 7556

@pytest.mark.skip()
def test_check_pending_payment_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_orders = filter_orders_generic(orders_df, "PENDING_PAYMENT")
    assert filtered_orders.count() == 15030

@pytest.mark.skip()
def test_check_complete_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_orders = filter_orders_generic(orders_df, "COMPLETE")
    assert filtered_orders.count() == 22900

# Instead of writing the above 3 test cases, we can write a generic test case
@pytest.mark.parametrize(
        "status, count",
        [
            ("CLOSED",7556),
            ("PENDING_PAYMENT",15030),
            ("COMPLETE",22900)
        ]
)

@pytest.mark.latest()
def test_check_orders_count(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_orders = filter_orders_generic(orders_df, status)
    assert filtered_orders.count() == count

# To run the test code, give below command
# python -m pytest -v

# If you want to run for specific marker, then give the marker name 
# python -m pytest -m transformation
# OR
# python -m pytest -m latest
