import pytest
from lib.utils import get_spark_session

@pytest.fixture
def spark():
    "Spark session fixture for testing"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    "gets expected result"
    result_schema = "state string, count int"
    return spark.read \
        .format("csv") \
        .schema(result_schema) \
        .load("data/test_result/state_aggregate.csv")


