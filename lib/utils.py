from pyspark.sql import SparkSession
from lib.configReader import get_pyspark_config

def get_spark_session(env):
    if env == "LOCAL":
        print("Inside utils/Local")
        return SparkSession.builder \
            .config(conf = get_pyspark_config(env)) \
            .config('spark.driver.extraJavaOptions', 
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .getOrCreate()
    
    else:
        return SparkSession.builder \
            .config(conf = get_pyspark_config(env)) \
            .enableHiveSupport() \
            .getOrCreate()