import configparser
from pyspark import SparkConf

def get_app_config(env):
    print("Inside configReader")
    config = configparser.ConfigParser()

    read_files = config.read('./configs/application.conf')
    # print(f"Config files read: {read_files}")

    # config.read('./configs/application.conf')
    
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf

def get_pyspark_config(env):
    config = configparser.ConfigParser()

    # config.read('configs/pyspark.conf')

    read_files = config.read('./configs/application.conf')
    # print(f"Config files read: {read_files}")

    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf
