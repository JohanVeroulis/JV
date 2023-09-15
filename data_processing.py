from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("data_processing1") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file cvas_data_transactions.csv from the HDFS
df = spark.read.json('hdfs://namenode:9000/data/cvas_data_transactions.json')

# Read the file cvas_data_transactions.csv from the HDFS
#df1 = spark.read.csv('hdfs://namenode:9000/data/subscribers.csv')

# Rename columns df
# df = df.withColumnRenamed("_c0", "day_id") \
#                         .withColumnRenamed("_c1", "sub_id") \
#                         .withColumnRenamed("_c2", "amount") \
#                         .withColumnRenamed("_c3", "channel")

# Rename columns df1
#df1 = df1.withColumnRenamed("_c0", "subscriber_id") \
                            #.withColumnRenamed("_c1", "activation date") \
                            


# Export the dataframe into the Hive table forex_rates
df.write.mode("append").insertInto("data_rates")

# Export the dataframe into the Hive table forex_rates
#df1.write.mode("append").insertInto("data_rates")
                                
# df.show()
# df1.show()
