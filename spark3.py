from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col
import altair as alt


# Initialize a Spark session
spark = SparkSession.builder.appName("TransactionDataProcessing").getOrCreate()

# Read the CSV file into a DataFrame
file_path = "/Users/ioannisveroulis/MyProjects/data/cvas_data_transactions.csv"
transactions_df = spark.read.csv(file_path, header=False, inferSchema=True)

# Rename columns
transactions_df = transactions_df.withColumnRenamed("_c0", "timestamp") \
                                 .withColumnRenamed("_c1", "amount") \
                                 .withColumnRenamed("_c2", "subscriber_id") \
                                 .withColumnRenamed("_c3", "channel")


# Read the CSV file into a DataFrame
file_path1 = "/Users/ioannisveroulis/MyProjects/data/subscribers.csv"
subscribers_df = spark.read.csv(file_path1, header=False, inferSchema=True)

# Rename columns
subscribers_df = subscribers_df.withColumnRenamed("_c0", "subscriber_id") \
                                 .withColumnRenamed("_c1", "activation date") \

# Clean the 'subscriber_id' column by converting it to integers
#transactions_df = transactions_df.withColumn("subscriber_id", col("subscriber_id").cast("int"))

# Clean the 'channel' column by removing rows with '***'
transactions_df = transactions_df.filter(transactions_df['channel'] != '***')


transactions_df.show()
subscribers_df.show()


