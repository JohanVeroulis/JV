from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, date_format, to_timestamp
from pyspark.sql.types import DecimalType


# Initialize a Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Read subscribers.csv 
file_path = "/Users/ioannisveroulis/MyProjects/data/subscribers.csv"
subscribers_df = spark.read.csv(file_path, header=False)

# Rename columns of subscribers.csv
subscribers_df = subscribers_df.withColumnRenamed("_c0", "sub_id") \
                                 .withColumnRenamed("_c1", "activation_date") \

#subscribers_df = subscribers_df.na.drop(subset=['activation_date'])

# cleaned_subscribers_df = subscribers_df.withColumn(
#     "row_key",
#     concat(col("subscriber_id"), "_", date_format(col("activation_date"), "yyyyMMdd"))
# ).select("row_key", "subscriber_id", "activation_date")

#cleaned_subscribers_df.show()

# Read transactions.csv 
file_path1 = "/Users/ioannisveroulis/MyProjects/data/cvas_data_transactions.csv"
transactions_df = spark.read.csv(file_path1, header=False)

# Rename columns of transactions.csv
transactions_df = transactions_df.withColumnRenamed("_c0", "timestamp") \
                                 .withColumnRenamed("_c1", "subscriber_id") \
                                 .withColumnRenamed("_c2", "amount") \
                                 .withColumnRenamed("_c3", "channel")


cleaned_transactions_df = transactions_df.withColumn(
    "timestamp",
    date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ssXXX")
).withColumn(
    "amount",
    col("amount").cast(DecimalType(10, 2))
).withColumnRenamed("subscriber_id", "sub_id")

cleaned_transactions_df.show()
# Filter out rows with null timestamps
cleaned_transactions_df = cleaned_transactions_df.filter(cleaned_transactions_df["timestamp"].isNotNull())

#Clean the 'channel' column by removing rows with '***'
cleaned_transactions_df1 = cleaned_transactions_df.filter(cleaned_transactions_df['channel'] != '***')

#cleaned_transactions_df1 = cleaned_transactions_df.na.drop(subset=['activation_date'])

# Show all rows in the DataFrame
cleaned_transactions_df1.show(cleaned_transactions_df1.count(), truncate=False)

cleaned_transactions_df1.show()

# Join with subscribers data
final_df = cleaned_transactions_df1.join(subscribers_df, ["sub_id"], "left")

#cleaned final_df from null values in column activate_date
final_df = final_df.na.drop(subset=['activation_date'])

final_df.show(final_df.count(), truncate=False)

# Persist to Parquet
#final_df.write.parquet("output1.parquet")

