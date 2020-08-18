from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

PROCESSED_FOLDER = "processed_data/"

subscriber_schema = StructType([
    StructField("user_id", StringType()),
    StructField("subscribe_bool", IntegerType()),
    StructField("subscribe_plan", StringType()),
    StructField("country", StringType()),
    StructField("credit_card_type", StringType()),
    StructField("transaction_amount", IntegerType()),
    StructField("timestamp", StringType()),
])


def write_to_csv(processed_df, filename):
    processed_df.coalesce(1).write\
        .option("header", True)\
        .mode("overwrite")\
        .csv(PROCESSED_FOLDER + filename)


def subscriber_per_country(subscriber_df):
    subscriber_per_country_df = subscriber_df.filter(col("subscribe_bool") == 1)
    subscriber_per_country_df = subscriber_per_country_df.groupBy("country").count()
    subscriber_per_country_df = subscriber_per_country_df.select(col("country"), col("count").alias("number_of_subscriber")) \
        .orderBy(col("number_of_subscriber").desc())
    write_to_csv(subscriber_per_country_df, "subscriber_per_country")


def subscriber_and_unsubscriber_count(subscriber_df):
    sub_unsub_count_df = subscriber_df.groupBy("subscribe_bool").count()
    sub_unsub_count_df = sub_unsub_count_df.withColumn("subscribe_bool_string",
                                                       when(sub_unsub_count_df.subscribe_bool == 1, "subscribe")
                                                       .otherwise("unsubscribe"))
    sub_unsub_count_df = sub_unsub_count_df.select(col("subscribe_bool_string"), col("count"))
    write_to_csv(sub_unsub_count_df, "subscriber_unsubscriber_count")


def payment_transaction(subscriber_df):
    payment_df = subscriber_df.groupBy("credit_card_type").agg(sum("transaction_amount").alias("total_transaction"),
                                                               count("credit_card_type").alias("number_of_transaction"))
    write_to_csv(payment_df, "payment_transaction")


def subscriber_plan_count(subscriber_df):
    subscriber_plan_count_df_main = subscriber_df.filter(col("subscribe_bool") == 1)
    subscriber_plan_count_df = subscriber_plan_count_df_main.groupBy("subscribe_plan").count()
    subscriber_plan_count_df = subscriber_plan_count_df.select(col("subscribe_plan"), col("count").alias("number_of_subscriber"))\
        .orderBy(col("number_of_subscriber").desc())
    write_to_csv(subscriber_plan_count_df, "subscriber_plan_count")


def main():
    spark = SparkSession.builder.appName("Subscriber_analysis").getOrCreate()
    subscriber_df = spark.read.csv("data_src/subscriber_data.csv",
                                   schema=subscriber_schema, header=True, inferSchema=True)
    subscriber_df.printSchema()

    # Process function
    subscriber_per_country(subscriber_df)
    subscriber_and_unsubscriber_count(subscriber_df)
    subscriber_plan_count(subscriber_df)
    payment_transaction(subscriber_df)

    spark.stop()


if __name__ == "__main__":
    main()
