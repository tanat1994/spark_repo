import json
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, lit, concat


spark = SparkSession.builder.master("local[*]").appName("SubscriberPipelinev1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

subscriber_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("subscribe_bool", IntegerType()),
    StructField("subscribe_plan", StringType()),
    StructField("country", StringType()),
    StructField("credit_card_type", StringType()),
    StructField("transaction_amount", IntegerType())
])

CHECKPOINT_LOCATION = "./checkpoint_path"
STREAMING_OUTPUT = "./streaming_output/"
RAW_SUBSCRIBE_STREAM_TOPIC = "raw_subscriber_pipeline"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def debug_stream(stream_df):
    stream_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    streaming_input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", RAW_SUBSCRIBE_STREAM_TOPIC) \
        .load()

    streaming_raw_df = streaming_input_df.selectExpr("CAST(value AS STRING)")
    streaming_df = streaming_raw_df.select(from_json(streaming_raw_df.value, subscriber_schema).alias("subscriber_streaming"))

    # .* = Access to child level
    streaming_df = streaming_df.select("subscriber_streaming.*")
    print("====== Printing RAW_STREAM_DATA[streaming_df] schema ======")
    streaming_df.printSchema()

    # Subscriber Streaming DF pipeline [TEST]
    subscriber_pipeline = streaming_df.select([
        "timestamp", "user_id", "subscribe_bool", "subscribe_plan", "country", "credit_card_type", "transaction_amount"
    ])
    print("====== Printing [subscriber_pipeline_df] ======")
    subscriber_pipeline.printSchema()


    subscriber_profit_streaming_df = streaming_df.groupBy("subscribe_bool").agg(func.sum("transaction_amount").alias("total_profit"), func.count("subscribe_bool").alias("no_of_subs"))
    subscriber_profit_streaming_df = subscriber_profit_streaming_df.select(col("subscribe_bool").alias("subscribe_boolean"), col("total_profit"), col("no_of_subs"))
    # Print the result to console
    debug_stream(subscriber_profit_streaming_df)

    # Kafka sink
    # kafka_sink_df = test2.selectExpr("to_json(struct(*)) AS value")\
    #     .writeStream \
    #     .format("kafka") \
    #     .outputMode("update") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    #     .option("topic", "test_output8") \
    #     .option("checkpointLocation", CHECKPOINT_LOCATION) \
    #     .queryName("Kafka Sink - Total Profit Amount") \
    #     .start()


