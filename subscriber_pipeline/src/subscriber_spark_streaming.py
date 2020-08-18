from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

SUBSCRIBER_TOPIC = "raw_subscriber_pipeline"

def main():
    sc = SparkContext(appName="SparkAndKafkaSubscriberStremaing")
    ssc = StreamingContext(sc, 5)
    kafka_stream = KafkaUtils.createStream(ssc, SUBSCRIBER_TOPIC)



if __name__ == "__main__":
    main()