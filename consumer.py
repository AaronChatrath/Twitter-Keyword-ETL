from kafka import KafkaConsumer
import json
from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from requests import options

results = []


#Either pass it through the kafkaConsumer or pass the kafka producer content to pyspark
if __name__ == "__main__":

    #initialising the kafka ocnsumer
    consumer = KafkaConsumer(
        "TwitterTweets",
        bootstrap_servers = "localhost:9092",
        auto_offset_reset = 'earliest',
        group_id = "consumer-group-a",
        consumer_timeout_ms=5000)
    print("starting the consumer")
    #load up the json data on recent tweet searches stored in kafka server
    for msg in consumer:
        dictJson = json.loads(msg.value)
        print(dictJson)
        results.append(dictJson)
    consumer.close()
    print()
    for index in range(len(results)):
        for key in results[index]:
            print(results[index][key])