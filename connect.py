from struct import Struct
import findspark
from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import pymongo
from pymongo import MongoClient
from pyspark.sql.streaming import StreamingQuery

#submit packages added for when .py is run
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

#create sparksession with mongodb configurations for connection.
spark = SparkSession.builder \
        .appName('LearningDataframeWork') \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitterdb.streamoutput") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitterdb.streamoutput") \
        .getOrCreate()

#tweet schema defined
schema = StructType([ 
        StructField("tweet_id", LongType(), True),
        StructField("tweet_text" , StringType(), True),
        StructField("userID" , LongType(), True),
        StructField("username" , StringType(), True),
        ])

#Read from kafka server the saved data
kafkaDf = spark.read.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", 'TwitterTweets')\
  .option("startingOffsets", "earliest")\
  .load()

#select the values from the json stored in kafka and create dataframe
parsed_df = kafkaDf.select(
    from_json(col("value").cast("string"), schema).alias("value")
).select("value.*")

parsed_df.show()

#input the dataframe into mongodb
parsed_df.write.format("mongo").option('uri', 'mongodb://127.0.0.1')\
    .option('database', 'twitterdb') \
    .option('collection', 'streamoutput') \
    .mode("overwrite") \
    .save()