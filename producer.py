from kafka import KafkaProducer
from pyspark import F
import tweepy
from tweepy import OAuthHandler
from datetime import datetime
import json
import time


def json_serializer(data):
    return json.dumps(data).encode("utf-8")

#initialise the kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=json_serializer)

#function that calls the tweepy configuration that allows connection to twitter API
def getClient():
    client = tweepy.Client(
        bearer_token="YOUR BEARER-TOKEN",
        consumer_key = "YOUR CONSUMER-KEY",
        consumer_secret = "YOUR-CONSUMER-SECRET",
        access_token = "YOUR-ACCESS-TOKEN",
        access_token_secret = "YOUR-SECRET-TOKEN")
    return client


#search tweets based on keyword/s
def searchTweets(query):
    client  = getClient()
    #API call for recent tweets
    tweets = client.search_recent_tweets(query=query, max_results=10)

    tweet_data = tweets.data
    #store list of dictionaries inside results list
    results = []

    #iterate through the tweet data and add the json to an dict with headings
    #append it to the results list.
    if not tweet_data is None and len(tweet_data) > 0:
        for tweet in tweet_data:
            obj = {}
            tweetID = client.get_tweet(tweet.id, expansions='author_id')
            obj = {
                "tweet_id": tweet.id,
                "tweet_text": tweet.text,
                "userID": tweetID.includes['users'][0].id,
                "username": tweetID.includes['users'][0].username
            }
            results.append(obj)
    else:
        return ''

    return results


if __name__ == "__main__":
    #Searching recent tweets on "Ronaldo"
    tweets = searchTweets("Ronaldo")
    #iterate through and send tweets to Kafka server and print it out
    for x in tweets:
        producer.send("TwitterTweets", x)
        print(x)
        time.sleep(1)
    producer.flush()
    producer.close()    
        