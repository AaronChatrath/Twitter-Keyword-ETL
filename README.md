# Twitter-Keyword-ETL


Connecting to the Twitter API (using Tweepy) through Kafka, publishing results of the twitter search to the Kafka Server.
Using pyspark to pull data from the Kafka Server create a spark dataframe. Results are passed to MongoDB for storage.

Another .py is created called the Consumer. Here is a small set of code used to just consume the published data and display it.


To run these python scripts we need Kafka-Server, Kafka-Zookeeper and Kafka-Manager running.

To run both Kafka-Server and Kafka-Zookeeper. You need to follow the link below and the instructuctions.

https://kafka.apache.org/quickstart


After downloading the correct apache Kafka version, you will now be able to run Kafka server and zookeeper with two simple commands.
In this instance, kafka_2.13-3.2.0 has been downloaded.

But before this we need to set up some configurations.
Head to:

kafka_2.13-3.2.0 > /config/server.properties






You need to open up two seperate windows of bash.
Commands on bash window 1, would be:

<code>cd kafka_2.13-3.2.0
bin/zookeeper-server-start.sh config/zookeeper.properties<code>

Commands on bash window 2, would be:
  
<code>
cd kafka_2.13-3.2.0
JMX_port=8004 bin/kafka-server-start.sh config/server.properties 
<code>
