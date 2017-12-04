# Brief Description
Exercise on 2016 Italian Referendum dataset. The exerciseis really simple and is performed in 3 ways:
1. with Python pandas dataframes
2. Python confluent-kafka client
3. Scala Spark.
# Notebook Data Analysis
Firstly, I have analyzed the data in the ScrutiniFi.csv to find out data inconsistences, wrong formats, data structures. The analysis was made using pandas framework.
The main features discolsed:
* Type inconsistency in the first 3 columns ELETTORI, ELETTORI_M, VOTANTI during the import from csv.
* Several NaN values
* Need to perform votes validation using the following formula: VOTANTI=NUMVOTISI+NUMVOTINO+NUMVOTIBIANCHI+NUMVOTINONVALIDI+NUMVOTICONTESTATI
* In Umbria it was a Fraud
* In analysis of the DESCREGIONE, DESCPROVINCIA, DESCCOMUNE were discovered a new region (SICLIA), and cities without names (they were kept in the analysis).
* Discovered dirty data like: not digits, NaN, digits concat with chars.

Some data could be recovered and used in other branch of analysis, indeed the Python script could produce 2 files of aggregated data

# Python Script
It was the result of the noteebook cleaned by the analysis code lines.
This script produce 2 files:
1. <input_filename>-aggregated.csv: data are purged by dirty data with the above features.
2. withRecovery_<input_filename>-aggregated.csv: the dataset used in the previous file is enriched with dirty data recovered. The recovery function cleans digits from chars, re-compute the number of ELETTORI or VOTANTI, in the small cities, by the trend in the Province.

# Kafka Python Script
The python script include confluent-kafka-python library to instantiate a Producer and a Consumer.
To run the exercise you need to use the property file __server-scrutini.properties__.
# Kafka SetUp:
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh  config/server-scrutini.properties
#add on server.properties "auto.create.topics.enable=true" <---in this way the kafka structure is created dinamically
#modify on server.properties "num.partitions=15" <---the max num of province in each regione (non è ottimizzato, lo so...)
#comment on server.properties "#log.retention.hours=168"
#add on server.properties "log.retention.minutes=1"

# Prerequisites to run:
* Python3.X
* pip3 install librdkafka-dev
* pip3 install confluent-kafka
* spark2.0 with Scala2.11
