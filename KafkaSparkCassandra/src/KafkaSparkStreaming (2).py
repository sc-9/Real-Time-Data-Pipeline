#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Task configuration.
topic = "Weather"
brokerAddresses = "localhost:9092"
batchTime = 20

# Creating stream.
spark = SparkSession.builder.appName("Kakfa-SparkStreaming").getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)
stream = StreamingContext(sc, batchTime)
kafka_stream = KafkaUtils.createDirectStream(stream, [topic], {"metadata.broker.list": brokerAddresses})

# Creating Schema
schema = StructType([StructField("columns", StringType(), True),StructField("data", StringType(), True)])

def main():

    lines = kafka_stream.map(lambda x: x[1])
    lines.pprint()
    lines.foreachRDD(process_batch)

    # Starting the task run.
    stream.start()
    stream.awaitTermination()

def process_batch(rdd):
    if not rdd.isEmpty():
        lines = rdd.map(str)
        df2 = sql.read.json(lines, schema)
        split_col = split(df2['data'], ',')
        df2 = df2.withColumn('city', split_col.getItem(0))
        df2 = df2.withColumn('country', split_col.getItem(1))
        df2 = df2.withColumn('windSpeed', split_col.getItem(2))
        df2 = df2.withColumn('city', regexp_replace(regexp_replace('city', '"', ''),'\\[\\[','')).withColumn('country', regexp_replace('country', '"',""))                .withColumn('windspeed', regexp_replace('windspeed', '\\]\\]',""))
        df2 = df2.drop('columns')
        df2 = df2.drop('data')
        df2.show()

        #Writing to Cassandra Table and Creating a csv file as backup
        df2.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="weather",keyspace="a20444878").save()
        df2.write.format('csv').save('file:///home/ubuntu/output3.csv')


if __name__ == '__main__':
    main()

