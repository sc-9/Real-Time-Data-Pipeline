# KafkaSparkCassandra
Steps for Execution and Results 
1. Once we have created a AWS EC2 instance of Ubuntu 18.04 and also created openweather api Key, we can then download and install all the required packages and dependencies.
2. Open three terminal windows such as EC2-1 (for Kafka), EC2-2 (for Spark) and EC2-3 (for Cassandra). SSH all three windows to connect to our Machine
Command: ssh -i emr-key-pair.pem ubuntu@ec2-x-xx-xx-xxx.compute-1.amazonaws.com
3. In your EC2-2 window, move to ~/SERVER/SPARK/spark-2.1.0-bin-hadoop2.7 directory and considering you have KafkaSparkStreaming.py in ~/SERVER/SPARK/ directory, execute the following command to make Spark Streaming Running.
Command: bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,com.datastax.spark:spark-cassandra-connector_2.11:2.5.0 ../KafkaSparkStreaming.py
4. Now, In your EC2-1 window, move to ~/SERVER/KAFKA/kafka_2.12-2.4.0 directory and considering you have Kafka_Producer.py and Kafka_Consumer.py in ~/SERVER/KAFKA/ directory, execute the following command to make Kafka Producer running
5. Now, in the EC2-3 window, enter cqlsh to start the Cassandra Query Language Shell. Enter the following command to see the loaded data.
Command: 
USE a2044878;
SELECT * FROM weather;


Result: We observe that the data has been successfully loaded into the database through the pipeline. After this, we can use CQL to analyse the data and derive inferences.
