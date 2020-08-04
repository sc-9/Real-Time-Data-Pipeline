#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from kafka import KafkaConsumer

KAFKA_TOPIC = 'Weather'
KAFKA_BROKERS = 'localhost:9092'

consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS,auto_offset_reset='latest')

consumer.subscribe([KAFKA_TOPIC])
try:
    for message in consumer:
        print(message.value)
except KeyboardInterrupt:
    sys.exit()

