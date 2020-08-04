#!/usr/bin/env python
# coding: utf-8

# In[12]:


import webbrowser
import json
import requests
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from urllib.parse import urlencode
import pandas as pd
from pandas.io.json import json_normalize


API_KEY = 'f3e79fb085115fa0b99649a178bf704f'
Locations = [4887398,4164138,5391959]
weatherDF = pd.DataFrame(columns=("Name","Country","WindSpeed"))


#Setting up Kafka
KAFKA_TOPIC = 'Weather'
KAFKA_BROKERS = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,
                         value_serializer=lambda v:json.dumps(v).encode('utf-8'),
                         linger_ms=10)

for i in Locations:
    mydict = {'id': i, 'appid': API_KEY}
    WEATHER_URL = 'http://api.openweathermap.org/data/2.5/weather?'
    url = WEATHER_URL + urlencode(mydict, doseq=True)
    #webbrowser.open(url)
    response1 = requests.get(url)
    info_as_json = json.loads(response1.text)
    #print(info_as_json)
    weatherDF = weatherDF.append({"Name":info_as_json['name'],"Country":info_as_json['sys']['country'],"WindSpeed":info_as_json['wind']['speed']}, ignore_index=True)
    producer.flush()
weather= json.loads(weatherDF.to_json(orient='split',index=False))
producer.send(KAFKA_TOPIC, weather)
producer.flush()
print(weatherDF)


