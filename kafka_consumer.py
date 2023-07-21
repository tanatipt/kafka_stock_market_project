import pandas as pd
from kafka import KafkaConsumer
from time import sleep
from json import loads
from s3fs import S3FileSystem
import json

consumer = KafkaConsumer("demo_test", bootstrap_servers="16.171.159.106:9092",
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

s3 = S3FileSystem()
for i, message in enumerate(consumer):
    with s3.open("s3://kafka-stock-market-project-tanatip/stock_market_{}.json".format(i), "w") as file:
        json.dump(message.value, file)
