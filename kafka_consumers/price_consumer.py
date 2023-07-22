import pandas as pd
from kafka import KafkaConsumer
from time import sleep
from json import loads
from s3fs import S3FileSystem
import json
import signal
import sys


def signal_handler(sig, frame):
    try:
        consumer.close()
    except Exception as e:
        print("An error has occured while closing the consumer : ", e)
    print("Consumer Closed")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

try:
    consumer = KafkaConsumer("stock_price", bootstrap_servers="16.171.200.168:9092", key_deserializer=lambda x: loads(x.decode('utf-8')),
                             value_deserializer=lambda x: loads(x.decode('utf-8')), group_id="price_consumer_group")
except Exception as e:
    print("An error has occured while creating the consumer : ", e)
    consumer = None

if consumer:
    try:
        s3 = S3FileSystem()
        counter = 0

        while True:
            records = consumer.poll(timeout_ms=1000)

            if records:
                for i, record in enumerate(list(records.values())[0]):
                    with s3.open("s3://kafka-stock-market-project-tanatip/stock_price/price_{}_{}.json".format(counter, i), "w") as file:
                        json.dump(record.value, file)

                counter += 1
    except Exception as e:
        print("An error occurred while inserting data into S3 bucket :", e)
