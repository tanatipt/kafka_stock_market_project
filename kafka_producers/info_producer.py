import pandas as pd
from kafka import KafkaProducer
from json import dumps
import sys
import signal
from time import sleep


def signal_handler(sig, frame):
    try:
        producer.close()
    except Exception as e:
        print("An error has occured while closing the producer : ", e)
    print("Producer Closed")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

try:
    stock_info = pd.read_csv("../data/stock_info.csv")
    producer = KafkaProducer(bootstrap_servers="16.171.200.168:9092", key_serializer=lambda x: dumps(x).encode('utf-8'),
                             value_serializer=lambda x: dumps(x).encode('utf-8'), acks="all", retries=sys.maxsize, compression_type="lz4", linger_ms=1)
except Exception as e:
    print("An error has occured while creating the producer : ", e)
    producer = None
    df = None

if producer and stock_info is not None:
    for i in range(stock_info.shape[0]):
        try:
            sample = stock_info.iloc[i].to_dict()
            symbol = sample["Symbol"]
            producer.send("stock_info",  key=symbol, value=sample)
            producer.flush()

        except Exception as e:
            print("An error has occured while sending message to Kafka : ", e)

    try:
        producer.close()
    except Exception as e:
        print("An error has occured while closing the producer : ", e)
