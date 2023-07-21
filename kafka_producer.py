import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps


df = pd.read_csv("data/indexProcessed.csv")
producer = KafkaProducer(bootstrap_servers="16.171.159.106:9092",
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    sample = df.sample(1).to_dict(orient="records")[0]
    producer.send("demo_test", value=sample)
    producer.flush()
    sleep(1)
