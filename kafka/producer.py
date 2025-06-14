# producer.py
from confluent_kafka import Producer
import time

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

topic = 'topic1'

'''
for i in range(10):
    message = f"Avenger-{i}"
    producer.produce(topic, key=str(i), value=message)
    print(f"Sent: {message}")
    time.sleep(1)
'''

for i in range(10):
    message=f'My-Message -->{i}'
    producer.produce('topic1',value=message)
    print(f'sent data is {message}')
    time.sleep(1)
    producer.flush()