'''Read the table from postgres and produce them into kafka topic'''

''' console based consumer
/opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --topic customer-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
'''

import json
from confluent_kafka import Producer
conf={'bootstrap.servers':'localhost:9092'}

cust_producer=Producer(conf)

topic = 'customer-topic'
cust_transaction='''{
  "transaction_id": "TXN12345",
  "customer_id": "CUST001",
  "amount": 2500.75,
  "flag_reason": "Amount exceeds 80% of credit limit",
  "timestamp": "2025-06-14T10:15:00",
  "full_name": "John Doe"
}'''
# convert cust_transaction to JSON before publish to topic
transaction=json.dumps(cust_transaction)

''' publis a string to topic'''
# cust_producer.produce(topic,value=cust_transaction)

'''Publish JSON to topic'''
# cust_producer.produce(topic,value=transaction)

'''This will return 1-10 numbers '''

for num in range(50,57):
    msg=f'Current Number is {num}'
    cust_producer.produce(topic,value=msg)

    

cust_producer.flush()