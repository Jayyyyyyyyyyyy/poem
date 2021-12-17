# !/usr/bin/env python
# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

class Kafka_producer():
    '''
    使用kafka的生产模块
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic):
        if kafkahost.find(",")>0:
            self.kafkaPort = kafkaport
            self.kafkatopic = kafkatopic
            self.producer = KafkaProducer(bootstrap_servers= kafkahost)
            pass
        else:
            self.kafkaHost = kafkahost
            self.kafkaPort = kafkaport
            self.kafkatopic = kafkatopic
            self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                kafka_host=self.kafkaHost,
                kafka_port=self.kafkaPort
            ))

    def sendjsondata(self, params, key):
        try:
            parmas_message = json.dumps(params, ensure_ascii=False)
            producer = self.producer
            producer.send(self.kafkatopic, value=parmas_message.encode('utf-8'), key=key.encode("utf-8"))
            producer.flush()
        except KafkaError as e:
            print(e)

    def senddata(self, data):
        try:
            producer = self.producer
            producer.send(self.kafkatopic, data.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)


if __name__ == '__main__':
    pass
    # producer = Kafka_producer("10.19.17.74:9092,10.19.130.22:9092,10.19.11.29:9092", 9092, "dance_machine")
    # producer.sendjsondata(json_data, id)
