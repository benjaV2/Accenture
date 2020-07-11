import pika
import threading
from bs4 import BeautifulSoup
import requests
import requests.exceptions
from urllib.parse import urlsplit
from collections import deque
import re
import sys
sys.path.append('..')
from settings import RESPONSE_QUEUE_IP, TASK_QUEUE_IP, NUMBER_OF_THREADS


class crawler_worker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.response_ch = self.prepare_response_channel()

    def on_request(self, ch, method, props, body):
        self.response_ch.basic_publish(exchange='',
                                       routing_key='response_queue',
                                       properties=pika.BasicProperties(correlation_id=props.correlation_id,
                                                                       delivery_mode=2),
                                       body=str(""))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def prepare_consume_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=TASK_QUEUE_IP), )
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='task_queue', on_message_callback=self.on_request)
        return channel

    def prepare_response_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RESPONSE_QUEUE_IP), )
        channel = connection.channel()
        channel.queue_declare(queue='response_queue', durable=True)
        return channel

    def run(self):
        consume_ch = self.prepare_consume_channel()
        consume_ch.start_consuming()


for _ in range(NUMBER_OF_THREADS):
    td = crawler_worker()
    td.start()

