import pika
import threading
import json
from bs4 import BeautifulSoup
import requests
import requests.exceptions
from urllib.parse import urlsplit
from collections import deque
import re
import uuid
import time
import logging
from settings import RESPONSE_QUEUE_IP, TASK_QUEUE_IP, NUMBER_OF_THREADS


logger = logging.getLogger("crawlers")
ch = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class crawler_worker(threading.Thread):

    def __init__(self, _id):
        threading.Thread.__init__(self)
        self.id = _id
        self.response_ch = self.prepare_response_channel()

    def on_request(self, thread_id):
        def callback(ch, method, props, body):
            logger.info(f"thread: {thread_id} message_received {props.correlation_id}")
            logger.info(f"message {body}")

            msg = json.loads(body)
            time.sleep(msg.get("time"))

            logger.info(f"thread: {thread_id} message_processed")
            self.response_ch.basic_publish(exchange='',
                                           routing_key='response_queue',
                                           properties=pika.BasicProperties(correlation_id=props.correlation_id,
                                                                           delivery_mode=2),
                                           body=json.dumps(msg))

            ch.basic_ack(delivery_tag=method.delivery_tag)
        return callback

    def prepare_consume_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=TASK_QUEUE_IP), )
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='task_queue', on_message_callback=self.on_request(self.id))
        return channel

    def prepare_response_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RESPONSE_QUEUE_IP), )
        channel = connection.channel()
        channel.queue_declare(queue='response_queue', durable=True)
        return channel

    def run(self):
        print(f"THREAD {self.id} LISTENING")
        consume_ch = self.prepare_consume_channel()
        consume_ch.start_consuming()


for _ in range(NUMBER_OF_THREADS):
    td = crawler_worker(_)
    td.start()
