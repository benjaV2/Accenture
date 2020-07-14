import pika
import threading
import json
import logging
import requests
from settings import RESPONSE_QUEUE_IP, NUMBER_OF_THREADS


logger = logging.getLogger("response_service")
ch = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class response_worker(threading.Thread):

    def __init__(self, _id):
        threading.Thread.__init__(self)
        self.id = _id

    def callback(self, ch, method, props, body):
        logger.info(f"thread: {self.id} message_received {props.correlation_id}")
        logger.info(f"message {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def prepare_consume_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RESPONSE_QUEUE_IP), )
        channel = connection.channel()
        channel.queue_declare(queue='response_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='response_queue', on_message_callback=self.callback)
        return channel

    def run(self):
        print(f"THREAD {self.id} LISTENING")
        consume_ch = self.prepare_consume_channel()
        consume_ch.start_consuming()


for _ in range(NUMBER_OF_THREADS):
    td = response_worker(_)
    td.start()
