import pika
import threading
import json
import logging
import requests
from pymongo import MongoClient, errors
from settings import RESPONSE_QUEUE_IP, NUMBER_OF_THREADS, E_MAIL_DB, URL_SERVICE_IP


logger = logging.getLogger("response_service")
ch = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def insert_mails(mails):
    client = MongoClient(E_MAIL_DB, 27017)
    db = client["URL"]
    table = db.M
    payload = [{'mail': mail} for mail in mails]
    try:
        table.insert_many(payload, ordered=False)
    except errors.BulkWriteError as e:
        for error in e.details['writeErrors']:
            logger.error(error)


class response_worker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def callback(self, ch, method, props, body):
        logger.info(f"{props.correlation_id} message received")
        msg = json.loads(body)
        mails = msg['mails']
        insert_mails(mails)
        urls = msg['urls']
        requests.post(URL_SERVICE_IP, json={'urls': urls})
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def prepare_consume_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RESPONSE_QUEUE_IP), )
        channel = connection.channel()
        channel.queue_declare(queue='response_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='response_queue', on_message_callback=self.callback)
        return channel

    def run(self):
        logger.info("start running")
        consume_ch = self.prepare_consume_channel()
        consume_ch.start_consuming()


for _ in range(NUMBER_OF_THREADS):
    td = response_worker()
    td.start()
