import pika
import threading
import json
from bs4 import BeautifulSoup
import requests
import requests.exceptions
from urllib.parse import urlsplit
import re
import logging
from settings import RESPONSE_QUEUE_IP, TASK_QUEUE_IP, NUMBER_OF_THREADS

logger = logging.getLogger("crawlers")
ch = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class crawler_worker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.response_ch = self.prepare_response_channel()

    def proccess_url(self, url):
        new_urls = set()
        parts = urlsplit(url)
        base_url = "{0.scheme}://{0.netloc}".format(parts)
        path = url[:url.rfind('/') + 1] if '/' in parts.path else url
        try:
            response = requests.get(url)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError):
            # ignore pages with errors
            pass
        new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))

        soup = BeautifulSoup(response.text)
        for anchor in soup.find_all("a"):
            # extract link url from the anchor
            link = anchor.attrs["href"] if "href" in anchor.attrs else ''
            # resolve relative links
        if link.startswith('/'):
            link = base_url + link
        elif not link.startswith('http'):
            link = path + link
        new_urls.add(link)
        return list(new_urls), list(new_emails)

    def on_request(self, ch, method, props, body):
        logger.info(f"message_received {props.correlation_id}")

        msg = json.loads(body)
        url = msg['url']
        urls, mails = self.proccess_url(url)

        logger.info(f"message {props.correlation_id} processed")
        self.response_ch.basic_publish(exchange='',
                                       routing_key='response_queue',
                                       properties=pika.BasicProperties(correlation_id=props.correlation_id,
                                                                       delivery_mode=2),
                                       body=json.dumps({'mails': mails, 'urls': urls}))

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
