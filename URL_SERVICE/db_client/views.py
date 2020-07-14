from rest_framework.decorators import api_view
from rest_framework.response import Response
from pymongo import MongoClient, errors
from URL_SERVICE.settings import TASK_QUEUE_IP, URL_DB
import pika
import uuid
import json
import logging

logger = logging.getLogger("response_service")
ch = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def insert_urls(urls):
    client = MongoClient(URL_DB, 27017)
    db = client["URL"]
    table = db.U
    payload = [{'url': url} for url in urls]
    try:
        dup = []
        table.insert_many(payload, ordered=False)
    except errors.BulkWriteError as e:
        for error in e.details['writeErrors']:
            if error['code'] == 11000:
                dup.append(error["op"]["name"])
    new_urls = list(set(urls) - set(dup))
    return new_urls


@api_view(('POST',))
def insert(request):
    new_urls = insert_urls(request.data['urls'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=TASK_QUEUE_IP))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    for url in new_urls:
        channel.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=json.dumps({'url': url}),
                properties=pika.BasicProperties(delivery_mode=2, correlation_id=str(uuid.uuid4()))
        )
    connection.close()
    return Response(200)


