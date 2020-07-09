import pika, threading


class crawler_worker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.0.130.73'), )
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='task_queue', on_message_callback=on_request)
        channel.start_consuming()





def on_request(ch, method, props, body):

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=str(""))
    ch.basic_ack(delivery_tag=method.delivery_tag)

