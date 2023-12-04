import pika
import redis
import json
import time

r = redis.Redis(host='redis-cours', port=6379, db=0)

def safe_connect_rabbitmq():
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    return channel

def callback(ch, method, properties, body):
    data_string = body.decode("utf-8")
    data = json.loads(data_string)

    r = redis.Redis(host='redis-cours', port=6379, db=0)
    if not r.exists(int(data['@Id'])):
        id = data['@Id']
        del data['@Id']
        r.set(id, json.dumps(data))

    print("post: ", json.dumps(data))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    print("Starting redis_sync.py")

    channel = safe_connect_rabbitmq()

    channel.queue_declare(queue='posts_to_redis')
    channel.basic_consume(
        queue='posts_to_redis',
        on_message_callback=callback
    )
    channel.start_consuming()
    print("Done")

if __name__ == "__main__":
    main()
