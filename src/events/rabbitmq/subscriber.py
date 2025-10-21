import traceback
import json
import os
from src.events.rabbitmq.rabbitmq import get_rabbitmq_connection

import pika

RABBITMQ_EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "sagittarius-a")
RABBITMQ_EXCHANGE_TYPE = os.environ.get("RABBITMQ_EXCHANGE_TYPE", "fanout")
RABBITMQ_CONSUME_QUEUE = os.environ.get("RABBITMQ_CONSUME_QUEUE", None)

class Subscriber:
    def __init__(self, callback):
        if RABBITMQ_CONSUME_QUEUE is None:
            raise ValueError("RABBITMQ_CONSUME_QUEUE must be set in environment variables")
        self.queue_name = RABBITMQ_CONSUME_QUEUE
        self.exchange = RABBITMQ_EXCHANGE
        self.callback = callback

        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()

        # Declare exchange and bind queue
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=RABBITMQ_EXCHANGE_TYPE, durable=True)
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name)

    def consume(self):
        def wrapped_callback(ch, method, properties, body):
            try:
                # Decode and parse
                message = body.decode("utf-8")
                payload = json.loads(message)
                self.callback(payload, properties)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                print(f"[Error processing message]: {e}")
                traceback.print_exc()
                # Optionally: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=wrapped_callback,
            auto_ack=False
        )

        print(f"[Subscriber Ready] Listening on queue: '{self.queue_name}' (exchange: '{self.exchange}')")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()
        print("[🔌 RabbitMQ connection closed]")
