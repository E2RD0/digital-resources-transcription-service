import functools
import json
import time
import uuid
import pika
import os
from src.events.rabbitmq.rabbitmq import get_rabbitmq_connection

RABBITMQ_APP_ID = os.environ.get("RABBITMQ_APP_ID", None)
RABBITMQ_EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "sagittarius-a")
RABBITMQ_EXCHANGE_TYPE = os.environ.get("RABBITMQ_EXCHANGE_TYPE", "fanout")
class Publisher:
    def __init__(self, routing_key: str = ""):
        self.routing_key = routing_key
        self.app_id = RABBITMQ_APP_ID
        self.exchange = RABBITMQ_EXCHANGE
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        retries = 5
        while retries > 0:
            try:
                print("Attempting to connect to RabbitMQ...")
                self.connection = get_rabbitmq_connection()
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type=RABBITMQ_EXCHANGE_TYPE, durable=True)
                print("Connected to RabbitMQ.")
                return
            except Exception as e:
                retries -= 1
                print(f"Failed to connect to RabbitMQ: {e}. Retrying in 5 seconds...")
                time.sleep(5)

        raise Exception("Unable to connect to RabbitMQ after multiple attempts.")

    def publish(self, event_type: str, message: dict):
        try:
            # Check connection and channel
            if not self.connection or self.connection.is_closed:
                print("RabbitMQ connection lost. Reconnecting...")
                self.connect()
            if not self.channel or self.channel.is_closed:
                print("RabbitMQ channel closed. Recreating channel...")
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type=RABBITMQ_EXCHANGE_TYPE, durable=True)
                
            # Publish the message
            message_id = str(uuid.uuid4())
            payload = {
                "uuid": message_id,
                "fired_at": time.strftime("%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime()),
                "data" : message,
            }

            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.exchange,  # Fanout ignores routing key
                body=json.dumps(payload),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    type=f"{self.app_id}.{event_type}",
                    message_id=message_id,
                    app_id=RABBITMQ_APP_ID,
                    timestamp=int(time.time()),
                    headers= {
                        'content_type': 'application/json',
                        'type': f"{self.app_id}.{event_type}",
                    }
                )
            )
            print(f"Published to exchange '{self.exchange}' with type '{event_type}'")

        except (pika.exceptions.ConnectionClosedByBroker,
                pika.exceptions.StreamLostError,
                pika.exceptions.AMQPConnectionError) as e:
            print(f"Connection lost while publishing: {e}. Reconnecting...")
            self.connect()
            self.publish(event_type, message)  # Retry publishing after reconnecting

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
