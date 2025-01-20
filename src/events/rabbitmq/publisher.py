import json
import time
import uuid
import pika
import os
from src.events.rabbitmq.rabbitmq import get_rabbitmq_connection

RABBITMQ_APP_ID = os.environ.get("RABBITMQ_APP_ID", None)

class Publisher:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
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
                self.channel.queue_declare(queue=self.queue_name, durable=True)
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
            if self.connection is None or self.connection.is_closed:
                print("RabbitMQ connection lost. Reconnecting...")
                self.connect()

            if self.channel is None or self.channel.is_closed:
                print("RabbitMQ channel closed. Recreating channel...")
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=True)

            # Publish the message
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    type=f"{self.queue_name}.{event_type}",
                    message_id=str(uuid.uuid4()),
                    app_id=RABBITMQ_APP_ID
                )
            )
            print(f"Published message to {self.queue_name}: {message}")
        except (pika.exceptions.ConnectionClosedByBroker,
                pika.exceptions.StreamLostError,
                pika.exceptions.AMQPConnectionError) as e:
            print(f"Connection lost while publishing: {e}. Reconnecting...")
            self.connect()
            self.publish(event_type, message)  # Retry publishing after reconnecting
        except Exception as e:
            print(f"Failed to publish message: {e}")
            raise e

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
            print("RabbitMQ connection closed.")
