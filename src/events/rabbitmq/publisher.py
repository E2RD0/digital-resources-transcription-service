import json
from src.events.rabbitmq.rabbitmq import get_rabbitmq_connection

class Publisher:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def publish(self, message: dict):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make the message persistent
                )
            )
            print(f"Published message to {self.queue_name}: {message}")
        except Exception as e:
            print(f"Failed to publish message: {e}")
            raise e

    def close(self):
        self.connection.close()
