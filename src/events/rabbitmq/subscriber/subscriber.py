from src.events.rabbitmq.rabbitmq import get_rabbitmq_connection

class Subscriber:
    def __init__(self, queue_name: str, callback):
        self.queue_name = queue_name
        self.callback = callback
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def consume(self):
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.callback,
            auto_ack=True  # Automatically acknowledge the message
        )
        print(f"Started consuming on queue: {self.queue_name}")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()
