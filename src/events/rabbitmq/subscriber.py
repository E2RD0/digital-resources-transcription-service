from src.events.rabbitmq.rabbitmq import get_rabbitmq_connection
import traceback
class Subscriber:
    def __init__(self, queue_name: str, callback):
        self.queue_name = queue_name
        self.callback = callback
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def consume(self):
        def wrapped_callback(ch, method, properties, body):
            try:
                self.callback(ch, method, properties, body)
                ch.basic_ack(delivery_tag=method.delivery_tag) 
            except Exception as e:
                
                print(f"Error processing message: {e}")
                traceback.print_exc()
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=wrapped_callback,
            auto_ack=False
        )
        print(f"Started consuming on queue: {self.queue_name}")
        self.channel.start_consuming()


    def close(self):
        self.connection.close()
