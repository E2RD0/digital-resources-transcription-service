from src.events.rabbitmq.publisher import Publisher
from src.events.rabbitmq.subscriber import Subscriber
import os

RABBITMQ_PUBLISH_QUEUE = os.environ.get("RABBITMQ_PUBLISH_QUEUE", None)
RABBITMQ_CONSUME_QUEUE = os.environ.get("RABBITMQ_CONSUME_QUEUE", None)


class EventDispatcher:
    def __init__(self):
        if(RABBITMQ_PUBLISH_QUEUE is None or RABBITMQ_CONSUME_QUEUE is None):
            raise ValueError("RABBITMQ_PUBLISH_QUEUE and RABBITMQ_CONSUME_QUEUE must be set")
        self.publisher = Publisher(RABBITMQ_PUBLISH_QUEUE)

    def dispatch_event(self, event_type: str, payload: dict):
        event = {
            "type": event_type,
            "payload": payload
        }
        self.publisher.publish(event)

    def start_subscriber(self, callback):
        subscriber = Subscriber(RABBITMQ_CONSUME_QUEUE, callback)
        subscriber.consume()

    def close(self):
        self.publisher.close()
