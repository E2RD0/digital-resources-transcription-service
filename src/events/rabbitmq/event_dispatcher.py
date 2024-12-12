from src.events.rabbitmq.publisher import Publisher
from src.events.rabbitmq.subscriber import Subscriber
import os

RABBITMQ_APP_ID = os.environ.get("RABBITMQ_APP_ID", None)
RABBITMQ_CONSUME_QUEUE = os.environ.get("RABBITMQ_CONSUME_QUEUE", None)


class EventDispatcher:
    def __init__(self):
        if(RABBITMQ_APP_ID is None):
            raise ValueError("RABBITMQ_APP_ID must be set")
        self.publisher = Publisher(RABBITMQ_APP_ID)

    def dispatch_event(self, event_type: str, payload: dict):
        self.publisher.publish(event_type, payload)

    def close(self):
        self.publisher.close()
