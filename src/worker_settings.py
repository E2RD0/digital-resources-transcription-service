import os
REDIS_URL = os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"
QUEUES = ["default"]