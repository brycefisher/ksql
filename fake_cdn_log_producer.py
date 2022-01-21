import json
import math
import time
from datetime import datetime, timezone

from pykafka import KafkaClient

print('Hi from fake producer')

client = KafkaClient(hosts="localhost:29092")
topic = client.topics['cdn-logs']
with topic.get_sync_producer() as producer:
    while True:
        now = math.floor(datetime.now(timezone.utc).timestamp() * 1000)
        record = {
            "asset_id": 1,
            "request_begin": now, # int (unix timestamp in milliseconds)
            "segment_duration": 5, # int (seconds of video content)
            "status_code": 200,
            "environment_id": 1,
        }
        print(record)
        msg = json.dumps(record).encode('utf-8')
        producer.produce(msg)
        time.sleep(0.1)
