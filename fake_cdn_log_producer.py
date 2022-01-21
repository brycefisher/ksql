import json
import math
import random
import time
from datetime import datetime, timezone

from pykafka import KafkaClient

print('Hi from fake producer')

client = KafkaClient(hosts="localhost:29092")
topic = client.topics['cdn-logs-json']
with topic.get_sync_producer() as producer:
    asset_ids = [random.randint(1000,9999), random.randint(1000, 9999), random.randint(1000, 9999)]
    while True:
        now = math.floor(datetime.now(timezone.utc).timestamp() * 1000)
        duration = random.randint(1,5)
        # asset_id = random.randint(1,100)
        asset_id = random.choices(asset_ids, [100,1, 1])[0]
        environment_id = random.randint(1,4)
        status_code = random.choices([200, 206, 304, 404, 416],
                                     [ 50,  15,  25,   5,   5])

        record = {
            "asset_id": asset_id,
            "request_begin": now, # int (unix timestamp in milliseconds)
            "segment_duration": duration, # int (seconds of video content)
            "status_code": status_code[0],
            "environment_id": environment_id,
        }
        print(record)
        msg = json.dumps(record).encode('utf-8')
        producer.produce(msg)
        time.sleep(0.01)
