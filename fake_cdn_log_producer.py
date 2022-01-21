import json
import time

from pykafka import KafkaClient

print('Hi from fake producer')

client = KafkaClient(hosts="localhost:29092")
topic = client.topics['cdn-logs']
with topic.get_sync_producer() as producer:
    while True:
        record = {
            "asset_id": 1,
            "request_begin": 0, # int (unix timestamp in milliseconds)
            "segment_duration": 5, # int (seconds of video content)
            "status_code": 200,
            "environment_id": 1,
        }
        print(record)
        msg = json.dumps(record).encode('utf-8')
        producer.produce(msg)
        time.sleep(0.1)
