#!/bin/bash -eu

pip3 install pykafka

python3 fake_cdn_log_producer.py
