#!/bin/bash -eu

pip install pykafka

python fake_cdn_log_producer.py
