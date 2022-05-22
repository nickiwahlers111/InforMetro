#!/bin/bash
cd "$(dirname "$0")"
PATH=....
source confluent-exercise/bin/activate
python3 ctran_consumer.py -f ../.confluent/librdkafka.config -t sensors
python3 stop_consumer.py -f ../.confluent/librdkafka.config -t stop_data
