#!/bin/bash
cd "$(dirname "$0")"
PATH=....
source confluent-exercise/bin/activate
python3 ctran_gather.py -f ../.confluent/librdkafka.config -t nicki > ctran_data/producer_output.txt
