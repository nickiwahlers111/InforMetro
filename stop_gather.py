#! /usr/bin/python3

import json
import requests
from datetime import date
import sys
import os
from confluent_kafka import Producer, KafkaError
import ccloud_lib
import getpass
import get_stop_data as gs
import pandas as pd

def main(args):

    # set up topic??
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        # global delivered_records
        delivered_records=0
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            """
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
            """

    stop_data = gs.get_stop_data()
    
    username = getpass.getuser()
    path = "/home/" + username + "/InforMetro/stop_data/" 
    exists = os.path.exists(path)
    if not exists:
      os.makedirs(path)
    filename = "/home/" + username + "/InforMetro/stop_data/" + date.today().strftime('%m-%d-%Y') + ".json"
    f = open(filename, "w")
    f.write(json.dumps(stop_data, indent=4, sort_keys=True))
    
    count = 0
    for i in stop_data:
        record_key = "nicki"
        record_value = json.dumps(i)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        count += 1
        # from previous produce() calls.
        producer.poll(0)
      
    producer.flush()

    print("Produced {} stop event records.".format(count))


if __name__ == '__main__':
  main(sys.argv)
