#! /usr/bin/python3

import json
import requests
from datetime import date
import sys
import os
from confluent_kafka import Producer, KafkaError
import ccloud_lib
import getpass

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


    username = getpass.getuser()
    response = requests.get("http://www.psudataeng.com:8000/getBreadCrumbData")
    path = "/home/" + username + "/InforMetro/ctran_data/" 
    exists = os.path.exists(path)
    if not exists:
      os.makedirs(path)
    filename = "/home/" + username + "/InforMetro/ctran_data/" + date.today().strftime('%m-%d-%Y') + ".json"
    f = open(filename, "w")
    f.write(response.text)

    count = 0;
    with open(filename) as f:
      data=json.load(f)
      for i in data:
        record_key = "nicki"
        record_value = json.dumps({'count' :i})
        #print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # from previous produce() calls.
        producer.poll(0)
        count += 1
      
      producer.flush()
    print("Produced {} breadcrumb records.".format(count))


if __name__ == '__main__':
  main(sys.argv)
