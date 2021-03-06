#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from urllib.parse import non_hierarchical
from confluent_kafka import Consumer
import json
import ccloud_lib
import os
from datetime import date
import validator as vd
import utility as util
import database as db
import transformer as tf
import pandas as pd



if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    conn = None
    my_list = list()
    try:
        path = os.getcwd() + "/stop_data/"
        exists = os.path.exists(path)
        if not exists:
            os.makedirs(path)
        
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(ddmsg.error()))
            else:
                #If there is a message, open a file and write to it until there are no more messages.
                #f = open(filename, "w")
                while msg is not None:
                    # Check for Kafka message
                    record_key = msg.key()
                    record_value = msg.value()
                    data = json.loads(record_value)
                    if vd.validate_stop_event(data):
                        my_list.append(data) 

                    total_count+=1
                   
                    #check for more messages before closing.
                    msg = consumer.poll(1.0)

                print("Consumed {} stop event records.".format(total_count))
                df = pd.DataFrame.from_records(my_list)
                stop_data = tf.transform_stop_event(df) 
                stop_data.to_csv("stop_data.csv", sep = ',', index=False)

                conn = db.open_and_connect()
                db.insert_stop_event(conn)
                db.merge_tables(conn)

               
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets

        ##write to CSV
        #call insert_db(csv file)
        consumer.close()
       
        db.close_db(conn)
        # f.write(total_count)
        # print(total_count)
