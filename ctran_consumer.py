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
#import getpass
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
        #username = getpass.getuser()
        #username = os.getlogin()
        #path = "/home/" + username + "/InforMetro/ctran_data/"
        # this was causing filepath issues when it relied on a username. Should be the current directory.
        path = os.getcwd() + "/ctran_data/"
        exists = os.path.exists(path)
        if not exists:
            os.makedirs(path)
        #filename = "/home/" + username + "/InforMetro/ctran_data/" + date.today().strftime('%m-%d-%Y') + "output.txt"
        filename = os.getcwd() + "/ctran_data/" + date.today().strftime('%m-%d-%Y') + "output.txt"
        print(filename)
        conn = db.open_and_create()
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
                x=1
                #print('error: {}'.format(msg.error()))
            else:
                #If there is a message, open a file and write to it until there are no more messages.
                f = open(filename, "w")
                current_trip_id = None
                previous_trip_id = None

                while msg is not None:
                    # Check for Kafka message
                    record_key = msg.key()
                    record_value = msg.value()
                    data = json.loads(record_value)
                    input = data['count']
                    vd.do_validate(input) 
                    my_list.append(data['count']) 

                    total_count+=1
                    # print(total_count)
                    f.write("Consumed record with key {} and value {}, \
                        and updated total count to {}"
                        .format(record_key, record_value, total_count))
                    #check for more messages before closing.
                    previous_trip_id = current_trip_id
                    msg = consumer.poll(1.0)
                f.close()
                df = pd.DataFrame.from_records(my_list)
                # print(df.head(10))
                
                trip, breadcrumb = tf.transform(df)
                
                trip.to_csv("trip.csv", sep = ',', index=False)
                breadcrumb.to_csv("breadcrumb.csv", sep = ',', index=False)
                
                # print(trip.head(10))
                db.insert_csv(conn)
                #print(total_count)
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
