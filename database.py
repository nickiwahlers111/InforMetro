#! /usr/bin/python3

import json, sys
from numpy import insert
import psycopg2
import subprocess
import pandas as pd

def database_stuff(df_trip, df_bread):

  dtrip = pd.DataFrame(data=df_trip)
  dbread= pd.DataFrame(data=df_bread)
  
  #commands to create tables
  commands = (
    "drop table if exists BreadCrumb;", 
    "drop table if exists Trip;",
    "drop type if exists service_type;",
    "drop type if exists tripdir_type;",
    "create type service_type as enum ('Weekday', 'Saturday', 'Sunday');",
    "create type tripdir_type as enum ('Out', 'Back');",
    "create table Trip (trip_id integer, \
                        route_id integer, \
                        vehicle_id integer, \
                        service_key service_type, \
                        direction tripdir_type, \
                        PRIMARY KEY (trip_id));",
    "create table BreadCrumb ( tstamp timestamp, \
                              latitude float, \
                              longitude float,\
                              direction integer,\
                              speed float,\
                              trip_id integer,\
                              FOREIGN KEY (trip_id) REFERENCES Trip);",
  )

  #commands to insert into tables
  insert_commands = (
    "INSERT INTO Trip VALUES(%s, %s, %s, %s, %s);",
    "INSERT INTO BreadCrumb VALUES(%s, %s, %s, %s, %s, %s);",
  )
 
  # for formats for the insertions
  formats = (
    (str(dtrip['trip_id'][0]), str(dtrip['route_id'][0]), str(dtrip['vehicle_id'][0]), str(dtrip['service_key'][0]), str(dtrip['direction'][0]) ),
    (str(dbread['tstamp'][0]), str(dbread['latitude'][0]), str(dbread['longitude'][0]), str(dbread['direction'][0]), str(dbread['speed'][0]), str(dbread['trip_id'][0])) 
  )
  
  db_name = 'consumer_test'
  conn = None

  try:
    try:
      conn = psycopg2.connect(dbname=db_name, user='postgres', password='postgres',host= "localhost")
    except psycopg2.OperationalError as e:
      subprocess.run(["sudo", "-u", "postgres", "createdb", db_name ]) 
      conn = psycopg2.connect(dbname=db_name, user='postgres', password='postgres',host= "localhost")


    cur = conn.cursor()
    for command in commands: 
     cur.execute(command)

    for i in range(2):
      cur.execute(insert_commands[i], formats[i])

    cur.close()
    conn.commit()

  finally:
    if conn is not None:
      conn.close()
