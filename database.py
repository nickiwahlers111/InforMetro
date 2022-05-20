#! /usr/bin/python3

import json, sys
from re import A
from numpy import insert
import psycopg2
import subprocess
import pandas as pd

#commands to create tables
commands = (
  "drop table if exists BreadCrumb;", 
  "drop table if exists Trip;",
  "drop table if exists Stop;",
  "drop table if exists temp_table;"
  "drop type if exists service_type;",
  "drop type if exists tripdir_type;",
  "create type service_type as enum ('Weekday', 'Saturday', 'Sunday');",
  "create type tripdir_type as enum ('Out', 'Back');",
  "create table Trip (trip_id integer, \
                      vehicle_id integer, \
                      route_id integer, \
                      service_key service_type, \
                      direction tripdir_type);",
  "create table BreadCrumb (latitude float, \
                            longitude float,\
                            direction integer,\
                            speed float,\
                            trip_id integer,\
                            tstamp timestamp);",
  "create table Stop(vehicle_id integer,\
                     route_id integer,\
                     service_key service_type,\
                     direction tripdir_type);",
)
#commands to insert into tables
insert_commands = (
  "INSERT INTO Trip VALUES(%s, %s, %s, %s, %s);",
  "INSERT INTO BreadCrumb VALUES(%s, %s, %s, %s, %s, %s);",
)
merge_command = (
  "UPDATE Trip T SET route_id = S.route_id,\
  service_key = S.service_key,\
  direction = S.direction\
  FROM Stop S WHERE T.vehicle_id = S.vehicle_id;"
)
 
db_name = 'informetro'
conn = None

#open and create databases
#return a connection object so other methods can continue on with this connection
#and work inside the db that was created
def open_and_create():
  try:
    conn = psycopg2.connect(dbname=db_name, user='postgres', password='postgres',host= "localhost")
  except psycopg2.OperationalError as e:
    # subprocess.run(["sudo", "-u", "postgres", "createdb", db_name ]) 
    # Create database
    create_db = "sudo -u postgres createdb {db_name};"
    subprocess.call(create_db, shell=True)
    conn = psycopg2.connect(dbname=db_name, user='postgres', password='postgres',host= "localhost")

  cur = conn.cursor()
  for command in commands: 
    cur.execute(command)
  conn.commit()
  return conn

#open the csv file to write from
#call copy_from method to get info and write into DB
def insert_csv(conn):
 
  cur = conn.cursor()
  # with open('trip.csv', 'r') as f:
  #   next(f)
  #   cur.copy_from(f, 'trip', sep = ',')
  # with open('breadcrumb.csv', 'r') as f:
  #   next(f)
  #   cur.copy_from(f, 'breadcrumb', sep = ',')
  with open('trip_test.csv', 'r') as f:
    next(f)
    cur.copy_from(f, 'trip', sep = ',',null='')
  with open('stop_test.csv', 'r') as f:
    next(f)
    cur.copy_from(f, 'stop', sep = ',', null='')
  conn.commit()

def merge_tables(conn):
  cur = conn.cursor()
  cur.execute(merge_command)
  conn.commit()


#add key constraints to tables
#TODO this isnt called, are we giving up on primary key? :)
def add_keys(conn):
  commands = ("ALTER TABLE trip ADD PRIMARY KEY(trip_id)",\
              "ALTER TABLE breadcrumb ADD FOREIGN KEY(trip_id) REFERENCES trip")
  cur = conn.cursor()
  for c in commands:
    cur.execute(c)

#close the database connection
def close_db(conn):
  if conn is not None:
    # add_keys(conn)
    conn.commit()
    conn.close()