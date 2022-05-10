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
  "drop type if exists service_type;",
  "drop type if exists tripdir_type;",
  "create type service_type as enum ('Weekday', 'Saturday', 'Sunday');",
  "create type tripdir_type as enum ('Out', 'Back');",
  "create table Trip (trip_id integer, \
                      route_id integer, \
                      vehicle_id integer, \
                      service_key service_type, \
                      direction tripdir_type);",
  "create table BreadCrumb (latitude float, \
                            longitude float,\
                            direction integer,\
                            speed float,\
                            trip_id integer,\
                            tstamp timestamp);",
)
#commands to insert into tables
insert_commands = (
  "INSERT INTO Trip VALUES(%s, %s, %s, %s, %s);",
  "INSERT INTO BreadCrumb VALUES(%s, %s, %s, %s, %s, %s);",
)
 
db_name = 'db2test'
conn = None

def open_and_create():
  try:
    conn = psycopg2.connect(dbname=db_name, user='postgres', password='postgres',host= "localhost")
  except psycopg2.OperationalError as e:
    subprocess.run(["sudo", "-u", "postgres", "createdb", db_name ]) 
    conn = psycopg2.connect(dbname=db_name, user='postgres', password='postgres',host= "localhost")

  cur = conn.cursor()
  for command in commands: 
    cur.execute(command)
  conn.commit()
  return conn


def insert_csv(conn):
 
  cur = conn.cursor()
  with open('trip.csv', 'r') as f:
    next(f)
    cur.copy_from(f, 'trip', sep = ',')
  with open('breadcrumb.csv', 'r') as f:
    next(f)
    cur.copy_from(f, 'breadcrumb', sep = ',')
  conn.commit()


def add_keys(conn):
  commands = ("ALTER TABLE trip ADD PRIMARY KEY(trip_id)",\
              "ALTER TABLE breadcrumb ADD FOREIGN KEY(trip_id) REFERENCES trip")
  cur = conn.cursor()
  for c in commands:
    cur.execute(c)


def close_db(conn):
  if conn is not None:
    # add_keys(conn)
    conn.commit()
    conn.close()