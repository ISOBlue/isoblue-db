#!/usr/bin/env python

import io
import sys
import re
import struct
import pprint
import sys
import time

import pymysql.cursors

import avro.schema
import avro.io

from kafka import KafkaConsumer

from datetime import datetime
from struct import *

# specify timestamp format
fmt = "%Y-%m-%d %H:%M:%S"

with open('passwd') as f:
  _db_passwd = f.readline().strip('\n')

# connect to the database
conn = pymysql.connect(host='localhost', \
                       user='isoblue', \
                       password=_db_passwd,
                       db='isoblueData')
cursor = conn.cursor()

# setup kafka consumer
consumer = KafkaConsumer('debug', auto_offset_reset='latest', \
                         group_id='hb-db')
# avro schema path
schema_path = './schema/d_hb.avsc'

# load avro schema
schema = avro.schema.parse(open(schema_path).read())

try:
  for message in consumer:
      splited_keys = message.key.split(':')
      if splited_keys[0] != 'hb':
          continue
      isoblue_id = splited_keys[1]

      # setup avro decoder
      bytes_reader = io.BytesIO(message.value)
      decoder = avro.io.BinaryDecoder(bytes_reader)
      reader = avro.io.DatumReader(schema)
      hb_msg = reader.read(decoder)

      print hb_msg

      t = datetime.utcfromtimestamp(hb_msg['timestamp'])

      # setup mysql query
      sql = 'INSERT INTO `hb` \
          (`isoblue_id`, `ts`, `wifins`, `cellns`, `netled`, `statled`) \
          VALUES (%s, %s, %s, %s, %s, %s)'

      # excute the insert
      cursor.execute(sql, \
          (isoblue_id, \
          t.strftime(fmt), \
          hb_msg['wifins'], \
          hb_msg['cellns'], \
          hb_msg['netled'], \
          hb_msg['statled']))

      conn.commit()
except KeyboardInterrupt:
  conn.close()
