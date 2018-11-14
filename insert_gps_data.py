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
## setting group_id to `None` to force it to consume from the latest, i.e.,
## 'no memory' of what has been consumed
consumer = KafkaConsumer('remote', group_id=None)
# avro schema path
schema_path = './schema/gps.avsc'

# load avro schema
schema = avro.schema.parse(open(schema_path).read())

try:
    for message in consumer:
        splited_keys = message.key.split(':')
        if splited_keys[0] != 'gps':
            continue

        isoblue_id = splited_keys[1]

        # Setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        gps_msg = reader.read(decoder)

        print gps_msg

        if gps_msg['object_name'] == 'TPV':
            gps_data = gps_msg['object']
            if gps_data['time'] is None:
                continue
            else:
                t = datetime.utcfromtimestamp(gps_data['time'])

            # Setup mysql query
            sql = 'INSERT INTO `gps` \
                (`isoblue_id`, `ts`, `lat`, `lon`, `alt`, `speed`) \
                VALUES (%s, %s, %s, %s, %s, %s)'

            # Excute the insert
            cursor.execute(sql, \
                (isoblue_id, \
                t.strftime(fmt), \
                gps_data['lat'], \
                gps_data['lon'], \
                gps_data['lat'], \
                gps_data['speed']))

            conn.commit()
except KeyboardInterrupt:
    conn.close()
