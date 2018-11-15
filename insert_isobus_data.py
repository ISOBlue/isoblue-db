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

with open('/home/yang/isoblue-db/passwd') as f:
  _db_passwd = f.readline().strip('\n')

# connect to the database
conn = pymysql.connect(host='localhost', \
                       user='isoblue', \
                       password=_db_passwd,
                       db='isoblueData')
cursor = conn.cursor()

# setup kafka consumer
consumer = KafkaConsumer('remote', group_id=None)

# avro schema path
schema_path = '/home/yang/isoblue-db/schema/raw_can.avsc'

# load avro schema
schema = avro.schema.parse(open(schema_path).read())

try:
    for message in consumer:
        splited_keys = message.key.split(':')
        if splited_keys[0] != 'tra' and splited_keys[0] != 'imp':
            continue

        # Get different fields
        bus_type, pgn, isoblue_id = splited_keys

        # Setup avro decoder
        bytes_reader = io.BytesIO(message.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        isobus_msg = reader.read(decoder)

        t = datetime.utcfromtimestamp(isobus_msg['timestamp'])

        payload = struct.unpack('BBBBBBBB', isobus_msg['payload'])
        data_list = list(payload)

        # iterate through data_list and pad 0 if the length is not 2
        for i in range(len(data_list)):
            # convert each number to hex string
            data_list[i] = hex(data_list[i])[2:]
            # pad zero if the hex number length is 1
            if len(data_list[i]) == 1:
                data_list[i] = data_list[i].rjust(2, '0')

        payload = ''.join(data_list)

        print t, bus_type, pgn, isoblue_id, payload

        # setup mysql query
        sql = 'INSERT INTO `isobus` \
            (`isoblue_id`, `ts`, `bus_type`, `pgn`, `payload`) \
            VALUES (%s, %s, %s, %s, %s)'

        # excute the insert
        cursor.execute(sql, \
            (isoblue_id, \
            t.strftime(fmt), \
            bus_type, \
            pgn, \
            payload))

        conn.commit()
except KeyboardInterrupt:
    conn.close()
