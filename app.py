#!/usr/bin/env python

from flask import Flask, request, render_template, make_response

import pymysql
import csv
import StringIO

with open('passwd') as f:
  _db_passwd = f.readline().strip('\n')

# connect to the database
conn = pymysql.connect(host='localhost', \
                       user='isoblue', \
                       password=_db_passwd,
                       db='isoblueData')

app = Flask('isoblue-db', static_url_path='/static')

@app.route('/')
def root():
    return app.send_static_file('index.html')

@app.route('/getLastHour/<string:dataType>')
def getHbLastHour(dataType):
    '''
    Get heartbeat message within the last hour.
    Output as an csv.
    '''
    cursor = conn.cursor()
    sql = "SELECT * FROM `" + dataType + "` WHERE `ts` > \
        DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR)"
    print sql
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # No data, show bonne chance
    if not results:
        return render_template('index.html', results=results)

    # Write the fetched data into a csv
    si = StringIO.StringIO()
    csv_out = csv.writer(si)
    first_row = ['idx', 'ts', \
        'isoblue_id', 'wifins', 'cellns', 'netled', 'statled']
    csv_out.writerow(first_row)
    for row in results:
        csv_out.writerow(row)

    # make output response
    output = make_response(si.getvalue())
    output.headers['Content-Disposition'] = 'attachment; \
        filename=hb_last_hr.csv'
    output.headers['Content-Type'] = 'text/csv'
    return output

@app.route('/getLastDay/<string:dataType>')
def getHbLastDay(dataType):
    '''
    Get heartbeat message within the last day
    Output as an csv.
    '''
    cursor = conn.cursor()
    sql = "SELECT * FROM `" + dataType + "` WHERE `ts` > \
        DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)"
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # No data, show bonne chance
    if not results:
        return render_template('index.html', results=results)

    # Write the fetched data into a csv
    si = StringIO.StringIO()
    csv_out = csv.writer(si)
    first_row = ['idx', 'ts', \
        'isoblue_id', 'wifins', 'cellns', 'netled', 'statled']
    csv_out.writerow(first_row)
    for row in results:
        csv_out.writerow(row)

    # make output response
    output = make_response(si.getvalue())
    output.headers['Content-Disposition'] = 'attachment; \
        filename=hb_last_day.csv'
    output.headers['Content-Type'] = 'text/csv'
    return output

@app.route('/getLastHourById/<string:dataType>/<string:isoblueId>')
def getHbLastHourById(dataType, isoblueId):
    '''
    Get heartbeat message within the last hour by isoblue id.
    Output as an csv.
    '''
    cursor = conn.cursor()
    sql = "SELECT * FROM `" + dataType + \
        "` WHERE `isoblue_id` LIKE '%" + isoblueId + "%' AND" + \
        "`ts` > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR)"
    print sql
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # No data, show bonne chance
    if not results:
        return render_template('index.html', results=results)

    # Write the fetched data into a csv
    si = StringIO.StringIO()
    csv_out = csv.writer(si)
    first_row = ['idx', 'ts', \
        'isoblue_id', 'wifins', 'cellns', 'netled', 'statled']
    csv_out.writerow(first_row)
    for row in results:
       csv_out.writerow(row)

    # make output response
    output = make_response(si.getvalue())
    output.headers['Content-Disposition'] = \
        'attachment; filename=hb_last_hr_%s.csv' %isoblueId
    output.headers['Content-Type'] = 'text/csv'
    return output

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
