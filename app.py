#!/usr/bin/env python

from flask import Flask, request, render_template, make_response, \
    send_from_directory, url_for

import pymysql
import csv
import StringIO
import time

from ibQuery import ibQuery

with open('passwd') as f:
  _db_passwd = f.readline().strip('\n')

# connect to the database
conn = pymysql.connect(host='localhost', \
                       user='isoblue', \
                       password=_db_passwd,
                       db='isoblueData')

app = Flask('isoblue-db')

@app.route('/csv/<filename>')
def csvDownloads(filename):
    return send_from_directory('csv', filename)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    return '%s is not implemented. Please stand by.' % path

@app.route('/getlasthour/<string:dataType>')
def getlasthour(dataType):
    '''
    Get specified message within the last hour.
    Output as an csv.
    '''
    cursor = conn.cursor()

    # Get the query parameters and the csv first row
    isoblue_id = None
    q = ibQuery(dataType, None)
    app.logger.debug('%s, %s', q.query_param, q.first_row)

    sql = "SELECT " + q.query_param + " FROM `" + dataType + \
        "` WHERE `ts` > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR)"
    app.logger.info('Query is: %s', sql)

    # Get start time
    start = time.time()

    # Execute the query
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # Get end time
    end = time.time()

    if results:
        app.logger.info('Query returned data!')

        filename = '%s_last_hr.csv' %dataType

        p = {'filename': filename, \
            'count': len(results), \
            'duration': round((end - start), 3),
            'sql': sql}

        # Write the fetched data into a csv
        with open('./csv/' + filename, 'w') as f:
            csv_out = csv.writer(f)
            csv_out.writerow(q.first_row)
            for row in results:
                csv_out.writerow(row)
    else:
        app.logger.info('Query returned nothing!')
        p = {'sql': sql}

    app.logger.debug('Template parameters: %s', p)
    return render_template('index.html', p=p)

    # make output response as a attachment
#    output = make_response(si.getvalue())
#    output.headers['Content-Disposition'] = 'attachment; \
#        filename=%s_last_hr.csv' %dataType
#    output.headers['Content-Type'] = 'text/csv'
#    return output

@app.route('/getlastday/<string:dataType>')
def getLastDay(dataType):
    '''
    Get specified message within the last day
    Output as an csv.
    '''
    cursor = conn.cursor()

    # Get the query parameters and the csv first row
    isoblue_id = None
    q = ibQuery(dataType, None)
    app.logger.debug('%s, %s', q.query_param, q.first_row)

    sql = "SELECT " + q.query_param + " FROM `" + dataType + \
        "` WHERE `ts` > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)"
    app.logger.info('Query is: %s', sql)

    # Get start time
    start = time.time()

    # Execute the query
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # Get end time
    end = time.time()

    if results:
        app.logger.info('Query returned data!')

        filename = '%s_last_day.csv' %dataType

        p = {'filename': filename, \
            'count': len(results), \
            'duration': round((end - start), 3),
            'sql': sql}

        # Write the fetched data into a csv
        with open('./csv/' + filename, 'w') as f:
            csv_out = csv.writer(f)
            csv_out.writerow(q.first_row)
            for row in results:
                csv_out.writerow(row)
    else:
        app.logger.info('Query returned nothing!')
        p = {'sql': sql}

    return render_template('index.html', p=p)

@app.route('/getlasthourbyid/<string:dataType>/<string:isoblueId>')
def getLastHourById(dataType, isoblueId):
    '''
    Get specified message within the last hour by isoblue id.
    Output as an csv.
    '''
    cursor = conn.cursor()

    # Get the query parameters and the csv first row
    q = ibQuery(dataType, isoblueId)
    app.logger.debug('%s, %s', q.query_param, q.first_row)

    sql = "SELECT " + q.query_param + " FROM `" + dataType + \
        "` WHERE `isoblue_id` LIKE '%" + isoblueId + "%' AND" + \
        "`ts` > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR)"
    app.logger.info('Query is: %s', sql)

    # Get start time
    start = time.time()

    # Execute the query
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # Get end time
    end = time.time()

    if results:
        app.logger.info('Query returned data!')

        filename = '%s_last_hour_%s.csv' %(dataType, isoblueId)

        p = {'filename': filename, \
            'count': len(results), \
            'duration': round((end - start), 3),
            'sql': sql}

        # Write the fetched data into a csv
        with open('./csv/%s_last_hr_%s.csv' %(dataType, isoblueId), 'w') as f:
            csv_out = csv.writer(f)
            csv_out.writerow(q.first_row)
            for row in results:
                csv_out.writerow(row)
    else:
        app.logger.info('Query returned nothing!')
        p = {'sql': sql}

    return render_template('index.html', p=p)


@app.route('/getlastdaybyid/<string:dataType>/<string:isoblueId>')
def getLastDayById(dataType, isoblueId):
    '''
    Get specified message within the last day by isoblue id.
    Output as an csv.
    '''
    cursor = conn.cursor()

    # Get the query parameters and the csv first row
    q = ibQuery(dataType, isoblueId)
    app.logger.debug('%s, %s', q.query_param, q.first_row)

    sql = "SELECT " + q.query_param + " FROM `" + dataType + \
        "` WHERE `isoblue_id` LIKE '%" + isoblueId + "%' AND" + \
        "`ts` > DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)"
    app.logger.info('Query is: %s', sql)
    # Get start time
    start = time.time()

    # Execute the query
    cursor.execute(sql)
    conn.commit();
    results = cursor.fetchall()

    # Get end time
    end = time.time()

    if results:
        app.logger.info('Query returned data!')

        filename = '%s_last_day_%s.csv' %(dataType, isoblueId)

        p = {'filename': filename, \
            'count': len(results), \
            'duration': round((end - start), 3),
            'sql': sql}
        # Write the fetched data into a csv
        with open('./csv/' + filename, 'w') as f:
            csv_out = csv.writer(f)
            csv_out.writerow(q.first_row)
            for row in results:
                csv_out.writerow(row)
    else:
        app.logger.info('Query returned nothing!')
        p = {'sql': sql}

    return render_template('index.html', p=p)

if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)
