#!/usr/bin/env python
"""Commute Traffic Data."""

from __future__ import print_function
import sys
import logging
import os
import json
import time
from datetime import datetime, timedelta
import pymysql
from pymysql.cursors import DictCursor
from pytz import timezone


try:
    DATA = {'db_host': os.environ['DATABASE_HOST'],
            'db_user': os.environ['DATABASE_USER'],
            'db_pass': os.environ['DATABASE_PASS'],
            'db_name': os.environ['DATABASE_NAME']}
    CONNECTION = pymysql.connect(host=DATA['db_host'],
                                 user=DATA['db_user'],
                                 password=DATA['db_pass'],
                                 db=DATA['db_name'],
                                 autocommit=True,
                                 cursorclass=DictCursor)
    logging.info('Successfully connected to MySql.')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('Unexpected error: could not connect to MySql. (%s)', ex)
    sys.exit()


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)

    # CORS
    req_origin = event['headers']['Origin']
    allowed_origins = ['http://127.0.0.1',
                       'https://127.0.0.1',
                       'http://localhost',
                       'https://localhost']
    if 'COMMUTE_ALLOW_ORIGIN' in os.environ:
        allowed_origins.append(os.environ['COMMUTE_ALLOW_ORIGIN'])
    allow_origin = ','.join([req_origin for x in allowed_origins if x in req_origin])
    logging.info(allowed_origins)

    # header
    header = {'Content-Type': 'application/json',
              'Access-Control-Allow-Origin': allow_origin,
              'Access-Control-Allow-Methods': 'POST'}
    logging.info(header)

    # fail early if unallowed origin
    if not allow_origin:
        result = {'statusCode': 403,
                  'body': json.dumps({'status': 'ERROR',
                                      'message': 'origin not allowed'}),
                  'headers': header}
        logging.info(result)
        return result

    # database table name
    table_name = 'traffic'

    # setup vars from event
    body = json.loads(event['body'])
    origin = body['origin']
    destination = body['destination']
    graph_name = '{0} -> {1}'.format(origin, destination)
    graph_type = 'area'
    if 'name' in body:
        graph_name = body['name']
    if 'type' in body:
        graph_type = body['type']

    # date range
    current_date = datetime.utcnow()
    past_date = current_date + timedelta(-365)
    start_date = past_date.strftime('%Y-%m-%d 00:00:00')
    end_date = current_date.strftime('%Y-%m-%d %H:%M:%S')

    # get data
    with CONNECTION.cursor() as cursor:
        # check if database exists
        sql = ('select * from %s'
               ' where origin = "%s" and destination = "%s"'
               ' and timestamp between "%s" and "%s"') % (table_name, origin, destination,
                                                          start_date, end_date)
        logging.info(sql)
        cursor.execute(sql)
        recs = cursor.fetchall()
        logging.info('number of records: %s', len(recs))
        results = {"x_axis": {'type': 'datetime'},
                   "series": [{'type': graph_type, 'name': graph_name, 'data': []}]}
        for rec in recs:
            value = int(rec['duration_in_traffic']) / 60
            timestamp = timezone('UTC').localize(rec['timestamp'])
            timestamp = float(time.mktime(timestamp.timetuple())) * 1000
            results['series'][0]['data'].append([timestamp, value])

    return {'statusCode': 200,
            'body': json.dumps(results),
            'headers': header}


if __name__ == '__main__':
    print(handler({}, None))
