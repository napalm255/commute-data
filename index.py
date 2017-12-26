#!/usr/bin/env python
"""Commute Traffic Data."""

from __future__ import print_function
import sys
import logging
import os
import json
import time
from datetime import datetime, timedelta
from urlparse import parse_qsl
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


def error(message, header=None, code=403):
    """Return error object."""
    if not header:
        header = {'Content-Type': 'application/json'}
    return {'statusCode': code,
            'body': json.dumps({'status': 'ERROR',
                                'message': message}),
            'headers': header}


def cors(origin):
    """CORS."""
    allowed_origins = ['http://127.0.0.1',
                       'https://127.0.0.1',
                       'http://localhost',
                       'https://localhost']

    if 'COMMUTE_ALLOW_ORIGIN' in os.environ:
        allowed_origins.append(os.environ['COMMUTE_ALLOW_ORIGIN'])

    logging.info(allowed_origins)

    allow_origin = ','.join([origin for x in allowed_origins if x in origin])
    allow_origin = '*'

    if not allow_origin:
        result = error('invalid origin: %s' % origin)
        logging.info(result)
        return result

    return allow_origin


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)

    # database table name
    table_name = 'traffic'

    # read headers
    headers = dict((k.lower(), v) for k, v in event['headers'].iteritems())

    # header
    header = {'Content-Type': 'application/json',
              'Access-Control-Allow-Origin': cors(headers['origin']),
              'Access-Control-Allow-Methods': 'POST'}
    logging.info(header)

    # load data
    if 'application/x-www-form-urlencoded' in headers['content-type'].lower():
        data = dict(parse_qsl(event['body']))
    else:
        result = error('invalid content-type', header)
        logging.info(result)
        return result

    # setup vars from event
    origin = data['origin']
    destination = data['destination']
    graph_name = '{0} -> {1}'.format(origin, destination)
    graph_type = 'area'
    if 'name' in data:
        graph_name = data['name']
    if 'type' in data:
        graph_type = data['type']

    # date range
    current_date = datetime.utcnow()
    past_date = current_date + timedelta(-365)
    start_date = past_date.strftime('%Y-%m-%d 00:00:00')
    end_date = current_date.strftime('%Y-%m-%d %H:%M:%S')

    # get data
    with CONNECTION.cursor() as cursor:
        # check if database exists
        sql = ('select origin, destination, timestamp, duration_in_traffic from %s'
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
