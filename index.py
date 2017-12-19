#!/usr/bin/env python
"""Commute Traffic Data."""

from __future__ import print_function
import sys
import logging
import os
import json
import time
import pymysql
from pymysql.cursors import DictCursor
from pytz import timezone
from datetime import datetime, timedelta


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

    if 'COMMUTE_ALLOW_ORIGIN' in os.environ:
        allow_origin = os.environ['COMMUTE_ALLOW_ORIGIN']
    else:
        allow_origin = '*'

    header = {'Content-Type': 'application/json',
              'Access-Control-Allow-Origin': allow_origin,
              'Access-Control-Allow-Methods': 'GET'}

    table_name = 'traffic'

    body = json.loads(event['body'])
    origin = body['origin']
    destination = body['destination']

    current_date = datetime.utcnow()
    past_date = current_date + timedelta(-30)
    start_date = current_date.strftime('%Y-%m-%d 00:00:00')
    end_date = past_date.strftime('%Y-%m-%d 00:00:00')

    with CONNECTION.cursor() as cursor:
        # check if database exists
        sql = ('select * from %s'
               ' where origin = "%s" and destination = "%s"'
               ' and timestamp between "%s" and "%s"') % (table_name, origin, destination,
                                                          start_date, end_date)
        logging.info(sql)
        cursor.execute(sql)
        recs = cursor.fetchall()
        logging.info(recs)
        results = {"x_axis": {'type': 'datetime'},
                   "series": [{'name': '', 'data': []}]}
        for rec in recs:
            results['series'][0]['name'] = '{0} -> {1}'.format(
                rec['origin'], rec['destination'])
            value = int(rec['duration_in_traffic']) / 60
            # timestamp = timezone('UTC').localize(rec['timestamp']).astimezone(timezone('EST'))
            timestamp = timezone('UTC').localize(rec['timestamp'])
            timestamp = float(time.mktime(timestamp.timetuple())) * 1000
            results['series'][0]['data'].append([timestamp, value])

    return {'statusCode': 200,
            'body': json.dumps(results),
            'headers': header}


if __name__ == '__main__':
    print(handler({}, None))
