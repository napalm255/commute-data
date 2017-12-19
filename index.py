#!/usr/bin/env python
"""Commute Traffic Data."""

from __future__ import print_function
import sys
import logging
import os
import json
import pymysql
from pymysql.cursors import DictCursor


try:
    DATA = {'db_host': os.environ['DATABASE_HOST'],
            'db_user': os.environ['DATABASE_USER'],
            'db_pass': os.environ['DATABASE_PASS'],
            'db_name': os.environ['DATABASE_NAME']}
    CONNECTION = pymysql.connect(host=DATA['db_host'],
                                 user=DATA['db_user'],
                                 password=DATA['db_pass'],
                                 db=DATA['db_name'],
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

    header = {'Content-Type': 'application/json',
              'Access-Control-Allow-Origin': '*'}

    table_name = 'traffic'

    with CONNECTION.cursor() as cursor:
        # check if database exists
        cursor.execute('select * from %s' % (table_name))
        recs = cursor.fetchall()
        logging.info(recs)
        results = {"x_axis": {'type': 'datetime'},
                   "series": [{'name': '', 'data': []}]}
        for rec in recs:
            results['series'][0]['name'] = '{0} -> {1}'.format(
                rec['origin'], rec['destination'])
            value = int(rec['duration_in_traffic']) / 60
            timestamp = rec['timestamp']
            results['series'][0]['data'].append([timestamp.strftime('%Y-%m-%d %H:%M:%S'), value])

    return {'statusCode': 200,
            'body': json.dumps(results),
            'headers': header}


if __name__ == '__main__':
    print(handler({}, None))
