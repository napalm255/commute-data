#!/usr/bin/env python
"""Commute Traffic Data."""

from __future__ import print_function
import sys
import logging
import os
import json
from collections import OrderedDict
import pymysql


try:
    DATA = {'db_host': os.environ['DATABASE_HOST'],
            'db_user': os.environ['DATABASE_USER'],
            'db_pass': os.environ['DATABASE_PASS'],
            'db_name': os.environ['DATABASE_NAME']}
    CONNECTION = pymysql.connect(host=DATA['db_host'],
                                 user=DATA['db_user'],
                                 password=DATA['db_pass'],
                                 db=DATA['db_name'])
    logging.info('Successfully connected to MySql.')
except:
    logging.error('Unexpected error: could not connect to MySql.')
    sys.exit()


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)

    header = {'Content-Type': 'application/json'}

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
            results['series'][0]['data'].append([timestamp, value])

    return {'statusCode': 200,
            'body': json.dumps(results),
            'headers': header}


if __name__ == '__main__':
    print(handler({}, None))
