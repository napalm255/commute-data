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
import boto3


try:
    SSM = boto3.client('ssm')

    PREFIX = 'commute_'
    PARAMS = SSM.describe_parameters(ParameterFilters=[{'Key': PREFIX,
                                                        'Option': 'BeginsWith'}])
    DATABASE = dict()
    for param in PARAMS['Parameters']:
        if 'database_' in param['Name']:
            key = param['Name'].replace('%sdatabase_' % PREFIX, '')
            val = SSM.get_parameter(Name=param['name'])['Parameter']['Value']
            DATABASE.update((key, val))

    HEADERS = dict()
    for param in PARAMS['Parameters']:
        if 'header_' in param['Name']:
            key = param['Name'].replace('%sheader_' % PREFIX, '')
            val = SSM.get_parameter(Name=param['name'])['Parameter']['Value']
            HEADERS.update((key, val))
# pylint: disable=broad-except
except Exception as ex:
    logging.error('error: could not connect to SSM. (%s)', ex)
    sys.exit()

try:
    CONNECTION = pymysql.connect(host=DATABASE['host'],
                                 user=DATABASE['user'],
                                 password=DATABASE['pass'],
                                 db=DATABASE['name'],
                                 autocommit=True,
                                 cursorclass=DictCursor)
    logging.info('Successfully connected to MySql.')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('error: could not connect to MySql. (%s)', ex)
    sys.exit()


def error(message, header=None, code=403):
    """Return error object."""
    logging.info('error handler')
    if not header:
        header = {'Content-Type': 'application/json'}
    logging.error('error: %s (%s)', message, header)
    return {'statusCode': code,
            'body': json.dumps({'status': 'ERROR',
                                'message': message}),
            'headers': header}


def cors(origin):
    """CORS."""
    logging.info('cors handler')
    allowed_origins = ['http://127.0.0.1',
                       'https://127.0.0.1',
                       'http://localhost',
                       'https://localhost']

    if 'COMMUTE_ALLOW_ORIGIN' in os.environ:
        allowed_origins.append(os.environ['COMMUTE_ALLOW_ORIGIN'])

    logging.debug('allowed_origins: %s', allowed_origins)
    allow_origin = ','.join([origin for x in allowed_origins if x in origin])
    allow_origin = '*'
    logging.debug('allow_origin: %s', allow_origin)
    return allow_origin


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.info('event: %s', event)
    logging.info(HEADERS)

    # database table name
    table_name = 'traffic'

    # read headers
    headers = dict((k.lower(), v) for k, v in event['headers'].iteritems())

    # cors
    try:
        allow_origin = cors(headers['origin'])
        assert allow_origin
    except AssertionError:
        return error('invalid origin')

    # header
    header = {'Content-Type': 'application/json',
              'Access-Control-Allow-Origin': allow_origin,
              'Access-Control-Allow-Methods': 'POST'}
    logging.debug('header: %s', header)

    # load data
    try:
        assert 'application/x-www-form-urlencoded' in headers['content-type'].lower()
        data = dict(parse_qsl(event['body']))
    except AssertionError:
        message = 'invalid content-type: %s' % headers['content-type'].lower()
        return error(message, header)

    # setup vars from event
    try:
        graph = {'org': data['origin'],
                 'dst': data['destination'],
                 'type': 'area',
                 'name': '{0} -> {1}'.format(data['origin'], data['destination'])}
        if 'name' in data:
            graph['name'] = data['name']
        if 'type' in data:
            graph['type'] = data['type']
        logging.debug('graph data: %s', graph)
    except KeyError as ex:
        message = 'invalid arguments (%s)' % ex
        return error(message, header)

    # date range
    dates = dict()
    dates['current'] = datetime.utcnow()
    dates['past'] = dates['current'] + timedelta(-365)
    dates['start'] = dates['past'].strftime('%Y-%m-%d 00:00:00')
    dates['end'] = dates['current'].strftime('%Y-%m-%d %H:%M:%S')
    logging.debug('dates: %s', dates)

    # get data
    with CONNECTION.cursor() as cursor:
        logging.info('database: query')
        sql = ('select origin, destination, timestamp, duration_in_traffic from %s'
               ' where origin = "%s" and destination = "%s"'
               ' and timestamp between "%s" and "%s"') % (table_name,
                                                          graph['org'], graph['dst'],
                                                          dates['start'], dates['end'])
        logging.debug('database: sql(%s)', sql)

        cursor.execute(sql)
        recs = cursor.fetchall()
        logging.info('database: #%s records', len(recs))

        results = {"x_axis": {'type': 'datetime'},
                   "series": [{'type': graph['type'], 'name': graph['name'], 'data': []}]}
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
