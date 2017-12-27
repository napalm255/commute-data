#!/usr/bin/env python
"""Commute Traffic Data."""

from __future__ import print_function
import sys
import logging
import json
import time
from datetime import datetime, timedelta
from urlparse import parse_qsl
import pymysql
from pymysql.cursors import DictCursor
from pytz import timezone
import boto3


# logging configuration
logging.getLogger().setLevel(logging.INFO)

try:
    SSM = boto3.client('ssm')

    PREFIX = '/commute'
    PARAMS = SSM.get_parameters_by_path(Path=PREFIX, Recursive=True,
                                        WithDecryption=True)
    logging.debug('ssm: parameters(%s)', PARAMS)

    DATABASE = dict()
    HEADERS = dict()
    for param in PARAMS['Parameters']:
        if '/database/' in param['Name']:
            key = param['Name'].replace('%s/database/' % PREFIX, '')
            DATABASE.update({key: param['Value']})
        elif '/headers/' in param['Name']:
            key = param['Name'].replace('%s/headers/' % PREFIX, '')
            HEADERS.update({key: param['Value']})
    logging.debug('ssm: database(%s)', DATABASE)
    logging.debug('ssm: headers(%s)', HEADERS)

    logging.info('ssm: successfully gathered parameters')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('database: could not connect to SSM. (%s)', ex)
    sys.exit()

try:
    CONNECTION = pymysql.connect(host=DATABASE['host'],
                                 user=DATABASE['user'],
                                 password=DATABASE['pass'],
                                 db=DATABASE['name'],
                                 autocommit=True,
                                 cursorclass=DictCursor)
    logging.info('database: successfully connected to mysql')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('database: could not connect to mysql (%s)', ex)
    sys.exit()


def error(message, header=None, code=403):
    """Return error object."""
    logging.info('handler: error')
    if not header:
        header = {'Content-Type': 'application/json',
                  'Access-Control-Allow-Origin': '*'}
    logging.error('%s (%s)', message, header)
    return {'statusCode': code,
            'body': json.dumps({'status': 'ERROR',
                                'message': message}),
            'headers': header}


def cors(origin):
    """CORS."""
    logging.info('handler: cors')
    if origin in HEADERS['Access-Control-Allow-Origin']:
        logging.debug('allow_origin: %s', origin)
        return origin
    return '*'  # temp allow all


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logging.info('event: %s', event)

    # read event headers
    headers = dict((k.lower(), v) for k, v in event['headers'].iteritems())

    # header + cors
    try:
        header = dict(HEADERS)
        header.update({'Access-Control-Allow-Origin': cors(headers['origin'])})
        assert headers['origin'] == header['Access-Control-Allow-Origin']
        logging.debug('header: %s', header)
    except AssertionError:
        return error('invalid origin')

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
               ' and timestamp between "%s" and "%s"') % (DATABASE['table'],
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
