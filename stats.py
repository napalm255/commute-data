#!/usr/bin/env python3
"""Commute Traffic Data."""
# pylint: disable=broad-except

from __future__ import print_function
import sys
import logging
import json
from urllib.parse import parse_qsl
import pymysql
from pymysql.cursors import DictCursor
import boto3


# logging configuration
logging.getLogger().setLevel(logging.DEBUG)

try:
    SSM = boto3.client('ssm')

    PREFIX = '/commute'
    PARAMS = dict()
    for cat in ['database', 'headers', 'config']:
        PARAMS[cat] = SSM.get_parameters_by_path(Path='%s/%s' % (PREFIX, cat),
                                                 Recursive=True, WithDecryption=True)
    logging.debug('ssm: parameters(%s)', PARAMS)

    DATABASE = dict()
    for param in PARAMS['database']['Parameters']:
        if '/database/' in param['Name']:
            key = param['Name'].replace('%s/database/' % PREFIX, '')
            DATABASE.update({key: param['Value']})
    logging.debug('ssm: database(%s)', DATABASE)

    HEADERS = dict()
    for param in PARAMS['headers']['Parameters']:
        if '/headers/' in param['Name']:
            key = param['Name'].replace('%s/headers/' % PREFIX, '')
            HEADERS.update({key: param['Value']})
    logging.info('ssm: headers(%s)', HEADERS)

    ROUTES = dict()
    for param in PARAMS['config']['Parameters']:
        if '/config/routes' in param['Name']:
            ROUTES = json.loads(param['Value'])
    logging.info('ssm: routes(%s)', ROUTES)

    logging.info('ssm: successfully gathered parameters')
except ValueError as ex:
    logging.error('ssm: could not convert json routes (%s)', ex)
    sys.exit()
except Exception as ex:
    logging.error('ssm: could not connect to SSM. (%s)', ex)
    sys.exit()

try:
    CONNECTION = pymysql.connect(host=DATABASE['host'],
                                 user=DATABASE['user'],
                                 password=DATABASE['pass'],
                                 db=DATABASE['name'],
                                 autocommit=True,
                                 cursorclass=DictCursor)
    logging.info('database: successfully connected to mysql')
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
    return ''


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logging.info('event: %s', event)

    # read event headers
    headers = dict((k.lower(), v) for k, v in event['headers'].items())

    # header + cors
    try:
        header = dict(HEADERS)
        header.update({'Access-Control-Allow-Origin': cors(headers['origin'])})
        assert headers['origin'] == header['Access-Control-Allow-Origin']
        logging.info('header: %s', header)
    except AssertionError:
        return error('header: invalid origin')

    # load data
    try:
        assert 'application/x-www-form-urlencoded' in headers['content-type'].lower()
        data = dict(parse_qsl(event['body']))
    except AssertionError:
        message = 'invalid content-type: %s' % headers['content-type'].lower()
        return error(message, header)

    # setup vars from event
    try:
        graph = dict(ROUTES[data['id']])
        graph.update(data)
        if 'fields' in graph:
            graph['fields'] = ['s_%s' % x for x in list(graph['fields'].split(','))]
        if 'name' not in graph:
            graph['name'] = '{0} -> {1}'.format(graph['origin'], graph['destination'])
        if 'type' not in graph:
            graph['type'] = 'area'
        logging.debug('graph data: %s', graph)
    except KeyError as ex:
        message = 'invalid arguments (%s)' % ex
        return error(message, header)

    # get data
    with CONNECTION.cursor() as cursor:
        logging.info('database: query')
        sql = ('select * from %s where s_origin = "%s" and'
               ' s_destination = "%s"') % (DATABASE['stats/table'],
                                           graph['origin'], graph['destination'])
        logging.debug('database: sql(%s)', sql)

        cursor.execute(sql)
        recs = cursor.fetchall()

        results = {"stats": {"count": len(recs)},
                   "series": [{'type': graph['type'], 'name': graph['name'], 'data': []}]}
        logging.info('database: stats (%s)', results)

        for rec in recs:
            record = list()
            for field in graph['fields']:
                record.append(rec[field])
            results['series'][0]['data'].append(record)

    return {'statusCode': 200,
            'body': json.dumps(results),
            'headers': header}


if __name__ == '__main__':
    with open('test.json') as json_file:
        DATA = json.load(json_file)
    print(handler(DATA, None))
