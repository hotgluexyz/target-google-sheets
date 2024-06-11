#!/usr/bin/env python3

import argparse
import functools
import io
import os
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources
import backoff

from jsonschema import validate
import singer

import httplib2

from apiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import ssl

try:
    collectionsAbc = collections.abc
except AttributeError:
    collectionsAbc = collections   


# Read the config
try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()
except ImportError:
    flags = None

logging.getLogger('backoff').setLevel(logging.CRITICAL)
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

MAX_RETRIES = 10

def get_credentials(config):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    auth_url = config.get('auth_url')
    if not auth_url:
        auth_url = 'https://oauth2.googleapis.com/token'
    credentials = client.OAuth2Credentials(config['access_token'], config['client_id'], config['client_secret'], config['refresh_token'], config['expires_in'], auth_url, config.get("user-agent", 'target-google-sheets <hello@hotglue.xyz>'))
    return credentials


def giveup(exc):
    return exc.resp is not None \
        and 400 <= int(exc.resp["status"]) < 500 \
        and int(exc.resp["status"]) != 429


def retry_handler(details):
    logger.info("Http unsuccessful request -- Retry %s/%s", details['tries'], MAX_RETRIES)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def get_spreadsheet(service, spreadsheet_id):
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

def get_values(service, spreadsheet_id, range):
    return service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range).execute()

def add_sheet(service, spreadsheet_id, title, lines=1000, columns=100):
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            'requests':[
                {
                    'addSheet': {
                    'properties': {
                        'title': title,
                        'gridProperties': {
                            'rowCount': lines,
                            'columnCount': columns
                        }
                    }
                    }
                }
            ]
        }).execute()

def append_schema_keys(record, schema):
    for key in schema['properties']:
        if key not in record:
            record[key] = None
    return record

@backoff.on_exception(
    backoff.constant,
    (
        HttpError,
        ssl.SSLEOFError,
        ConnectionError,
        ConnectionResetError,
    ),
    interval=60,
    max_tries=MAX_RETRIES,
    jitter=None,
    giveup=giveup,
    on_backoff=retry_handler,
)
def append_to_sheet(service, spreadsheet_id, range, values):
    response = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range,
        valueInputOption='USER_ENTERED',
        body={'values': [values]}).execute()
    logger.info(f"Appended data to sheet {spreadsheet_id}. Response: {response}")
    return response


@backoff.on_exception(
    backoff.expo,
    (
        HttpError,
        ssl.SSLEOFError,
        ConnectionError,
        ConnectionResetError,
    ),
    max_tries=MAX_RETRIES,
    jitter=None,
    giveup=giveup,
    on_backoff=retry_handler,
)
def update_to_sheet(service, spreadsheet_id, range, values):
    response = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range,
        valueInputOption='USER_ENTERED',
        body={'values': [values]}).execute()
    logger.info(f"Updated data in sheet {spreadsheet_id}. Response: {response}")
    return response


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collectionsAbc.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def get_pk_index(properties_arr, key_properties):
    pk_indexes = []
    for i, property in enumerate(properties_arr):
        if property in key_properties:
            pk_indexes.append(i)
    return pk_indexes

def persist_lines(service, spreadsheet, lines):
    state = None
    schemas = {}
    key_properties = {}

    headers_by_stream = {}
    data = None

    lines = list(lines)

    for line_no, line in enumerate(lines):
        posted = False
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]
            validate(msg.record, schema)
            flattened_record = flatten(msg.record)
            flattened_record = append_schema_keys(flattened_record, schema)

            matching_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] == msg.stream]
            new_sheet_needed = len(matching_sheet) == 0
            range_name = "{}!A{}:ZZZ".format(msg.stream, line_no)
            append = functools.partial(append_to_sheet, service, spreadsheet['spreadsheetId'], range_name)
            update_row = functools.partial(update_to_sheet, service, spreadsheet['spreadsheetId'])

            if data is None and not new_sheet_needed:
                data = get_values(service, spreadsheet['spreadsheetId'], range_name)

                if data.get('values'):
                    sheet_headers = data.get('values')[0]
                    pks = key_properties[msg.stream]
                    pk_indexes = get_pk_index(sheet_headers, pks)
                    key_properties[msg.stream + "_pk_index"] = pk_indexes
                else:
                    headers_by_stream[msg.stream] = list(flattened_record.keys())
                    append(headers_by_stream[msg.stream])

            if new_sheet_needed:
                columns_to_be_added = len(json.loads(lines[0]).get("schema", {}).get("properties", {}).keys())
                lines_to_be_added = len(["line" for line in lines if json.loads(line).get("stream") == msg.stream and json.loads(line).get("type") == "RECORD"])
                add_sheet(service, spreadsheet['spreadsheetId'], msg.stream, lines_to_be_added, columns_to_be_added)
                spreadsheet = get_spreadsheet(service, spreadsheet['spreadsheetId']) # refresh this for future iterations
                headers_by_stream[msg.stream] = list(flattened_record.keys())
                append(headers_by_stream[msg.stream])

            elif msg.stream not in headers_by_stream:
                first_row = get_values(service, spreadsheet['spreadsheetId'], range_name + '1')
                if 'values' in first_row:
                    headers_by_stream[msg.stream] = first_row.get('values', None)[0]
                    new_records_columns = flattened_record.keys()
                    new_columns = [col for col in new_records_columns if col not in headers_by_stream[msg.stream]]
                    if new_columns:
                        #update headers in google sheets mantaining the order of existing columns
                        new_headers = headers_by_stream[msg.stream] + new_columns
                        headers_range = "{}!A1:ZZZ1".format(msg.stream)
                        update_row(headers_range, new_headers)
                        headers_by_stream[msg.stream] = new_headers
                        # update the primary key index for dupplicates logic
                        pks = key_properties[msg.stream]
                        pk_indexes = get_pk_index(new_headers, pks)
                        key_properties[msg.stream + "_pk_index"] = pk_indexes
                else:
                    headers_by_stream[msg.stream] = list(flattened_record.keys())
                    append(headers_by_stream[msg.stream])

            if data is not None and not new_sheet_needed and key_properties.get(msg.stream) and key_properties.get(msg.stream + "_pk_index"):
                for i, row in enumerate(data["values"]):
                    pk_index = key_properties[msg.stream + "_pk_index"][0]
                    if len(row) != 0 and len(row) >= pk_index and (row[pk_index] == flattened_record[key_properties[msg.stream][0]]):
                        index = i + 1
                        update_range_name = "{}!A{}:ZZZ{}".format(msg.stream, index, index)
                        result = update_row(update_range_name, [flattened_record.get(x, None) for x in headers_by_stream[msg.stream]])
                        posted = True
            if data is not None and not new_sheet_needed and not len(key_properties[msg.stream]):
                print("No primary keys provided, not able to update existing rows")

            if not posted:
                result = append([flattened_record.get(x, None) for x in headers_by_stream[msg.stream]]) # order by actual headers found in sheet

            state = None
        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value
        elif isinstance(msg, singer.SchemaMessage):
            schemas[msg.stream] = msg.schema
            key_properties[msg.stream] = msg.key_properties

        else:
            raise Exception("Unrecognized message {}".format(msg))

    return state


def main():
    # Read the config
    with open(flags.config) as input:
        config = json.load(input)

    # Get the Google OAuth creds
    credentials = get_credentials(config)
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    # Get spreadsheet_id
    spreadsheet = get_spreadsheet(service, config['spreadsheet_id'])

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = None
    state = persist_lines(service, spreadsheet, input)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
