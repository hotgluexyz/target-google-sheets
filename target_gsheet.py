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
BATCH_SIZE = 1000  # Default batch size for bulk operations

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
def bulk_append_to_sheet(service, spreadsheet_id, range, values_list):
    """Bulk append multiple rows to a sheet in a single API call."""
    if not values_list:
        return None
    
    response = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range,
        valueInputOption='USER_ENTERED',
        body={'values': values_list}).execute()
    logger.info(f"Bulk appended {len(values_list)} rows to sheet {spreadsheet_id}")
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
def bulk_update_to_sheet(service, spreadsheet_id, updates_list):
    """Bulk update multiple ranges in a sheet using batchUpdate."""
    if not updates_list:
        return None
    
    # Prepare batch update requests
    data = []
    for range_name, values in updates_list:
        data.append({
            'range': range_name,
            'values': [values] if not isinstance(values[0], list) else values,
            'majorDimension': 'ROWS'
        })
    
    body = {
        'valueInputOption': 'USER_ENTERED',
        'data': data
    }
    
    response = service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body).execute()
    logger.info(f"Bulk updated {len(updates_list)} ranges in sheet {spreadsheet_id}")
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


def get_dynamic_batch_size(column_count, batch_size=None):
    """Get optimal batch size based on number of columns in the stream."""
    if batch_size is not None:
        return batch_size

    if column_count <= 5:
        return 10000
    elif column_count <= 10:
        return 5000
    elif column_count <= 20:
        return 1000
    else:
        return 500

def process_record_batch(service, spreadsheet_id, stream_name, batch_records, headers, key_properties, existing_data=None):
    """Process a batch of records for a specific stream."""
    if not batch_records:
        return []
    
    # Separate records that need updates vs inserts
    updates_list = []
    insert_rows = []
    
    # Handle updates if we have primary keys and existing data
    if (key_properties.get(stream_name) and 
        key_properties.get(stream_name + "_pk_index") and 
        existing_data and existing_data.get('values')):
        
        pk_col = key_properties[stream_name][0]
        pk_index = key_properties[stream_name + "_pk_index"][0]
        existing_rows = existing_data.get('values', [])
        
        for record in batch_records:
            record_pk_value = record.get(pk_col)
            updated = False
            
            # Look for existing row to update
            for i, existing_row in enumerate(existing_rows):
                if (len(existing_row) > pk_index and 
                    existing_row[pk_index] == str(record_pk_value)):
                    # Found matching row - prepare for update
                    row_index = i + 1  # Google Sheets is 1-indexed
                    range_name = f"{stream_name}!A{row_index}:ZZZ{row_index}"
                    row_values = [record.get(col, None) for col in headers]
                    updates_list.append((range_name, row_values))
                    updated = True
                    break
            
            if not updated:
                # No existing row found - add to inserts
                row_values = [record.get(col, None) for col in headers]
                insert_rows.append(row_values)
    else:
        # No primary keys or existing data - all records are inserts
        for record in batch_records:
            row_values = [record.get(col, None) for col in headers]
            insert_rows.append(row_values)
    
    results = []
    
    # Execute bulk update
    if updates_list:
        logger.info(f"Bulk updating {len(updates_list)} rows for stream {stream_name}")
        result = bulk_update_to_sheet(service, spreadsheet_id, updates_list)
        results.append(result)
    
    # Execute bulk insert
    if insert_rows:
        logger.info(f"Bulk inserting {len(insert_rows)} rows for stream {stream_name}")
        range_name = f"{stream_name}!A:ZZZ"
        result = bulk_append_to_sheet(service, spreadsheet_id, range_name, insert_rows)
        results.append(result)
    
    return results


def persist_lines(service, spreadsheet, lines, batch_size=None):
    state = None
    schemas = {}
    key_properties = {}
    headers_by_stream = {}
    existing_data_by_stream = {}
    
    # Collect all records by stream
    records_by_stream = collections.defaultdict(list)
    
    lines = list(lines)
    
    # First pass: Parse all messages and collect by type
    for line_no, line in enumerate(lines):
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value
            
        elif isinstance(msg, singer.SchemaMessage):
            schemas[msg.stream] = msg.schema
            key_properties[msg.stream] = msg.key_properties
            
        elif isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))
                
            schema = schemas[msg.stream]
            validate(msg.record, schema)
            flattened_record = flatten(msg.record)
            flattened_record = append_schema_keys(flattened_record, schema)
            
            records_by_stream[msg.stream].append(flattened_record)
            
        else:
            raise Exception("Unrecognized message {}".format(msg))
    
    logger.info(f"Collected {sum(len(records) for records in records_by_stream.values())} total records across {len(records_by_stream)} streams")
    
    # Second pass: Process records by stream in batches
    for stream_name, stream_records in records_by_stream.items():
        logger.info(f"Processing {len(stream_records)} records for stream '{stream_name}'")
        
        # Check if sheet exists
        matching_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] == stream_name]
        new_sheet_needed = len(matching_sheet) == 0
        
        if new_sheet_needed:
            # Create new sheet
            schema = schemas[stream_name]
            columns_to_be_added = len(schema.get("properties", {}).keys())
            lines_to_be_added = len(stream_records)
            add_sheet(service, spreadsheet['spreadsheetId'], stream_name, lines_to_be_added + 100, columns_to_be_added + 10)
            spreadsheet = get_spreadsheet(service, spreadsheet['spreadsheetId'])  # refresh
            
            # Set headers from first record
            if stream_records:
                headers_by_stream[stream_name] = list(stream_records[0].keys())
                header_range = f"{stream_name}!A1:ZZZ1"
                bulk_append_to_sheet(service, spreadsheet['spreadsheetId'], header_range, [headers_by_stream[stream_name]])
        
        else:
            # Get existing headers and data for updates
            range_name = f"{stream_name}!A:ZZZ"
            existing_data = get_values(service, spreadsheet['spreadsheetId'], range_name)
            existing_data_by_stream[stream_name] = existing_data
            
            if existing_data.get('values'):
                sheet_headers = existing_data.get('values')[0]
                headers_by_stream[stream_name] = sheet_headers
                
                # Set up primary key indexes for updates
                if key_properties.get(stream_name):
                    pks = key_properties[stream_name]
                    pk_indexes = get_pk_index(sheet_headers, pks)
                    key_properties[stream_name + "_pk_index"] = pk_indexes
                
                # Check if we need to add new columns
                if stream_records:
                    new_records_columns = stream_records[0].keys()
                    new_columns = [col for col in new_records_columns if col not in sheet_headers]
                    if new_columns:
                        new_headers = sheet_headers + new_columns
                        headers_range = f"{stream_name}!A1:ZZZ1"
                        update_to_sheet(service, spreadsheet['spreadsheetId'], headers_range, new_headers)
                        headers_by_stream[stream_name] = new_headers
                        
                        # Update primary key indexes
                        if key_properties.get(stream_name):
                            pks = key_properties[stream_name]
                            pk_indexes = get_pk_index(new_headers, pks)
                            key_properties[stream_name + "_pk_index"] = pk_indexes
            else:
                # Empty sheet - set headers from first record
                if stream_records:
                    headers_by_stream[stream_name] = list(stream_records[0].keys())
                    header_range = f"{stream_name}!A1:ZZZ1"
                    bulk_append_to_sheet(service, spreadsheet['spreadsheetId'], header_range, [headers_by_stream[stream_name]])
        
        # Process records in batches with dynamic sizing based on column count
        headers = headers_by_stream.get(stream_name, [])
        existing_data = existing_data_by_stream.get(stream_name)
        
        if batch_size is not None:
            dynamic_batch_size = batch_size
        else:
            # Calculate dynamic batch size based on column count
            column_count = len(headers)
            dynamic_batch_size = get_dynamic_batch_size(column_count)
            logger.info(f"Stream '{stream_name}' has {column_count} columns, using batch size: {dynamic_batch_size}")
        
        for i in range(0, len(stream_records), dynamic_batch_size):
            batch = stream_records[i:i + dynamic_batch_size]
            logger.info(f"Processing batch {i//dynamic_batch_size + 1} of {(len(stream_records)-1)//dynamic_batch_size + 1} for stream '{stream_name}' ({len(batch)} records)")
            
            process_record_batch(
                service=service,
                spreadsheet_id=spreadsheet['spreadsheetId'],
                stream_name=stream_name,
                batch_records=batch,
                headers=headers,
                key_properties=key_properties,
                existing_data=existing_data
            )
    
    logger.info("Bulk processing completed")
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
    try:
        spreadsheet_id = config['spreadsheet_id'] if 'spreadsheet_id' in config else config['files'][0]['id']
    except Exception as e:
        logger.error(f"Error getting spreadsheet_id: {e}")
        logger.error(f"Either 'spreadsheet_id' or a list called 'files' with at least one element that contains the 'id' of the spreadsheet must be provided in the config")
        raise e
    spreadsheet = get_spreadsheet(service, spreadsheet_id)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = None
    batch_size = config.get('batch_size')
    if batch_size is not None:
        logger.info(f"Using batch size: {batch_size}")
    else:
        logger.info("Using dynamic batch sizes based on column count")
    state = persist_lines(service, spreadsheet, input, batch_size)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
