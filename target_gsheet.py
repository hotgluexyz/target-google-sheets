#!/usr/bin/env python3

import argparse
from datetime import datetime
import io
import os
import sys
import json
import logging
import collections
import backoff
import psutil

from jsonschema import validate
import singer

import httplib2

from apiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client import client
from oauth2client import tools
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

def log_memory_usage(msg):
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB
    logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg} - Memory usage: {memory_usage:.2f} MB")


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


def get_dynamic_batch_size(column_count):
    """Get optimal batch size based on number of columns in the stream."""
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
    
    # Process streams individually for memory efficiency
    
    lines = list(lines)
    stream_schemas = [s for s in lines if '"type": "SCHEMA"' in s]
    stream_names = []
    for line_no, schema_line in enumerate(stream_schemas):
        try:
            msg = singer.parse_message(schema_line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(schema_line))
            raise
        
        if isinstance(msg, singer.SchemaMessage):
            schemas[msg.stream] = msg.schema
            key_properties[msg.stream] = msg.key_properties
            logger.info(f"Stream '{msg.stream}' has {len(msg.schema['properties'])} columns")
            stream_names.append(msg.stream)
            
    # Process each stream individually to avoid loading all data into memory
    for stream_name in stream_names:
        logger.info(f"Processing stream '{stream_name}' with {len(schemas[stream_name]['properties'])} columns")
        
        # Initialize stream setup
        matching_sheet = [s for s in spreadsheet['sheets'] if s['properties']['title'] == stream_name]
        new_sheet_needed = len(matching_sheet) == 0
        
        if new_sheet_needed:
            # Create new sheet
            schema = schemas[stream_name]
            columns_to_be_added = len(schema.get("properties", {}).keys())
            add_sheet(service, spreadsheet['spreadsheetId'], stream_name, 10000, columns_to_be_added)
            spreadsheet = get_spreadsheet(service, spreadsheet['spreadsheetId'])  # refresh
            logger.info(f"Created new sheet '{stream_name}'")
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
        
        # Determine batch size for this stream
        if batch_size is not None:
            dynamic_batch_size = batch_size
        else:
            column_count = len(schemas[stream_name]['properties'])
            dynamic_batch_size = get_dynamic_batch_size(column_count)
            logger.info(f"Stream '{stream_name}' using batch size: {dynamic_batch_size}")
        
        # Process records for this stream in batches
        batch_records = []
        stream_record_count = 0
        batch_count = 0
        
        # Filter and process records for this specific stream
        for line_no, line in enumerate(lines):
            if f'"type": "RECORD", "stream": "{stream_name}"' in line:
                try:
                    msg = singer.parse_message(line)
                    if isinstance(msg, singer.RecordMessage) and msg.stream == stream_name:
                        schema = schemas[stream_name]
                        validate(msg.record, schema)
                        flattened_record = flatten(msg.record)
                        flattened_record = append_schema_keys(flattened_record, schema)
                        
                        batch_records.append(flattened_record)
                        stream_record_count += 1
                        
                        # Process batch when it's full
                        if len(batch_records) >= dynamic_batch_size:
                            batch_count += 1
                            process_stream_batch(service, spreadsheet, stream_name, batch_records,
                                               headers_by_stream, key_properties, existing_data_by_stream, batch_count)
                            batch_records = []  # Clear batch
                            
                except json.decoder.JSONDecodeError:
                    logger.error("Unable to parse:\n{}".format(line))
                    continue
        
        # Process remaining records in final batch
        if batch_records:
            batch_count += 1
            process_stream_batch(service, spreadsheet, stream_name, batch_records,
                               headers_by_stream, key_properties, existing_data_by_stream, batch_count)
        
        logger.info(f"Completed stream '{stream_name}': {stream_record_count} records processed in {batch_count} batches")
        
        # Process state messages after each stream
        for line in lines:
            if '"type": "STATE"' in line:
                try:
                    msg = singer.parse_message(line)
                    if isinstance(msg, singer.StateMessage):
                        state = msg.value
                except json.decoder.JSONDecodeError:
                    continue
    
    logger.info("Stream-by-stream processing completed")
    return state


def process_stream_batch(service, spreadsheet, stream_name, batch_records, headers_by_stream, 
                        key_properties, existing_data_by_stream, batch_num):
    """Process a batch of records for a specific stream."""
    if not batch_records:
        return
    
    logger.info(f"Processing batch {batch_num} for stream '{stream_name}' ({len(batch_records)} records)")
    
    # Set headers if not done yet (for new sheets)
    if stream_name not in headers_by_stream and batch_records:
        headers_by_stream[stream_name] = list(batch_records[0].keys())
        header_range = f"{stream_name}!A1:ZZZ1"
        bulk_append_to_sheet(service, spreadsheet['spreadsheetId'], header_range, [headers_by_stream[stream_name]])
        logger.info(f"Set headers for sheet '{stream_name}': {len(headers_by_stream[stream_name])} columns")
    
    # Check for new columns
    headers = headers_by_stream.get(stream_name, [])
    if batch_records and headers:
        new_records_columns = batch_records[0].keys()
        new_columns = [col for col in new_records_columns if col not in headers]
        if new_columns:
            new_headers = headers + new_columns
            headers_range = f"{stream_name}!A1:ZZZ1"
            update_to_sheet(service, spreadsheet['spreadsheetId'], headers_range, new_headers)
            headers_by_stream[stream_name] = new_headers
            
            # Update primary key indexes
            if key_properties.get(stream_name):
                pks = key_properties[stream_name]
                pk_indexes = get_pk_index(new_headers, pks)
                key_properties[stream_name + "_pk_index"] = pk_indexes
            
            logger.info(f"Added {len(new_columns)} new columns to sheet '{stream_name}'")
    
    # Process the batch
    headers = headers_by_stream.get(stream_name, [])
    existing_data = existing_data_by_stream.get(stream_name)
    
    process_record_batch(
        service=service,
        spreadsheet_id=spreadsheet['spreadsheetId'],
        stream_name=stream_name,
        batch_records=batch_records,
        headers=headers,
        key_properties=key_properties,
        existing_data=existing_data
    )


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
        logger.error("Either 'spreadsheet_id' or a list called 'files' with at least one element that contains the 'id' of the spreadsheet must be provided in the config")
        raise e
    spreadsheet = get_spreadsheet(service, spreadsheet_id)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = None
    batch_size = config.get('batch_size')
    if batch_size is not None:
        logger.info(f"Using batch size: {batch_size}")
    else:
        logger.info("Using dynamic batch sizes based on column count")
    
    log_memory_usage("Starting to process records")
    state = persist_lines(service, spreadsheet, input, batch_size)
    log_memory_usage("Finished processing records")
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
