import jwt
from cryptography.hazmat.primitives import serialization
import time
from datetime import datetime, timezone
import http.client
import json
import pandas as pd
import os
from google.cloud import secretmanager
from binance.client import Client
import polars as pl


#------------------------------------------------------------
# Load secrets

def get_gcp_secret(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=f"projects/{project_id}/secrets/{secret_id}/versions/latest")
    return response.payload.data.decode("UTF-8")

gcp_project_id = os.getenv('gcp_crypto_project')
binance_api_key = get_gcp_secret(gcp_project_id, 'binance-api-key')
binance_api_secret = get_gcp_secret(gcp_project_id, 'binance-api-secret')
coinbase_api_key = get_gcp_secret(gcp_project_id, 'coinbase-api-key')
coinbase_api_secret = get_gcp_secret(gcp_project_id, 'coinbase-api-secret').replace("\\n", "\n") #needed for Coinbase secrets

service_name   = "retail_rest_api_proxy"

binance_client = Client(binance_api_key, binance_api_secret)

#------------------------------------------------------------
# Exchange mappings

interval_map = {
    '1m': {
        'binance': '1m',
        'coinbase': 'ONE_MINUTE'
    },
    '5m': {
        'binance': '5m',
        'coinbase': 'FIVE_MINUTE'
    },
    '15m': {
        'binance': '15m',
        'coinbase': 'FIFTEEN_MINUTE'
    },
    '30m': {
        'binance': '30m',
        'coinbase': 'THIRTY_MINUTE'
    },
    '1h': {
        'binance': '1h',
        'coinbase': 'ONE_HOUR'
    },
    '6h': {
        'binance': '6h',
        'coinbase': 'SIX_HOUR'
    },
    '1d': {
        'binance': '1d',
        'coinbase': 'ONE_DAY'
    }
}

#------------------------------------------------------------
# Various support functions

def build_jwt_coinbase(service, uri):
    private_key_bytes = coinbase_api_secret.encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)
    jwt_payload = {
        'sub': coinbase_api_key,
        'iss': "coinbase-cloud",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 60,
        'aud': [service],
        'uri': uri,
    }
    jwt_token = jwt.encode(
        jwt_payload,
        private_key,
        algorithm='ES256',
        headers={'kid': coinbase_api_key, 'nonce': str(int(time.time()))},
    )
    return jwt_token

#------------------------------------------------------------
# List symbols

def list_symbols_binance():
    """
    Retrieve all trading symbols available on Binance.
    
    :return: List of symbols
    """
    exchange_info = binance_client.get_exchange_info()
    symbols = [symbol['symbol'] for symbol in exchange_info['symbols']]
    return symbols

def list_symbols_coinbase():
    """
    Retrieve all trading symbols available on Coinbase.
    
    :return: List of symbols
    """
    request_host   = "api.coinbase.com"
    request_path   = "/api/v3/brokerage/products"
    request_method = "GET"

    uri = f"{request_method} {request_host}{request_path}"
    jwt_token = build_jwt_coinbase(service_name, uri)

    conn = http.client.HTTPSConnection(request_host)
    payload = ''
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {jwt_token}'
    }
    conn.request(request_method, request_path, payload, headers)
    res = conn.getresponse()
    data = res.read()

    symbols = pl.DataFrame(json.loads(data.decode("utf-8"))['products'])["product_id"].to_list()

    return symbols

#------------------------------------------------------------
# Download historical klines

def get_klines_subset_binance(symbol, interval, start_date, end_date=None):
    """
    Get historical kline data from Binance. This one doesn't seem to have a 
    limit on the number of candles you can request.
    
    :param symbol: The trading pair symbol (e.g., 'BTCUSDT')
    :param interval: The interval for klines (e.g., Client.KLINE_INTERVAL_1HOUR)
    :param start_str: The start date in 'yyyy-mm-dd' format
    :param end_str: The end date in 'yyyy-mm-dd' format (optional)
    :return: List of klines
    """
    klines = binance_client.get_historical_klines(symbol, interval, start_date, end_date)

    column_names = [
        "Open time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
        "ignore"
        ]

    # Create the DataFrame
    df_klines = pl.DataFrame(klines, schema=column_names) \
        .select(["Open time", "Low", "High", "Open", "Close", "Volume"]) \
        .with_columns([
            pl.col("Open").cast(pl.Float64),
            pl.col("High").cast(pl.Float64),
            pl.col("Low").cast(pl.Float64),
            pl.col("Close").cast(pl.Float64),
            pl.col("Volume").cast(pl.Float64)
        ]) \
        .rename({
            "Open time": "start",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }) \
        .with_columns((pl.col("start") / 1000).alias("start").cast(pl.Int64))
    return df_klines

def get_klines_subset_coinbase(symbol, interval, start_unix, end_unix):
    """
    Get historical kline data from Coinbase. At most only 300 candles can 
    be retrieved per API call
    
    :param symbol: The trading pair symbol (e.g., 'BTCUSDT')
    :param interval: The interval for klines (e.g., Client.KLINE_INTERVAL_1HOUR)
    :param start_unix: The start time in UNIX format
    :param end_unix: The end time in UNIX format
    :return: List of klines
    """
    request_host   = "api.coinbase.com"
    request_path_uri   = f"/api/v3/brokerage/products/{symbol}/candles"
    request_path    = request_path_uri + f"?start={start_unix}&end={end_unix}&granularity={interval}"
    request_method = "GET"

    uri = f"{request_method} {request_host}{request_path_uri}"
    jwt_token = build_jwt_coinbase(service_name, uri)

    conn = http.client.HTTPSConnection(request_host)
    payload = ''
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {jwt_token}'
    }
    conn.request(request_method, request_path, payload, headers)
    res = conn.getresponse()
    data = res.read()

    df_klines = pl.DataFrame(json.loads(data.decode("utf-8"))['candles']) \
        .reverse()\
        .with_columns([
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("start").cast(pl.Int64)
        ])

    return df_klines

def get_historical_klines(symbol, granularity, start_timestamp, end_timestamp, exchange, save_interval=60, print_interval=1):
    # Define the time interval for each API call (300 candles per call)
    interval = 300 * 60

    # Check if the partial CSV file exists
    if os.path.isfile('historical_data/historical_data_partial.csv'):
        # Load the existing data
        historical_data = pl.read_csv('historical_data/historical_data_partial.csv')

        # Find the last timestamp in the loaded data
        last_timestamp = historical_data['start'].max()

        # Set the start timestamp to the next interval after the last timestamp
        start_timestamp = last_timestamp + 60

        print(f"Resuming from timestamp: {last_timestamp}")

    else:
        # Initialize an empty dataframe if the partial CSV file doesn't exist
        historical_data = pl.DataFrame()

    total_intervals = (end_timestamp - start_timestamp) // interval
    progress_interval = int(total_intervals * print_interval / 100)

    start_time = time.time()

    # Loop through time intervals and make API calls
    interval_counter = 0
    current_start = start_timestamp
    while current_start < end_timestamp:
        current_end = min(current_start + interval, end_timestamp)

        # Make API call
        data = get_historical_klines_coinbase(symbol, current_start, current_end, granularity)

        # Concatenate the data to the main dataframe
        historical_data = pl.concat([data, historical_data], ignore_index=True)

        # Update the start timestamp for the next iteration
        current_start = current_end

        interval_counter += 1

        # Save the dataframe at regular intervals
        if interval_counter % save_interval == 0:
            historical_data.to_csv('historical_data_partial.csv', index=False)
        
        if interval_counter % progress_interval == 0:
            completion_percentage = (interval_counter / total_intervals) * 100
            elapsed_time = time.time() - start_time
            print(f"{completion_percentage:.2f}% complete | Elapsed Time: {elapsed_time:.2f} seconds")

        # Sleep to avoid API rate limits (adjust as needed)
        time.sleep(0.5)
    
    historical_data.to_csv('historical_data.csv', index=False)

    return historical_data
