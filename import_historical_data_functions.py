import jwt
from cryptography.hazmat.primitives import serialization
import time
import datetime
import http.client
import json
import pandas as pd
import os
from google.cloud import secretmanager
from binance.client import Client
import polars as pl

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

# request_path   = "/api/v3/brokerage/products/BTC-USD/candles"

def get_historical_klines_binance(symbol, interval, start_str, end_str=None):
    """
    Get historical kline data from Binance.
    
    :param symbol: The trading pair symbol (e.g., 'BTCUSDT')
    :param interval: The interval for klines (e.g., Client.KLINE_INTERVAL_1HOUR)
    :param start_str: The start date in 'yyyy-mm-dd' format
    :param end_str: The end date in 'yyyy-mm-dd' format (optional)
    :return: List of klines
    """
    klines = binance_client.get_historical_klines(symbol, interval, start_str, end_str)

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
        .select(["Open time", "Open", "High", "Low", "Close", "Volume"]) \
        .with_columns([
            pl.col("Open").cast(pl.Float64),
            pl.col("High").cast(pl.Float64),
            pl.col("Low").cast(pl.Float64),
            pl.col("Close").cast(pl.Float64),
            pl.col("Volume").cast(pl.Float64)
        ])
    return df_klines

def get_binance_symbols():
    """
    Retrieve all trading symbols available on Binance.
    
    :return: List of symbols
    """
    exchange_info = binance_client.get_exchange_info()
    symbols = [symbol['symbol'] for symbol in exchange_info['symbols']]
    return symbols

def build_jwt(service, uri):
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

def list_products():
    request_host   = "api.coinbase.com"
    request_path   = "/api/v3/brokerage/products"
    request_method = "GET"

    uri = f"{request_method} {request_host}{request_path}"
    jwt_token = build_jwt(service_name, uri)

    conn = http.client.HTTPSConnection(request_host)
    payload = ''
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {jwt_token}'
    }
    conn.request(request_method, request_path, payload, headers)
    res = conn.getresponse()
    data = res.read()

    products = pd.DataFrame(json.loads(data.decode("utf-8"))['products'])

    return products

def get_candles(product, start, end, granularity):
    request_host   = "api.coinbase.com"
    request_path_uri   = "/api/v3/brokerage/products/" + product + "/candles"
    request_path    = request_path_uri + f"?start={start}&end={end}&granularity={granularity}"
    request_method = "GET"

    uri = f"{request_method} {request_host}{request_path_uri}"
    jwt_token = build_jwt(service_name, uri)

    conn = http.client.HTTPSConnection(request_host)
    payload = ''
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {jwt_token}'
    }
    conn.request(request_method, request_path, payload, headers)
    res = conn.getresponse()
    data = res.read()

    candles = pd.DataFrame(json.loads(data.decode("utf-8"))['candles'])

    return candles

def fetch_historical_data(product, granularity, start_timestamp, end_timestamp, save_interval=60, print_interval=1):
    # Define the time interval for each API call (300 candles per call)
    interval = 300 * 60

    # Check if the partial CSV file exists
    if os.path.isfile('historical_data_partial.csv'):
        # Load the existing data
        historical_data = pd.read_csv('historical_data_partial.csv')

        # Find the last timestamp in the loaded data
        last_timestamp = historical_data['start'].max()

        # Set the start timestamp to the next interval after the last timestamp
        start_timestamp = last_timestamp + 60

        print(f"Resuming from timestamp: {last_timestamp}")

    else:
        # Initialize an empty dataframe if the partial CSV file doesn't exist
        historical_data = pd.DataFrame()

    total_intervals = (end_timestamp - start_timestamp) // interval
    progress_interval = int(total_intervals * print_interval / 100)

    start_time = time.time()

    # Loop through time intervals and make API calls
    interval_counter = 0
    current_start = start_timestamp
    while current_start < end_timestamp:
        current_end = min(current_start + interval, end_timestamp)

        # Make API call
        data = get_candles(product, current_start, current_end, granularity)

        # Concatenate the data to the main dataframe
        historical_data = pd.concat([data, historical_data], ignore_index=True)

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
