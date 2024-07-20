import jwt
from cryptography.hazmat.primitives import serialization
import time
from datetime import datetime, timezone, timedelta, UTC
import http.client
import json
import pandas as pd
import os
from google.cloud import secretmanager
from binance.client import Client
import polars as pl
import numpy as np


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

symbol_map = {
    'BTCUSD': {
        'binance': 'BTCUSDT',
        'coinbase': 'BTC-USD'
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
    :param start_date: The start date in 'yyyy-mm-dd' format
    :param end_date: The end date in 'yyyy-mm-dd' format (optional)
    :return: List of klines
    """
    interval_binance = interval_map[interval]['binance']
    klines = binance_client.get_historical_klines(symbol, interval_binance, start_date, end_date)

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
    
    df_klines = df_klines.slice(0, df_klines.shape[0] - 1)

    return df_klines

def get_klines_subset_coinbase(symbol, interval, start_date, end_date):
    """
    Get historical kline data from Coinbase. At most only 300 candles can 
    be retrieved per API call
    
    :param symbol: The trading pair symbol (e.g., 'BTCUSDT')
    :param interval: The interval for klines (e.g., Client.KLINE_INTERVAL_1HOUR)
    :param start_date: The start date in 'yyyy-mm-dd' format
    :param end_date: The end date in 'yyyy-mm-dd' format
    :return: List of klines
    """
    start_unix = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
    end_unix = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()) - 60
    interval_coinbase = interval_map[interval]['coinbase']

    request_host = "api.coinbase.com"
    request_path_uri = f"/api/v3/brokerage/market/products/{symbol}/candles"
    request_method = "GET"

    # uri = f"{request_method} {request_host}{request_path_uri}"
    # jwt_token = build_jwt_coinbase(service_name, uri)

    conn = http.client.HTTPSConnection(request_host)
    payload = ''
    headers = {
    'Content-Type': 'application/json'#,
    # 'Authorization': f'Bearer {jwt_token}'
    }

    sub_start_unix = start_unix

    df_klines = pl.DataFrame()

    while sub_start_unix <= end_unix:
        sub_end_unix = min(sub_start_unix + 60*299, end_unix)
        request_path = request_path_uri + f"?start={sub_start_unix}&end={sub_end_unix}&granularity={interval_coinbase}"
        conn.request(request_method, request_path, payload, headers)
        res = conn.getresponse()
        data = json.loads(res.read().decode("utf-8"))['candles']

        if len(data) > 0:
            df_klines = df_klines.vstack(pl.DataFrame(data) \
                .reverse()\
                .with_columns([
                    pl.col("open").cast(pl.Float64),
                    pl.col("high").cast(pl.Float64),
                    pl.col("low").cast(pl.Float64),
                    pl.col("close").cast(pl.Float64),
                    pl.col("volume").cast(pl.Float64),
                    pl.col("start").cast(pl.Int64)
                ]))
        
        sub_start_unix = sub_end_unix + 60

        if sub_start_unix <= end_unix:
            time.sleep(0.15)

    return df_klines

def get_klines_subset(symbol, interval, start_date, end_date, exchange):
    if exchange == 'coinbase':
        df_klines = get_klines_subset_coinbase(symbol, interval, start_date, end_date)
    elif exchange == 'binance':
        df_klines = get_klines_subset_binance(symbol, interval, start_date, end_date)
    return df_klines

def get_historical_klines(symbol, interval, start_date, end_date, exchange, step_size=5):
    symbol_exchange = symbol_map[symbol][exchange]
    partial_csv_name = f'historical_data/{exchange}_{symbol_exchange}_{interval}_data_partial.csv'
    complete_csv_name = f'historical_data/{exchange}_{symbol_exchange}_{interval}_data_raw.csv'

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    if os.path.isfile(partial_csv_name):
        last_timestamp = pl.read_csv(partial_csv_name, columns=['start'])['start'][-1]

        sub_start_datetime = datetime.fromtimestamp(last_timestamp, UTC) + timedelta(days=1)
        print(f"Resuming from: {sub_start_datetime.strftime('%Y-%m-%d')}")
    else:
        pl.DataFrame(schema=["start", "low", "high", "open", "close", "volume"])\
            .write_csv(partial_csv_name)
        
        sub_start_datetime = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        print(f"Starting from: {sub_start_datetime}")
    
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    total_steps = int(np.ceil((end_datetime - start_datetime) / timedelta(days=step_size)))
    completed_steps_this_run = 0

    start_time = time.time()

    with open(partial_csv_name, 'a') as f:
        while sub_start_datetime < end_datetime:
            sub_start_date = sub_start_datetime.strftime('%Y-%m-%d')
            sub_end_datetime = min(sub_start_datetime + timedelta(days=step_size), end_datetime)
            sub_end_date = sub_end_datetime.strftime('%Y-%m-%d')

            sub_df_klines = get_klines_subset(symbol_exchange, interval, sub_start_date, sub_end_date, exchange)

            sub_df_klines.write_csv(f, include_header=False)

            completed_steps = int(np.ceil((sub_end_datetime - start_datetime) / timedelta(days=step_size)))
            completed_steps_this_run += 1

            end_time = time.time()

            print(f"Step {completed_steps}/{total_steps} complete ({round(100*completed_steps/total_steps, 1)}%)." +
                  f" Estimated time remaining: {round((end_time - start_time) * ((total_steps / completed_steps_this_run) - 1), 1)} s")
            
            sub_start_datetime = sub_end_datetime
    
    if not os.path.isfile(complete_csv_name):
        os.rename(partial_csv_name, complete_csv_name)
        print(f"Data import complete: {exchange} | {symbol} {interval} from {start_date} to {end_date}")
    else:
        print("Data import complete, but raw data file already exists. Please overwrite manually")
