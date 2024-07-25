# import http.client
# import json
# import time
# import os
# import pandas as pd
# from io import StringIO
# import numpy as np
from import_historical_data_functions import *

#####################################################################
# list symbols (i.e., trading pairs) available at different exchanges
symbols_binance = list_symbols_binance()
symbols_coinbase = list_symbols_coinbase()

#####################################################################
# Pull smaller subsets of kline data from the different exchanges. 
# Useful for determining how far back data is available for a given exchange + symbol
interval = '1m'
start_date = '2021-07-01'
end_date = '2021-07-02'

klines_coinbase = get_klines_subset_coinbase(symbol='BTC-USD', interval=interval, start_date=start_date, end_date=end_date)

klines_binance = get_klines_subset_binance(symbol='SOLUSDT', interval=interval, start_date=start_date, end_date=end_date)

klines_bybit = get_klines_subset_bybit(symbol='SOLUSDT', interval=interval, start_date=start_date, end_date=end_date, category='linear')

klines_okx = get_klines_subset_okx(symbol='SOL-USDT', interval=interval, start_date=start_date, end_date=end_date)

klines_digifinex = get_klines_subset_digifinex(symbol='sol_usdt', interval=interval, start_date=start_date, end_date=end_date)

klines_bitget = get_klines_subset_bitget(symbol='SOLUSDT', interval=interval, start_date=start_date, end_date=end_date)

#####################################################################
# Download and save longer periods of data
# Start dates vary depending on how far back data is available for each exchange

interval = '1m'
symbol = 'BTCUSD'
get_historical_klines(symbol=symbol, interval=interval, start_date='2016-01-01', end_date='2024-07-01', exchange='coinbase')
get_historical_klines(symbol=symbol, interval=interval, start_date='2017-09-01', end_date='2024-07-01', exchange='binance')
get_historical_klines(symbol=symbol, interval=interval, start_date='2018-11-01', end_date='2024-07-01', exchange='bybit')
get_historical_klines(symbol=symbol, interval=interval, start_date='2020-02-01', end_date='2024-07-01', exchange='okx')
get_historical_klines(symbol=symbol, interval=interval, start_date='2019-08-01', end_date='2024-07-01', exchange='digifinex')
get_historical_klines(symbol=symbol, interval=interval, start_date='2018-08-01', end_date='2024-07-01', exchange='bitget')

symbol = 'ETHUSD'
get_historical_klines(symbol=symbol, interval=interval, start_date='2016-06-01', end_date='2024-07-01', exchange='coinbase')
get_historical_klines(symbol=symbol, interval=interval, start_date='2017-09-01', end_date='2024-07-01', exchange='binance')
get_historical_klines(symbol=symbol, interval=interval, start_date='2019-02-01', end_date='2024-07-01', exchange='bybit')
get_historical_klines(symbol=symbol, interval=interval, start_date='2020-02-01', end_date='2024-07-01', exchange='okx')
get_historical_klines(symbol=symbol, interval=interval, start_date='2019-08-01', end_date='2024-07-01', exchange='digifinex')
get_historical_klines(symbol=symbol, interval=interval, start_date='2018-08-01', end_date='2024-07-01', exchange='bitget')

symbol = 'XRPUSD'
get_historical_klines(symbol=symbol, interval=interval, start_date='2023-08-01', end_date='2024-07-01', exchange='coinbase')
get_historical_klines(symbol=symbol, interval=interval, start_date='2018-07-01', end_date='2024-07-01', exchange='binance')
get_historical_klines(symbol=symbol, interval=interval, start_date='2019-07-01', end_date='2024-07-01', exchange='bybit')
get_historical_klines(symbol=symbol, interval=interval, start_date='2020-02-01', end_date='2024-07-01', exchange='okx')
get_historical_klines(symbol=symbol, interval=interval, start_date='2019-08-01', end_date='2024-07-01', exchange='digifinex')
get_historical_klines(symbol=symbol, interval=interval, start_date='2019-06-01', end_date='2024-07-01', exchange='bitget')

symbol = 'SOLUSD'
get_historical_klines(symbol=symbol, interval=interval, start_date='2021-07-01', end_date='2024-07-01', exchange='coinbase')
get_historical_klines(symbol=symbol, interval=interval, start_date='2020-09-01', end_date='2024-07-01', exchange='binance')
get_historical_klines(symbol=symbol, interval=interval, start_date='2021-11-01', end_date='2024-07-01', exchange='bybit')
get_historical_klines(symbol=symbol, interval=interval, start_date='2021-02-01', end_date='2024-07-01', exchange='okx')
get_historical_klines(symbol=symbol, interval=interval, start_date='2021-06-01', end_date='2024-07-01', exchange='digifinex')
get_historical_klines(symbol=symbol, interval=interval, start_date='2021-07-01', end_date='2024-07-01', exchange='bitget')
