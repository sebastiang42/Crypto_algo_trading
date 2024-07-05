# import http.client
# import json
# import time
# import os
# import pandas as pd
# from io import StringIO
# import numpy as np
from import_historical_data_functions import *


symbols_binance = list_symbols_binance()
symbols_coinbase = list_symbols_coinbase()


symbol = 'BTCUSDT'
interval = Client.KLINE_INTERVAL_1MINUTE
start_date = '2021-01-01'
end_date = '2021-01-31'

klines_binance = get_klines_subset_binance(symbol, interval, start_date, end_date)

klines_coinbase = get_klines_subset_coinbase(product='BTC-USD', start=int(time.time())-60*60*24*365*8, end=int(time.time())-60*60*24*365*8+60*600, granularity='ONE_MINUTE')

end_timestamp = int(time.time())
# start_timestamp = end_timestamp - (120 * 300 * 60)
start_timestamp = end_timestamp - (8 * 365 * 24 * 60 * 60) # 8 years

product = "BTC-USD"
granularity = "ONE_MINUTE"

historical_data = fetch_historical_data(product, granularity, start_timestamp, end_timestamp)

historical_data

help(historical_data['start'].is_monotonic_decreasing)
