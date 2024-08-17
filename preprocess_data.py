import polars as pl
from preprocess_data_functions import *

raw_data_folder = 'historical_data'
preprocessed_data_folder = 'preprocessed_data'

# Preprocess all data in raw_data_folder and output to preprocessed_data_folder
preprocess_data(raw_data_folder, preprocessed_data_folder)


# View preprocessed data
symbol = 'BTCUSD'
interval = '1m'
exchange = 'coinbase'

complete_file = f'preprocessed_data/{exchange}_{symbol_map[symbol][exchange]}_{interval}_data_complete.csv'

historical_data = pl.read_csv(complete_file)
plot_data(historical_data, 'close_pct_change')
