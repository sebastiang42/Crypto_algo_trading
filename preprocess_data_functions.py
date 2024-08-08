import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import os
import glob
from exchange_maps import *


def impute_missing_data(historical_data):
    # Remove any duplicate rows
    historical_data = historical_data.unique(subset=['start']).sort(by='start')

    # Generate a range of all possible timestamps from min to max with a step of 60 seconds
    min_timestamp = historical_data['start'].min()
    max_timestamp = historical_data['start'].max()
    all_timestamps = pl.DataFrame({
        'start': range(min_timestamp, max_timestamp + 1, 60)
    })

    # Find the missing timestamps
    missing_timestamps = all_timestamps.join(historical_data.select('start'), on='start', how='anti')

    # Create a DataFrame with missing timestamps and default values
    missing_data = missing_timestamps.with_columns([
        pl.lit(None).cast(pl.Float64).alias('low'),
        pl.lit(None).cast(pl.Float64).alias('high'),
        pl.lit(None).cast(pl.Float64).alias('open'),
        pl.lit(None).cast(pl.Float64).alias('close'),
        pl.lit(0).cast(pl.Float64).alias('volume')
    ])

    # Concatenate the original data with the missing data
    complete_data = pl.concat([historical_data, missing_data]).sort(by='start')

    # Forward-fill 'close' values
    complete_data = complete_data.with_columns(
        pl.col('close').forward_fill().alias('close')
    )

    # Fill missing 'open', 'low', 'high' with 'close'
    complete_data = complete_data.with_columns([
        pl.col('open').fill_null(pl.col('close')),
        pl.col('low').fill_null(pl.col('close')),
        pl.col('high').fill_null(pl.col('close'))
    ])

    return complete_data

def preprocess_data(data_folder):
    raw_data_files = [os.path.basename(file) for file in glob.glob(os.path.join(data_folder, '*raw.csv'))]

    for raw_file in raw_data_files:
        complete_file = raw_file.replace("raw", "complete")

        if os.path.isfile(os.path.join(data_folder, complete_file)):
            print(f"The file {complete_file} already exists.")
            continue

        raw_historical_data = pl.read_csv(os.path.join(data_folder, raw_file))

        if raw_historical_data['volume'].dtype != pl.Float64:
            raw_historical_data = raw_historical_data.with_columns(raw_historical_data['volume'].cast(pl.Float64).alias('volume'))

        complete_data = impute_missing_data(raw_historical_data)

        complete_data.write_csv(complete_file, include_header=False)

def plot_data(data, y_column):
    """
    Plot the data with 'start' column on the x-axis and the specified column on the y-axis.

    Parameters:
    - data (pd.DataFrame): The DataFrame containing the historical data.
    - y_column (str): The column name to plot on the y-axis.
    """
    plt.figure(figsize=(10, 6))
    plt.plot(data['start'], data[y_column], label=y_column)
    plt.xlabel('Timestamp')
    plt.ylabel(y_column)
    plt.title(f'Plot of {y_column} over time')
    plt.legend()
    plt.show()