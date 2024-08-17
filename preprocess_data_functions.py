import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import os
import glob
from exchange_maps import *


def impute_missing_data(historical_data):
    '''
    Impute missing rows. Imputed rows will have 0 volume, and the LHOC 
    values will all be the close value of the most recent "real" row.
    '''
    # Remove any duplicate rows (They should not be present, but this is a safeguard)
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

def remove_outliers(historical_data, max_change = 0.15):
    '''
    Detect outliers by looking for too-large percent changes from one candle to the next. 
    a point is only removed if it corresponds to a large jump followed by a large drop 
    (or vice versa). A lone jump or drop will be ignored. This algorithm is definitely 
    not perfect, but it works well enough for the data I've downloaded, so I'm leaving it 
    as-is. For others it would be worthwhile to check the results for a specific dataset.
    '''
    # Remove pct increases/decreases greater than max_change
    outliers = historical_data.with_columns(
        pl.col("close").pct_change().alias("close_pct_change")
    ).filter(
        (pl.col("close_pct_change") > max_change) | (pl.col("close_pct_change") < -max_change)
    )

    while outliers.height > 1:
        first_two_prod = (outliers['close_pct_change'][0] + 1) * (outliers['close_pct_change'][1] + 1) - 1
        if first_two_prod < max(outliers['close_pct_change'][0:2]) and first_two_prod > min(outliers['close_pct_change'][0:2]):
            historical_data = historical_data.filter(
                pl.col('start') != outliers['start'][0]
            )

            outliers = historical_data.with_columns(
                pl.col("close").pct_change().alias("close_pct_change")
            ).filter(
                (pl.col("close_pct_change") > max_change) | (pl.col("close_pct_change") < -max_change)
            )
        else:
            outliers = outliers.tail(outliers.height - 1)

    return historical_data

def preprocess_data(raw_data_folder, preprocessed_data_folder):
    raw_data_files = [os.path.basename(file) for file in glob.glob(os.path.join(raw_data_folder, '*raw.csv'))]

    for raw_file in raw_data_files:
        complete_file = raw_file.replace("raw", "complete")

        if os.path.isfile(os.path.join(preprocessed_data_folder, complete_file)):
            print(f"The file {complete_file} already exists.")
            continue

        raw_historical_data = pl.read_csv(os.path.join(raw_data_folder, raw_file))

        if raw_historical_data['volume'].dtype != pl.Float64:
            raw_historical_data = raw_historical_data.with_columns(raw_historical_data['volume'].cast(pl.Float64).alias('volume'))

        # Remove outliers
        complete_data = remove_outliers(raw_historical_data)

        # Impute missing rows
        complete_data = impute_missing_data(complete_data)

        # Add a few more columns that might be useful. (Might as well do it once here 
        # rather than once for every future strategy to be tested)
        complete_data = complete_data.with_columns(
            pl.col("close").pct_change().alias("close_pct_change"),
            pl.col("close").diff().alias("close_diff"),
            (pl.col("high") - pl.col("low")).alias("low_high_diff"),
            (pl.col("high") - pl.col("close")).alias("close_high_diff"),
            (pl.col("close") - pl.col("low")).alias("low_close_diff")
        ).with_columns(
            (pl.col("low_high_diff") / pl.col("close")).alias("low_high_diff_pct"),
            (pl.col("close_high_diff") / pl.col("close")).alias("close_high_diff_pct"),
            (pl.col("low_close_diff") / pl.col("close")).alias("low_close_diff_pct")
        )

        complete_data.write_csv(os.path.join(preprocessed_data_folder, complete_file))

def plot_data(data, y_column, file, yscale='linear'):
    """
    Plot the data with 'start' column on the x-axis and the specified column on the y-axis.

    Parameters:
    - data (pd.DataFrame): The DataFrame containing the historical data.
    - y_column (str): The column name to plot on the y-axis.
    """
    plt.figure(figsize=(16, 10))
    plt.plot(data['start'], data[y_column], label=y_column)
    plt.xlabel('Timestamp')
    plt.ylabel(y_column)
    plt.title(f'{file} {y_column} over time')
    plt.yscale(yscale)
    plt.legend()
    plt.show()