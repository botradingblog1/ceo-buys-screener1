import pandas as pd
import numpy as np
import os

"""
Dataframe utils
"""


def standardize_ohlcv_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes an OHLCV dataframe by renaming columns, handling infinites, dropping NaNs, and converting date column to datetime.

    Parameters:
        df (pd.DataFrame): The OHLCV data frame.

    Returns:
        pd.DataFrame: The standardized data frame.
    """
    # Define a mapping for renaming columns
    rename_mapping = {
        'Date': 'date',
        'Datetime': 'date',
        'DateTime': 'date',
        'Time': 'time',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'AdjClose': 'adj_close',
        'Adj Close': 'adj_close',
        'adjclose': 'adj_close',
        'Volume': 'volume'
    }

    # Rename columns
    df = df.rename(columns=str.lower)
    df = df.rename(columns=rename_mapping)

    # Convert date column to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Replace infinite values with NaN
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Drop rows with NaN values
    df.dropna(inplace=True)

    # Forward fill
    df.ffill(inplace=True)

    # Ensure numeric columns have a numeric format
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = pd.to_numeric(df[column], errors='coerce')

    return df