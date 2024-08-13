import os
import requests
import pandas as pd
from utils.df_utils import standardize_ohlcv_dataframe


class FmpDataLoader:
    """
    FmpDataLoader provides methods to interact with the Financial Modeling Prep (FMP) API to fetch various financial data.

    Attributes:
        api_key (str): FMP API key.
    """

    def __init__(self, api_key: str):
        """
        Initializes the FmpDataLoader with the given API key.

        Parameters:
            api_key (str): FMP API key.
        """
        self._api_key = api_key


    def fetch_daily_prices_by_date(self, symbol: str, start_date_str: str, end_date_str: str,
                                   cache_data: bool = False, cache_dir: str = "cache") -> pd.DataFrame:
        """
        Fetches daily prices by date from the FMP API.

        Parameters:
            symbol (str): Stock symbol.
            start_date_str (str): Start date in 'YYYY-MM-DD' format.
            end_date_str (str): End date in 'YYYY-MM-DD' format.
            cache_data (bool): Flag to specify if data should be cached
            cache_dir (str): Directory to cache the data.

        Returns:
            pd.DataFrame: DataFrame with daily prices.
        """

        file_name = f"{symbol}-{start_date_str}-{end_date_str}-prices.csv"
        path = os.path.join(cache_dir, file_name)
        if cache_data is True:
            if os.path.exists(path) is True:
                prices_df = pd.read_csv(path)
                prices_df['date'] = pd.to_datetime(prices_df['date'])
                prices_df.set_index('date', inplace=True)
                return prices_df

        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start_date_str}&to={end_date_str}&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                historical_data = data.get('historical', [])
                if historical_data:
                    prices_df = pd.DataFrame(historical_data)
                    prices_df = standardize_ohlcv_dataframe(prices_df)
                    prices_df['date'] = pd.to_datetime(prices_df['date'])
                    prices_df.set_index('date', inplace=True)
                    prices_df.sort_values(by=['date'], ascending=True, inplace=True)

                    if cache_data is True:
                        os.makedirs(cache_dir, exist_ok=True)
                        prices_df.to_csv(path)
                    return prices_df
                else:
                    return None
            else:
                print(f"Failed to fetch prices. Error: {response.reason}")
                return None
        except Exception as ex:
            print(ex)
            return None

    def fetch_multiple_daily_prices_by_date(self, symbol_list: list, start_date_str: str, end_date_str: str, cache_data: bool = False, cache_dir: str = "cache") -> dict:
        """
        Fetches daily prices by date for multiple symbols from the FMP API.

        Parameters:
            symbol_list (list): List of stock symbols.
            start_date_str (str): Start date in 'YYYY-MM-DD' format.
            end_date_str (str): End date in 'YYYY-MM-DD' format.
            cache_data (bool): Flag to specify if data should be cached
            cache_dir (str): Directory to cache the data.

        Returns:
            dict: A dictionary with symbols as keys and DataFrames with daily prices as values.
        """
        results = {}
        for symbol in symbol_list:
            print(f"Now fetching price data for {symbol}...")
            df = self.fetch_daily_prices_by_date(symbol, start_date_str, end_date_str, cache_data, cache_dir)
            if df is not None:
                results[symbol] = df
            else:
                print(f"Failed to fetch data for {symbol}")
        return results

    def fetch_insider_trades(self, symbol: str, from_date_str: str, to_date_str: str, cache_data: bool = False,
                             cache_dir: str = "cache") -> pd.DataFrame:
        """
        Fetches insider trades from the FMP API.

        Parameters:
            symbol (str): Stock symbol.
            from_date_str (str): Start date in 'YYYY-MM-DD' format.
            to_date_str (str): End date in 'YYYY-MM-DD' format.
            cache_data (bool): Flag to specify if data should be cached.
            cache_dir (str): Directory to cache the data.

        Returns:
            pd.DataFrame: DataFrame with insider trades.
        """

        file_name = f"{symbol}-insider-trades-{from_date_str}-to-{to_date_str}.csv"
        path = os.path.join(cache_dir, file_name)
        if cache_data is True:
            if os.path.exists(path) is True:
                trades_df = pd.read_csv(path)
                trades_df['transactionDate'] = pd.to_datetime(trades_df['transactionDate'])
                trades_df.set_index('transactionDate', inplace=True)
                trades_df = trades_df[(trades_df.index >= from_date_str) & (trades_df.index <= to_date_str)]
                return trades_df

        try:
            url = f"https://financialmodelingprep.com/api/v4/insider-trading?symbol={symbol}&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    trades_df = pd.DataFrame(data)
                    trades_df['transactionDate'] = pd.to_datetime(trades_df['transactionDate'])
                    trades_df.set_index('transactionDate', inplace=True)
                    trades_df.sort_values(by=['transactionDate'], ascending=True, inplace=True)

                    # Filter by date range
                    trades_df = trades_df[(trades_df.index >= from_date_str) & (trades_df.index <= to_date_str)]

                    if cache_data is True:
                        os.makedirs(cache_dir, exist_ok=True)
                        trades_df.to_csv(path)
                    return trades_df
                else:
                    return None
            else:
                print(f"Failed to fetch insider trades. Error: {response.reason}")
                return None
        except Exception as ex:
            print(ex)
            return None

    def fetch_multiple_insider_trades_by_date(self, symbol_list: list, start_date_str: str, end_date_str: str, cache_data: bool = False, cache_dir: str = "cache") -> dict:
        """
        Fetches daily prices by date for multiple insider trades from the FMP API.

        Parameters:
            symbol_list (list): List of stock symbols.
            start_date_str (str): Start date in 'YYYY-MM-DD' format.
            end_date_str (str): End date in 'YYYY-MM-DD' format.
            cache_data (bool): Flag to specify if data should be cached
            cache_dir (str): Directory to cache the data.

        Returns:
            dict: A dictionary with symbols as keys and DataFrames with daily prices as values.
        """
        results = {}
        for symbol in symbol_list:
            print(f"Now fetching insider trades data for {symbol}...")
            df = self.fetch_insider_trades(symbol, start_date_str, end_date_str, cache_data, cache_dir)
            if df is not None:
                results[symbol] = df
            else:
                print(f"Failed to fetch insider trades data for {symbol}")
        return results