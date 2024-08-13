from data_loaders.market_symbol_loader import MarketSymbolLoader
from data_loaders.fmp_data_loader import FmpDataLoader
from config import *
from datetime import datetime, timedelta
import pandas as pd
import os


class CandidateFinder:
    def __init__(self, fmp_api_key):
        self.symbol_loader = MarketSymbolLoader()
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def filter_by_price_drop(self, prices_dict, price_drop_percent):
        filtered_prices_dict = {}
        for symbol, df in prices_dict.items():
            if not df.empty:
                first_price = df.iloc[0]['close']
                last_price = df.iloc[-1]['close']
                price_drop = ((first_price - last_price) / first_price) * 100
                df['price_drop'] = price_drop
                if price_drop <= price_drop_percent:
                    filtered_prices_dict[symbol] = df
        return filtered_prices_dict

    def filter_by_ceo_buys(self, insider_trades_dict):
        filtered_trades_dict = {}
        for symbol, df in insider_trades_dict.items():
            ceo_buys = df[((df['typeOfOwner'].str.contains('CEO', case=False)) |
                          (df['typeOfOwner'].str.contains('CFO', case=False)) |
                          (df['typeOfOwner'].str.contains('COO', case=False)) |
                          (df['typeOfOwner'].str.contains('director', case=False))) &
                          (df['transactionType'] == 'P-Purchase')]
            if not ceo_buys.empty:
                filtered_trades_dict[symbol] = ceo_buys
        return filtered_trades_dict

    def find_candidates(self):
        # Fetch market symbols
        symbols_df = self.symbol_loader.fetch_sp500_symbols(cache_file=True)
        symbol_list = symbols_df['symbol'].unique()

        # Fetch daily prices
        start_date = datetime.today() - timedelta(days=120)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list,
                                                                               start_date_str,
                                                                               end_date_str,
                                                                               cache_data=True,
                                                                               cache_dir=CACHE_DIR)

        # Filter by price drop
        filtered_prices_dict = self.filter_by_price_drop(prices_dict, PRICE_DROP_PERCENT)
        symbol_list = filtered_prices_dict.keys()

        # Fetch insider trades
        start_date = datetime.today() - timedelta(days=20)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        insider_trades_dict = self.fmp_data_loader.fetch_multiple_insider_trades_by_date(symbol_list,
                                                                                         start_date_str,
                                                                                         end_date_str,
                                                                                         cache_data=True,
                                                                                         cache_dir=CACHE_DIR)

        # Filter by CEO buys
        filtered_trades_dict = self.filter_by_ceo_buys(insider_trades_dict)

        # Convert insider_trades_dict to DataFrame
        candidates_list = []
        for symbol, df in filtered_trades_dict.items():
            df['symbol'] = symbol
            df['ownership_change'] = df['securitiesTransacted'] / df['securitiesOwned'] * 100

            # Add price drop
            if symbol in filtered_prices_dict:
                prices_df = filtered_prices_dict[symbol]
                price_drop = prices_df['price_drop'].iloc[-1]
                df['price_drop'] = price_drop

            candidates_list.append(df[['symbol', 'ownership_change', 'price_drop']])

        if len(candidates_list) == 0:
            print("No matching stocks found")
            return None

        candidates_df = pd.concat(candidates_list)

        # Group by symbol and calculate average ownership change and price drop
        candidates_df = candidates_df.groupby('symbol').agg({
            'ownership_change': 'mean',
            'price_drop': 'mean'
        }).reset_index()

        # Store results
        os.makedirs(RESULTS_DIR, exist_ok=True)
        path = os.path.join(RESULTS_DIR, "candidates.csv")
        candidates_df.to_csv(path)

        return candidates_df
