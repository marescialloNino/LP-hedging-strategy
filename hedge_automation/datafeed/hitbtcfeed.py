import os
import ccxt
import ccxt.pro
import pandas as pd
from datafeed.motherfeeder import MotherFeeder


class HitbtcMarket(MotherFeeder):
    def __init__(self, request_timeout=30000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_timeout = request_timeout

    def _init_API(self):
        self.__API_KEY = os.environ['HITBTC_TRADE_API_KEY']
        self.__API_SEC = os.environ['HITBTC_TRADE_API_SECRET']
        self.__LIMIT = 960

    def _get_limit(self):
        return self.__LIMIT

    def build_end_point(self):
        return ccxt.hitbtc({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point_async(self):
        return ccxt.pro.hitbtc({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })
    def _arrange_data(self, ohlcv_dataframe):
        ohlcv_dataframe['date'] = ohlcv_dataframe[0]
        ohlcv_dataframe['open'] = ohlcv_dataframe[1]
        ohlcv_dataframe['high'] = ohlcv_dataframe[2]
        ohlcv_dataframe['low'] = ohlcv_dataframe[3]
        ohlcv_dataframe['close'] = ohlcv_dataframe[4]
        ohlcv_dataframe['volume'] = ohlcv_dataframe[5]
        ohlcv_dataframe = ohlcv_dataframe.set_index('date')
        # Change ms timestamp to date in UTC
        ohlcv_dataframe = ohlcv_dataframe.set_index(
            pd.to_datetime(ohlcv_dataframe.index, unit='ms').tz_localize('UTC'))
        ohlcv_dataframe.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)
        ohlcv_dataframe = ohlcv_dataframe.loc[~ohlcv_dataframe.index.duplicated(keep='last')]

        return ohlcv_dataframe

