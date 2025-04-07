import os
import ccxt
import ccxt.pro
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from datafeed.motherfeeder import MotherFeeder


class KucoinMarket(MotherFeeder):
    def __init__(self, account=0, request_timeout=30000, *args, **kwargs):
        self.account = account  # needed by next step
        self.__API_KEY = ''
        self.__API_SEC = ''
        self.__PASS__ = ''
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        if self.account == 0:
            self.__API_KEY = os.environ['KUCOIN_TRADE_API_KEY']
            self.__API_SEC = os.environ['KUCOIN_TRADE_API_SECRET']
        elif self.account == 1:
            self.__API_KEY = os.environ['KUCOIN_PAIRSPREAD1_API_KEY']
            self.__API_SEC = os.environ['KUCOIN_PAIRSPREAD1_API_SECRET']
            self.__PASS__ = ''
        self.__DEFAULT_TIMEOUT = 30000
        self.__LIMIT = 960

    def _get_limit(self):
        return self.__LIMIT

    def build_end_point_async(self):
        return ccxt.pro.kucoin({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'password': self.__PASS__,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point(self):
        return ccxt.kucoin({
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

        return ohlcv_dataframe

    async def set_leverage_async(self, symbol, leverage):
        return

    def _dict_from_pos(self, positions):
        book = {}

        for line in positions:
            qty = float(line['info']['positionAmt'])
            price = float(line['info']['markPrice'])
            symbol = line['info']['symbol']

            if qty != 0:
                entry_price = line['entryPrice']
                dt = line['datetime']
                amount = qty * price
                book[symbol] = qty, amount, entry_price, datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
        return book


class KucoinFutureMarket(MotherFeeder):
    __DEFAULT_TIMEOUT = 30000
    __LIMIT = 960
    __MARKETS = ['ETH/BTC', 'ETH/USDT']

    def __init__(self, request_timeout=30000, account=0, *args, **kwargs):
        self.account = account
        self.__API_KEY = ''
        self.__API_SEC = ''
        self.__PASS__ = ''
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        if self.account == 0:
            self.__API_KEY = os.environ['KUCOINFUT_TRADE_API_KEY']
            self.__API_SEC = os.environ['KUCOINFUT_TRADE_API_SECRET']
        elif self.account == 1:
            self.__API_KEY = os.environ['KUCOIN_PAIRSPREAD1_API_KEY']
            self.__API_SEC = os.environ['KUCOIN_PAIRSPREAD1_API_SECRET']
        self.__LIMIT = 960

    def build_end_point(self):
        return ccxt.kucoinfutures({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'password': self.__PASS__,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future'
            },
        })

    def build_end_point_async(self):
        return ccxt.pro.kucoinfutures({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
        })


    def _arrange_data(self, ohlcv_dataframe):
        ohlcv_dataframe['date'] = ohlcv_dataframe[0]
        ohlcv_dataframe['open'] = ohlcv_dataframe[1]
        ohlcv_dataframe['high'] = ohlcv_dataframe[2]
        ohlcv_dataframe['low'] = ohlcv_dataframe[3]
        ohlcv_dataframe['close'] = ohlcv_dataframe[4]
        ohlcv_dataframe['volume'] = ohlcv_dataframe[5]
        ohlcv_dataframe = ohlcv_dataframe.set_index('date')
        # Change binance ms timestamp to date in UTC
        ohlcv_dataframe = ohlcv_dataframe.set_index(
            pd.to_datetime(ohlcv_dataframe.index, unit='ms').tz_localize('UTC'))
        ohlcv_dataframe.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)

        return ohlcv_dataframe

    def _get_limit(self):
        return self.__LIMIT

    def _dict_from_pos(self, positions):
        book = {}

        for line in positions:
            qty = float(line['info']['positionAmt'])
            price = float(line['info']['markPrice'])
            symbol = line['info']['symbol']

            if qty != 0:
                entry_price = line['entryPrice']
                dt = line['datetime']
                amount = qty * price
                book[symbol] = [qty, amount, entry_price, datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")]
        return book

    async def set_leverage_async(self, symbol, leverage):
        return


async def main():
    market = KucoinMarket(account=0)
    await market._exchange_async.close()
    ticker = 'BCHUSDTM'
    timeframe = '30m'
    end_time = None  # int(datetime.timestamp(datetime(2022, 12, 1)))
    start_time = int(datetime.timestamp(datetime(2024, 12, 1)))
    market = KucoinFutureMarket(account=0)
    data, done = market.read_bars(symbol=ticker, timeframe=timeframe, start_time=start_time, end_time=end_time)
    print(done)
    # balance = await market.get_cash_async(['USDT']) # besoin de passcode
    await market._exchange_async.close()


if __name__ == '__main__':
    asyncio.run(main())


