import os
import ccxt
import ccxt.pro
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from datafeed.motherfeeder import MotherFeeder


class HuobiMarket(MotherFeeder):
    def __init__(self, account=0, request_timeout=30000, *args, **kwargs):
        self.account = account  # needed by next step
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        self.__API_KEY = '' #os.environ['HUOBI_READ_API_KEY']
        self.__API_SEC = '' # os.environ['HUOBI_READ_API_SECRET']
        self.__LIMIT = 100

    def build_end_point_async(self):
        return ccxt.pro.huobi({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point(self):
        return ccxt.huobi({
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

    def _get_limit(self):
        return self.__LIMIT

    async def set_leverage_async(self, symbol, leverage):
        rez = await self._exchange_async.set_leverage(leverage, symbol, {'marginMode': 'cross'})
        return

    def _dict_from_pos(self, positions):
        book = {}

        for line in positions:
            qty = float(line['info']['pos']) * float(line['contractSize'])
            price = float(line['info']['markPx'])
            symbol = line['info']['instId']

            if qty != 0:
                entry_price = line['entryPrice']
                dt = line['datetime']
                amount = qty * price
                book[symbol] = qty, amount, entry_price, datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
        return book


async def main():
    ticker = 'APT-USDT'
    timeframe = '30m'
    end_time = None  # int(datetime.timestamp(datetime(2022, 12, 1)))
    start_time = int(datetime.timestamp(datetime(2022, 9, 15)))
    market = HuobiMarket(account=0)
    data, done = market.read_bars(symbol=ticker, timeframe=timeframe, start_time = start_time, end_time=end_time)
    print(done)
    await market._exchange_async.set_leverage(5, 'BTCUSDT')
    balance = await market._exchange_async.fetch_balance()
    await market._exchange_async.close()
    if balance is not None and 'free' in balance:
        print(balance['free'])

if __name__ == '__main__':
    asyncio.run(main())


