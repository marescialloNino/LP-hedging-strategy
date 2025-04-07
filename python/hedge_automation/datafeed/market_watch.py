import sys
import asyncio
import datetime
import logging
import math
from collections import deque

import numpy as np
from asyncio import run, create_task
import traceback
import pandas as pd

from datafeed.okexfeed import OkexMarket
from datafeed.bitgetfeed import BitgetMarket
from datafeed.binancefeed import BinanceFutureMarket, BinanceMarket
from datafeed.utils_online import today_utc


class MarketWatch:
    def __init__(self, exchange, tf, event_queue=None):
        self.exchange = exchange
        self.timeframe = tf
        self.tf_ms = self.exchange.parse_timeframe(tf) * 1000
        self.bars = {}
        self.bars_df = pd.DataFrame()
        self.last = {'last_modified': pd.Timestamp.min.tz_localize('UTC')}
        self._ticker_tasks = dict()
        self._lob_tasks = dict()
        self._running = dict()
        self.limit = 1000
        self.lob_limit = {'Bybit':1, 'Binance':10}  # too many issues with OKX and Bitget orderbook_watch
        self.logger = logging.getLogger('market_watch')
        self.errors = deque([], maxlen=10)
        self.event_queue = event_queue

    def get_bidasklast(self, symbol):
        data = self.last.get(symbol, {})
        bid = data.get('bid', None)
        ask = data.get('ask', None)
        last = data.get('last', None)
        return bid, ask, last

    def get_age_last(self):
        return (today_utc() - self.last['last_modified']).total_seconds()

    def get_age_data(self, symbol):
        data = self.last.get(symbol, {})
        now_ts = today_utc().timestamp()
        result = {'last': now_ts - data.get('timestamp', 0),
                  'lob': now_ts - data.get('lob_ts', 0)}
        return result

    async def run_watcher(self, symbols):
        async def rerun_forever(coro, *args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    msg = traceback.format_exc()
                    self.errors.append(msg)
                    self.logger.warning(f'listener said {msg}')
                    timing = 30 + np.random.uniform(0, 5)
                    await asyncio.sleep(timing)

        def end_ticker_task(task):
            symbol = self._ticker_tasks.get(task, None)
            if symbol is not None:
                self._running[symbol] = False

        def build_histo_from_ticker(symbol, ticker):
            keep = ['last', 'bid', 'ask', 'timestamp']
            new_bar = False
            if ticker is not None:
                current = self.last.get(symbol, {})
                for key, value in ticker.items():
                    if key in keep and value is not None:
                        current.update({key: value})
                self.last.update({symbol: current})
                if 'timestamp' in ticker:
                    ts = ticker['timestamp']
                    last_date = pd.Timestamp(ts_input=ts * 1e6, tz='UTC')
                else:
                    last_date = pd.Timestamp(ts_input=self.exchange.milliseconds() * 1e6, tz='UTC')
                    ts = last_date.timestamp()
                self.last['last_modified'] = last_date
                opening_time = int(math.floor(ts / self.tf_ms)) * self.tf_ms
                closing_time = int(math.ceil(ts / self.tf_ms)) * self.tf_ms
                bar_date = pd.Timestamp(ts_input=opening_time * 1e6, tz='UTC')

                if bar_date not in self.bars_df.index:
                    bar_closingdate = pd.Timestamp(ts_input=closing_time * 1e6, tz='UTC')
                    self.bars_df.loc[bar_date, 'closing_time'] = bar_closingdate
                    new_bar = True
                if symbol not in self.bars_df.columns:
                    new_data = pd.Series(index=self.bars_df.index, dtype=float, name=symbol)
                    self.bars_df = pd.concat([self.bars_df, new_data], axis=1)
                last_quote = current.get('last', np.nan)
                self.bars_df.loc[bar_date, symbol] = last_quote
            return new_bar

        def update_lob(symbol, data):
            if data is not None:
                current = self.last.get(symbol, {})
                ts = data.get('timestamp', None)
                if ts is None:
                    ts = self.exchange.milliseconds()
                current['lob_ts'] = int(ts)
                bids = data.get('bids', [])
                asks = data.get('asks', [])
                if len(bids) > 0:
                    current['bid'] = bids[0][0]
                    if len(asks) > 0:
                        current['ask'] = asks[0][0]
                        current['mid'] = 0.5 * (current['ask'] + current['bid'])
                self.last.update({symbol: current})

        async def watch_lob_loop(exchange, symbol, limit):
            method = 'watchOrderBook'
            if method not in exchange.has or not exchange.has[method]:
                return
            while True:
                data = await exchange.watchOrderBook(symbol, limit=limit)  # 10 bin, 1 bybit
                update_lob(symbol, data)

        async def watch_ticker_loop(exchange, symbol):
            method = 'watchTicker'
            self.bars[symbol] = []
            self._running[symbol] = True
            if method not in exchange.has or not exchange.has[method]:
                return
            while True:
                ticker = await exchange.watch_ticker(symbol)
                new_bar = build_histo_from_ticker(symbol, ticker)
                if new_bar and self.event_queue is not None:
                    if len(self.bars_df) > 1:
                        # event = MarketEvent(time=self.last['last_modified'], index=self.bars_df.index[-2],
                        #                     size=1, period=self.timeframe)  # in case the client needs an event when bar is full
                        await self.event_queue.put('Beep')

        async def watch_ohlcv(exchange, symbol):
            method = 'watchTrades'
            if method not in exchange.has or not exchange.has[method]:
                return
            since = exchange.milliseconds() - 30 * 1000  # last hour
            collected_trades = []
            self.bars[symbol] = []

            while self._running.get(symbol, False):
                try:
                    trades = await exchange.watch_trades(symbol)
                    collected_trades.extend(trades)
                    generated_bars = exchange.build_ohlcvc(collected_trades, self.timeframe, since, self.limit)
                    for bar in generated_bars:
                        bar_timestamp = bar[0]
                        collected_bars_length = len(self.bars[symbol])
                        last_collected_bar_timestamp = self.bars[symbol][collected_bars_length - 1][0] if \
                            collected_bars_length > 0 else 0
                        if bar_timestamp == last_collected_bar_timestamp:
                            self.bars[symbol][collected_bars_length - 1] = bar
                        elif bar_timestamp > last_collected_bar_timestamp:
                            self.bars[symbol].append(bar)
                            collected_trades = exchange.filter_by_since_limit(collected_trades, bar_timestamp)
                except Exception as e:
                    print(str(e))

        for symbol in symbols:
            task = create_task(rerun_forever(watch_ticker_loop, self.exchange, symbol))
            task.add_done_callback(end_ticker_task)
            self._ticker_tasks[task] = symbol
            await asyncio.sleep(0.2)
            if self.exchange.name in self.lob_limit:
                limit = self.lob_limit[self.exchange.name]
                task = create_task(rerun_forever(watch_lob_loop, self.exchange, symbol, limit))
                self._lob_tasks[task] = symbol
                await asyncio.sleep(0.2)

    async def stop_watcher(self):
        not_cancelled = {}
        for task in self._ticker_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                if not task.cancelled():
                    print('not cancelled')
                    not_cancelled[task] = self._ticker_tasks[task]
        print('tick cancelled')
        self._ticker_tasks = not_cancelled
        if len(not_cancelled):
            print('remaining ticking')

        not_cancelled = {}
        for task in self._lob_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                if not task.cancelled():
                    print('not cancelled')
                    not_cancelled[task] = self._lob_tasks[task]
        print('lob cancelled')
        self._lob_tasks = not_cancelled

        for url, client in self.exchange.clients.items():
            if 'ws' in url:
                await client.close()
        print('ws closed')


async def main():
    market = 'bitget'

    if market == 'bitget':
        symbols = ['AAVEUSDT', 'LTCUSDT',
        'TOMIUSDT', 'MINAUSDT', 'AAVEUSDT', 'EOSUSDT',
        'MKRUSDT', 'NEOUSDT',
        'CHZUSDT', 'KSMUSDT', 'BATUSDT', 'CRVUSDT',
        'COMPUSDT', 'QTUMUSDT', '1INCHUSDT', 'YFIUSDT',
        'SNXUSDT', 'ONTUSDT', 'STORJUSDT', 'SUSHIUSDT',
        'BANDUSDT', 'APTUSDT', 'ARBUSDT', 'AXSUSDT',
        'BLURUSDT', 'BNBUSDT', 'DYDXUSDT', 'ENSUSDT']
        endpoint = BitgetMarket(account=0)
    elif market == 'okx':
        symbols = ['BTC/USDT:USDT',
                   'ETH/USDT:USDT', 'MATIC/USDT:USDT',
                   'LTC/USDT:USDT', 'LINK/USDT:USDT', 'XLM/USDT:USDT',
                   'ATOM/USDT:USDT', 'ETC/USDT:USDT', 'ALGO/USDT:USDT', 'EGLD/USDT:USDT',
                   'MANA/USDT:USDT', 'XTZ/USDT:USDT', 'FIL/USDT:USDT', 'SAND/USDT:USDT',
                   'AAVE/USDT:USDT', 'EOS/USDT:USDT', 'MKR/USDT:USDT', 'NEO/USDT:USDT',
                   'CHZ/USDT:USDT', 'KSM/USDT:USDT', 'BAT/USDT:USDT', 'CRV/USDT:USDT',
                   'COMP/USDT:USDT', 'QTUM/USDT:USDT', '1INCH/USDT:USDT', 'YFI/USDT:USDT',
                   'SNX/USDT:USDT', 'ONT/USDT:USDT', 'STORJ/USDT:USDT', 'SUSHI/USDT:USDT',
                   'BAND/USDT:USDT', 'APT/USDT:USDT', 'ARB/USDT:USDT', 'AXS/USDT:USDT',
                   'BLUR/USDT:USDT', 'BNB/USDT:USDT', 'DYDX/USDT:USDT', 'ENS/USDT:USDT',
                   'IMX/USDT:USDT', 'KLAY/USDT:USDT', 'LRC/USDT:USDT', 'MASK/USDT:USDT',
                   'OP/USDT:USDT', 'STX/USDT:USDT', 'ZIL/USDT:USDT',
                   'APE/USDT:USDT', 'BSV/USDT:USDT', 'ICP/USDT:USDT', 'GMX/USDT:USDT',
                   'HBAR/USDT:USDT', 'LDO/USDT:USDT', 'MAGIC/USDT:USDT', 'SUI/USDT:USDT',
                   'WOO/USDT:USDT', 'TON/USDT:USDT', 'MINA/USDT:USDT', 'WAXP/USDT:USDT']
        endpoint = OkexMarket(account=0, request_timeout=120000)
    elif market == 'bin':
        symbols = ['BTCUSDT', 'LTCUSDT', 'VETUSDT']
        endpoint = BinanceMarket(account=2)
    else:
        symbols = ['LTCUSDT']  #, 'ETCUSDT', 'SOLUSDT']
        endpoint = BinanceFutureMarket(account='mel_cm1')
    exchange = endpoint._exchange_async

    await exchange.load_markets()

    event_queue = asyncio.Queue()
    runner = MarketWatch(exchange, '1m', event_queue)

    async def printer_loop(watch):
        start = datetime.datetime.timestamp(datetime.datetime.today())
        end = start
        while end - start < 300:
            try:
                event = event_queue.get_nowait()
                if event is not None and watch.last is not None:
                    print(watch.bars_df)
            except asyncio.QueueEmpty:
                await asyncio.sleep(1)
                end = datetime.datetime.timestamp(datetime.datetime.today())
            except asyncio.CancelledError:
                break
            except Exception:
                raise

    create_task(runner.run_watcher(symbols))
    await printer_loop(runner)

    await runner.stop_watcher()
    await exchange.close()


if __name__ == '__main__':
    l = logging.getLogger('market_watch')
    h = logging.StreamHandler(sys.stdout)
    l.addHandler(h)
    l.setLevel(logging.INFO)
    run(main())
