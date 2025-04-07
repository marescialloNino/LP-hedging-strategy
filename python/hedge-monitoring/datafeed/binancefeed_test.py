import unittest
import datafeed.binancefeed as bf
from time import time
from asyncio import gather


class MyAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_something(self):
        ticker = 'BTC/USDT'
        tf = '1h'
        # first test
        end_time = int(round(time()))
        start_time = end_time - 2 * 24 * 60 * 60
        markets = [bf.BinanceMarket(), bf.BinanceFutureMarket()]  # , df.DummyMarket()]
        expect = 49

        # async
        async def get_data(market):
            promise = await market.read_bars(symbol=ticker, timeframe=tf, start_time=start_time, end_time=end_time)
            data[market._exchange.id] = promise

            return

        data = {}
        done = {}
        loops = [get_data(market) for market in markets]
        await gather(*loops)
        for item in data.values():
            self.assertEquals(len(item['bars']), expect)

class MyTestCase(unittest.TestCase):
    def test_something(self):
        ticker = 'BTC/USDT'
        tf = '1h'
        # first test
        end_time = int(round(time()))
        start_time = end_time - 2 * 24 * 60 * 60
        markets = [bf.BinanceMarket(), bf.BinanceFutureMarket()]  # , df.DummyMarket()]
        expect = 49

        for market in markets:
            try:
                data, done = market.read_bars(symbol=ticker, timeframe=tf, start_time = start_time, end_time=end_time)
                self.assertEquals(len(data), expect)
            except OSError as e:
                print(e)
            except (ValueError, TypeError) as e:
                print(e)
        size = markets[0].get_min_order(ticker)
        self.assertAlmostEqual(size, 1e-5)
        order = markets[0].get_rounded(0.123456789, ticker)
        self.assertEqual(order, "0.12345")
        size = markets[1].get_min_order(ticker)
        self.assertAlmostEqual(size, 1e-3)
        order = markets[1].get_rounded(0.123456789, ticker)
        self.assertEqual(order, "0.123")



if __name__ == '__main__':
    unittest.main()

