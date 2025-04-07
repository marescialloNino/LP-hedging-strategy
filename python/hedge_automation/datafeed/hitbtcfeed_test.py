import unittest
import datafeed.hitbtcfeed as bf
from time import time


class MyTestCase(unittest.TestCase):
    def test_something(self):
        symbol = 'BTC/USDT'
        tf = '1h'
        end_time = int(round(time()))
        start_time = end_time - 2 * 24 * 60 * 60
        markets = [bf.HitbtcMarket()]  # , df.DummyMarket()]

        for market in markets:
            try:
                data, done = market.read_bars(symbol=symbol, timeframe=tf, start_time = start_time, end_time=end_time)
                expect = 49
                self.assertEqual(len(data), expect)
            except OSError as e:
                print(e)
            except (ValueError, TypeError) as e:
                print(e)

if __name__ == '__main__':
    unittest.main()

