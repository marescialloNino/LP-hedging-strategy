import unittest
import datafeed.bingxfeed as bf
import datafeed.dummyfeed as df
import os
import pandas as pd
from time import time


class MyTestCase(unittest.TestCase):
    def test_something(self):
        ticker = 'BTC-USDT'
        tf = '1h'
        dummy_test = True

        # first test
        end_time = int(round(time()))
        start_time = end_time - 2 * 24 * 60 * 60

        try:
            market = bf.BingxMarket()
            data, done = market.read_bars(symbol=ticker, timeframe=tf, start_time=start_time, end_time=end_time)
            expect = 48
            self.assertEquals(len(data), expect)
            size = market.get_min_order(ticker)
            self.assertAlmostEqual(size, 1e-5)
            order = market.get_rounded(0.123456789, ticker)
            self.assertEqual(order, "0.12345")
        except OSError as e:
            print(e)
        except (ValueError, TypeError) as e:
            print(e)


if __name__ == '__main__':
    unittest.main()

