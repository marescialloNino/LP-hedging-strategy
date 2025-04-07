import unittest

import pandas as pd
import pandas.errors
from datetime import datetime
import time
from datafeed.dummyfeed import DummyMarket


class MyTestCase(unittest.TestCase):
    def test_something(self):
        tf = DummyMarket()
        tf.build_end_point()
        start = datetime.timestamp(datetime.today()) - 3600
        for index in range(10):
            data, done = tf.read_bars('BTC', '1m', start)
            expect = 49
            self.assertEqual(len(data), expect)
            print(el.shape)
            print(el.tail(1))
            time.sleep(10)
            start = datetime.timestamp(el.index[-1])
            

if __name__ == '__main__':
    unittest.main()
