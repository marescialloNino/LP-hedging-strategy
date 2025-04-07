import unittest
from datetime import datetime
from databasefeed import DatabaseMarket
from util import conversions

class MyTestCase(unittest.TestCase):
    def test_something(self):
        source = 1
        name = 'BTCUSDT'
        type = conversions.TYPESPOT
        symbol = conversions.symbol(type, name)
        tf = '8h'
        start_time = datetime(2020,1,1)
        end_time =datetime(2021,1,1)
        market = DatabaseMarket(1)
        data,_ = market.read_bars(
            symbol=symbol, timeframe=tf, start_time=start_time.timestamp(), end_time=end_time.timestamp())
        self.assertTupleEqual(data.shape, (1678,5))


if __name__ == '__main__':
    unittest.main()
