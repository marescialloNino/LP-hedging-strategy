import unittest
import pandas as pd
import numpy as np
import datafeed.downsample


class MyTestCase(unittest.TestCase):
    def test_something(self):
        n_periods = 100000
        np.random.seed(seed=1001)
        datelist = pd.date_range(end=pd.datetime.today(), periods=n_periods, normalize=True, freq='1min')
        ts = np.random.normal(size=n_periods).cumsum() + 100
        df = pd.DataFrame(index = datelist)
        df['open']=ts
        df['high']=ts
        df['low']=ts
        df['close']=ts
        df['volume'] = np.random.gumbel(2000, 2000, size=n_periods)
        df['volume'] = df['volume'] + 1000 - df['volume'].min()

        rez = datafeed.downsample.create_bars(df, 60, type='time')
        self.assertTupleEqual(rez.shape, (1668,7))

        rez = datafeed.downsample.create_bars(df, 50000, type='volume')
        self.assertTupleEqual(rez.shape, (13927,7))


if __name__ == '__main__':
    unittest.main()
