"""
This is the feeder class from file source
"""
from .motherfeeder import MotherFeeder
import pandas as pd, numpy as np
import datetime


class FileFeeder(MotherFeeder):
    def __init__(self, name=None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

    def build_end_point(self):
        pass

    def read_bars(self, symbol, timeframe, start_time, end_time=None, rf=None):
        pass

    @property
    def name(self):
        """
        """
        return self.name

    def read_json(self, filename):
        file = open(filename,'r')
        dict = eval(file.read())
        histo = pd.DataFrame(dict['prices'])
        histo[0] /= 1000
        histo.index = histo[0].apply(datetime.datetime.fromtimestamp)
        histo.drop(columns=0,inplace=True)
        return pd.DataFrame(index=histo.index)

    @staticmethod
    def read_csv(filename, column_names='all', single_header=True):
        if single_header:
            header = None
        else:
            header = [0,1]
        df = pd.read_csv(filename, parse_dates=True, header=header, index_col='date', sep=',')
        df = df.loc[~df.index.duplicated(keep='last')]

        try:
            if column_names == 'all':
                result = df
            else:
                result = df[column_names]
        except:
            raise('wrong data')
        return result
