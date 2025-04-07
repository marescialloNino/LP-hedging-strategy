import sqlite3
import pandas as pd
from datetime import datetime
from datafeed.motherfeeder import MotherFeeder

"""
exchange

	id 	name
0 	1 	binance
1 	2 	ftx
2 	3 	okex
3 	4 	deribit
4 	5 	huobi

typeinstrument

	id 	name
0 	1 	spot
1 	2 	future

CREATE TABLE instrument (
    id integer primary key AUTOINCREMENT,
    exchange_id integer NOT NULL,
    symbol TEXT NOT NULL,   -- nom de l'instrument utilisé par l'exchange
    type_id integer NOT NULL,  -- id du type de l'instrument (ex : spot ou fut)
    name TEXT,     -- nom commun de l'instrument
    matu TEXT,
    underlyingid integer,
    statut integer,
    FOREIGN KEY (exchange_id) 
      REFERENCES exchange (id),
    FOREIGN KEY (type_id) 
      REFERENCES typeinstrument (id) 
    )
CREATE TABLE ohlcv (
    instrument_id integer,
    datetime DATETIME,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    flag integer, -- pour indiquer si le point a été complété
    PRIMARY KEY (instrument_id, datetime),
    FOREIGN KEY (instrument_id) 
      REFERENCES instrument (id)
    )
    
Out[9]: 
      id  exchange_id     symbol  ...  matu underlyingid statut
0    273            1    BTCUSDT  ...  None          NaN   None
1    274            1    BTCUSDT  ...  Perp        273.0   None
2    277            1   QTUMUSDT  ...  None          NaN   None   
     id  exchange_id      symbol  ...  matu underlyingid statut
0    275            2         BTC  ...  None          NaN   None
1    276            2    BTC-PERP  ...  Perp        275.0   None 
      id  exchange_id          symbol  ...  matu underlyingid statut
0      3            3        GRT-USDT  ...  None          NaN   None
1      4            3   GRT-USDT-SWAP  ...  Perp          3.0   None
   id  exchange_id    symbol  type_id      name  matu  underlyingid statut
0   1            5  BTC-USDT        1  BTC/USDT  None           NaN   None
1   2            5  BTC-USDT        2  BTC/USDT  Perp           1.0   None
"""

class DatabaseConnector:
    __DBNAME = f'D:\Christian\python_github\crypto2.db'

    def __init__(self, exchange):
        self.con = sqlite3.connect(DatabaseConnector.__DBNAME)
        self.exchange = exchange

    def fetch_ohlcv(self, symbol, read_timeframe, since, limit):
        """
        fetch ohlcv for asset

        :param symbol:
        :type symbol: conversion.symbol
        :param read_timeframe: description of resampling
        :type read_timeframe: string
        :param since:
        :type since: timestamp in ms
        :param limit:
        :type limit:
        :return:
        :rtype:
        """
        type_id = symbol.Type
        ticker = symbol.Name

        since = datetime.fromtimestamp(since * 0.001)

        id = pd.read_sql(f"select id from instrument where exchange_id={self.exchange} and type_id={type_id} and symbol='{ticker}'",self.con).loc[0,'id']
        ohlcv = pd.read_sql(f"select datetime, open, high, low, close, volume from ohlcv where instrument_id={id} order by datetime",self.con, parse_dates='datetime')
        ohlcv = ohlcv[ohlcv['datetime'] > since]
        ohlcv.set_index('datetime', inplace=True)
        rez = ohlcv.resample(read_timeframe, label='left').last()

        return rez

    def fetch_ohlcv_full(self, symbol, read_timeframe, since, limit):
        """
        fetch ohlcv for ul, future and funding

        :param symbol:
        :type symbol:
        :param read_timeframe:
        :type read_timeframe:
        :param since:
        :type since:
        :param limit:
        :type limit:
        :return:
        :rtype:
        """
        ticker = symbol.Name
        ids = pd.read_sql(f"select type_id, id from instrument where exchange_id={self.exchange} and symbol='{ticker}'",self.con, index_col='type_id')
        idlist = ','.join(list(ids['id'].astype('str')))
        ohlcv = pd.read_sql(f"select datetime, instrument_id, open, high, low, close, volume from ohlcv where instrument_id in ({idlist}) order by datetime",self.con, parse_dates='datetime')
        ohlcv = ohlcv[ohlcv['datetime'] > since]
        futid=ids.loc[2, 'id']
        funding = pd.read_sql(f"select  datetime, fundingrate from fundingrate where instrument_id={futid} and matu=8",self.con, parse_dates='datetime', index_col='datetime')

        ohlcv= ohlcv.pivot_table(['open', 'high', 'low', 'close', 'volume'], 'datetime', 'instrument_id')
        ohlcv.columns.set_levels(['ul0','ul1'], level=1, inplace=True)
        ohlcv.columns = ohlcv.columns.to_flat_index()
        convert = {name : name[0] + '@' + name[1] for name in ohlcv.columns}
        aggreg = dict()
        for name in ['@ul0', '@ul1']:
            aggreg.update({'open'+name: 'first'})
            aggreg.update({'high' + name: 'max'})
            aggreg.update({'low' + name: 'min'})
            aggreg.update({'close' + name: 'last'})
            aggreg.update({'volume' + name: 'sum'})
        ohlcv.rename(columns=convert, inplace=True)
        rez = ohlcv.resample(read_timeframe).agg(aggreg)
        funding = funding.resample(read_timeframe, label='left').last()
        rez['funding'] = funding
        rez['funding'].ffill(inplace=True)

        return rez


    def load_markets(self):
        pass

    def getUnderlyingSymbol(self, exchange_id, symbol, type_id):
        spotId = pd.read_sql(f"select underlyingid from instrument where exchange_id={exchange_id} and type_id={type_id} and symbol='{symbol}'",self.con).iloc[0,0]
        spotSymbol = pd.read_sql(f"select symbol from instrument where id={spotId}",self.con).iloc[0,0]

    @property
    def rateLimit(self):
        return 1

class DatabaseMarket(MotherFeeder):
    __MARKETS = ['ETHBTC', 'ETHUSDT']

    def __init__(self, source, *args, **kwargs):
        self.source = source

        super().__init__(*args, **kwargs)

    def build_end_point(self):
        return DatabaseConnector(exchange=self.source)

    def _arrange_data(self, df):
        return df

    def _init_API(self):
        return

    def get_markets(cls):
        return cls.__MARKETS

    def _get_limit(self):
        return 1000000


