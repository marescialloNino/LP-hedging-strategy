import pandas as pd
from datafeed import alphafeed, tiingofeed
from synthetic import diffusiongenerator
from datetime import datetime

def main():
    # testing alpha

    ticker = 'LTC-ETH'
    start = datetime(2018, 1, 1)
    end = datetime(2019, 1, 1)
    af = alphafeed.AlphaFeeder()
    df = af.get_daily(ticker, start, end)
    assert(df.shape ==(366, 6))
    assert(df['volume'][0] == 11433)

    # testing tiingo

    tickers = ['AAPL', 'GOOGL']
    start = datetime(2018, 1, 1)
    end = datetime(2019, 1, 1)
    tf = tiingofeed.TiingoFeeder()
    df = tf.get_daily(tickers, start, end)
    assert(df.shape ==(502, 12))
    assert(df.index.names[0] == 'symbol')
    assert (df.loc['AAPL','adjVolume'][0] == 25048048)

# testing dummy
    nasset = 2
    nperiod = 1000
    gen = diffusiongenerator.diffusion_generator(nasset, nperiod)
    gen.set_diffusion(0.5, 0.1)
    data = gen.generate_TS(1, False)
    assert(isinstance(data, pd.Series))


if __name__ == '__main__':
    main()