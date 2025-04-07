import logging
import asyncio
import ccxt

from .datafeed.bitgetfeed import BitgetMarket  
from .datafeed.dummyfeed import DummyMarket    
from .datafeed.motherfeeder import MotherFeeder
from .datafeed.utils_online import extract_coin_with_factor, build_symbol

class BrokerHandler:
    def __init__(self, market_watch, strategy_param, end_point_trade, logger_name):
        """
        :param market_watch: name of exchange for data format names of coins
        :type market_watch: str
        :param strategy_param: dict with configuration including 'send_orders' and 'exchange_trade'
        :type strategy_param: dict
        :param end_point_trade: market endpoint for orders
        :type end_point_trade: MotherFeeder
        :param logger_name: name for logger
        :type logger_name: str
        """
        self.market_watch = market_watch
        self._destination = strategy_param.get('send_orders', 'dummy')
        self._end_point_trade = end_point_trade
        self.market_trade = strategy_param['exchange_trade']
        self._logger = logging.getLogger(logger_name)

    @staticmethod
    def build_end_point(market, account=0) -> MotherFeeder:
        """
        Build the appropriate market endpoint based on the market name.
        """
        if market == 'bitget':
            end_point = BitgetMarket(account=account, request_timeout=60000)
        else:
            end_point = DummyMarket(account)
        return end_point

    def symbol_to_market_with_factor(self, symbol, universal=False):
        """
        Transform a symbol name in data exchange format into trade format and returns the price factor
        :param symbol: symbol of coin
        :type symbol: str
        :param universal: true if universal ccxt name, false if exchange specific name
        :type universal: bool
        :return: tuple of (symbol, factor)
        :rtype: (str, float)
        """
        coin, factor1 = extract_coin_with_factor(symbol)
        symbol, factor2 = build_symbol(coin, self.market_trade, universal=universal)
        return symbol, factor1 * factor2

    def get_contract_qty_from_coin(self, coin, quantity):
        """
        Convert coin quantity to contract quantity based on exchange info
        :param coin: symbol of coin
        :type coin: str
        :param quantity: amount in coin units
        :type quantity: float
        :return: quantity in contracts
        :rtype: float
        """
        info = self._end_point_trade._exchange.market(coin)
        factor = 1
        if 'contractSize' in info and info['contractSize'] is not None:
            factor = info['contractSize']
        return quantity / factor

    async def close_exchange_async(self):
        """
        Close the exchange connection
        """
        self._logger.info('Closing exchange connection')
        await self._end_point_trade._exchange_async.close()