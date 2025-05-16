import os
import sys

import aiohttp
import pandas as pd
import websockets
import asyncio
import uuid
import logging
import json
import numpy as np
import traceback

from reporting.bot_reporting import TGMessenger
from data_handler import BrokerHandler
from core_ms.bot_event import EventType
from datafeed.utils_online import parse_pair, today_utc

ONE_BP = 1e-4


def order_reverse(s1, s2, spread_shift, action, target_size1, target_size2, p1, p2):
    if p2 < p1:
        s1, s2 = s2, s1
        p1, p2 = p2, p1
        target_size1, target_size2 = target_size2, target_size1
        action = - action
    spread = (1 - p1 / p2) / ONE_BP

    return s1, s2, action, target_size1, target_size2, spread, p1, p2


async def post_any(session, upi, json_req):
    async with session.post(upi, json=json_req) as resp:
        response = await resp.read()
        return {'status_code': resp.status, 'text': response}


async def get_any(session, upi):
    async with session.get(upi) as resp:
        response = await resp.read()
        return {'status_code': resp.status, 'text': response}


class WebSpreaderListener:
    AMAZON_WS_UPI = 'ws://54.249.138.8:8080/wsapi/strat/update'

    def __init__(self, logger):
        self.listener_task = None
        self.subscriptions = set()
        self.results = {'errors': [], 'last_modified': pd.Timestamp(year=2000,month=1,day=1, tz='UTC')}
        self._logger = logger

    def message_age(self) -> float:
        """
        Age in seconds
        :return: message age
        """
        return (today_utc() - self.results['last_modified']).total_seconds()

    async def _listen(self):
        greeting = 'Allo?'
        self._logger.info('starting listener')
        async with websockets.connect(self.AMAZON_WS_UPI, max_size=4194304) as websocket:
            await websocket.send(greeting)
            while True:
                message = await websocket.recv()
                if message:
                    messages = json.loads(message)
                    events = messages.get('stratEvents', [])
                    self.results['last_modified'] = today_utc()

                    for event in events:
                        strat_id = event.get('stratId', '')
                        if strat_id in self.subscriptions:
                            result = event.get('results', '')
                            self.results[strat_id] = result

    def _start_listener(self):
        async def rerun_forever(coro, *args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    msg = traceback.format_exc()
                    self.results['errors'].append(msg)
                    self._logger.warning(f'listener said {msg}')
                    await asyncio.sleep(60)

        def done_listener(task):
            self.listener_task = None

        self.results['errors'].clear()
        self.listener_task = asyncio.create_task(rerun_forever(self._listen))
        self.listener_task.add_done_callback(done_listener)

    def subscribe(self, strat_id):
        self.subscriptions.add(strat_id)
        if self.listener_task is None:
            self._start_listener()

    async def stop_listener(self, strat_id=None):
        if strat_id is not None:
            self.subscriptions.remove(strat_id)
            self.results.pop(strat_id, None)
            if len(self.subscriptions) > 0:
                return
        self.subscriptions.clear()
        if self.listener_task is not None:
            self._logger.info('cancelling listener')
            self.listener_task.cancel()

    def get_strat_result(self, strat_id):
        if strat_id in self.results:
            return self.results.get(strat_id, {})
        else:
            self._logger.warning(f'retrieving non existing strat from listener with state {self.results["errors"]}')
            return {}


class WebSpreaderBroker:
    AMAZON_URL2 = 'http://54.249.138.8:8080/api'
    AMAZON_UPI_CREATE = AMAZON_URL2 + '/strat/spreader'
    AMAZON_UPI_STOP = AMAZON_URL2 + '/strat/stop'
    AMAZON_UPI_SPREADER_ALL_STOP = AMAZON_URL2 + '/strat/stopAllMarketSpreader'
    AMAZON_UPI_SPREADER_DELETE_ALL = AMAZON_URL2 + '/strat/deleteAllMarketSpreader'
    AMAZON_UPI_SINGLE = AMAZON_URL2 + '/manualOrder/createOrUpdate'
    AMAZON_UPI_SINGLE_STOP = AMAZON_URL2 + '/manualOrder/stop'
    AMAZON_UPI_SINGLE_ALL_STOP = AMAZON_UPI_STOP + '/ManualOrder'
    AMAZON_UPI_SINGLE_DELETE_ALL = AMAZON_URL2 + '/manualOrder/deleteAll'
    # api/strat/stop/ManualOrder /api/
    # /api/manualOrder/deleteAll
    # /api/strat/deleteAllMarketSpreader

    ACCOUNT_DICT = {
        'okx':
            {
                'edo1': 'edo1',
                'edo3': 'edo3'
            },
        'bybit_fut':
            {
                1: 'pairspreading1',
                2: 'pairspreading2',
                3: 'pairspreading3'
            },
        'bin_spot':
            {
                1: 'pairspreading_api1',
                2: 'pairspreading_api2'
            },
        'bitget_fut':
            {
                1: 'Pairspreading1',
                2: 'Pairspreading2'
            },
        'bin_fut':
            {
                'mel_cm1': 'mel_cm1',
                'mel_cm2': 'mel_cm2',
                'mel_cm3': 'mel_cm3'
            }
    }

    def __init__(self, market, broker_handler, account=None, chat_channel=''):
        self._tg_channel = chat_channel
        self.id = None
        self.broker_handler = broker_handler
        self.long_short_split = True   # if True, split order in 2

        if 'ok' in market:
            self.exchange = 'okx'
        elif 'bin' in market and 'fut' in market:
            self.exchange = 'bin_fut'
        elif 'bin' in market:
            self.exchange = 'bin_spot'
        elif 'bybit' in market:
            self.exchange = 'bybit_fut'
        elif 'bitget' in market:
            self.exchange = 'bitget_fut'
        else:
            raise ValueError(f'Web broker does not support {market} market')

        if account not in WebSpreaderBroker.ACCOUNT_DICT[self.exchange]:
            raise ValueError('Bad account')
        self.account_name = WebSpreaderBroker.ACCOUNT_DICT[self.exchange][account]
        self.pair_config = {'legOneExchange': self.exchange,
                            'legTwoExchange': self.exchange,
                            'legOneAccount': self.account_name,
                            'legTwoAccount': self.account_name,
                            'legOneOffset': 'Open', 'legOneUseMargin': False,
                            'legTwoOffset': 'Open', 'legTwoUseMargin': False,
                            'qtyMethod': 'IsoUsdt',
                            'start': 'Automatic'
                            }
        self.use_spread = False
        self.persistance = {}  # dict with uid of order, type, parameters, status
        self.logger = logging.getLogger(f'broker_web_{market}_{account}')


    @staticmethod
    async def stop_all():
        async with aiohttp.ClientSession() as session:
            await post_any(session, WebSpreaderBroker.AMAZON_UPI_SPREADER_ALL_STOP, json_req={})
            await post_any(session, WebSpreaderBroker.AMAZON_UPI_SINGLE_ALL_STOP, json_req={})

    @staticmethod
    async def delete_all():
        async with aiohttp.ClientSession() as session:
            await post_any(session, WebSpreaderBroker.AMAZON_UPI_SPREADER_DELETE_ALL, json_req={})
            await post_any(session, WebSpreaderBroker.AMAZON_UPI_SINGLE_DELETE_ALL, json_req={})

    def get_contract_qty_from_coin(self, coin, qty):
        return self.broker_handler.get_contract_qty_from_coin(coin, qty)

    async def send_order(self, id, event, spread):
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, coin=event.ticker, action=event.action, price=event.price,
                                                target_quantity=event.quantity, comment=event.comment,
                                                translate_qty_incontracts=True, use_algo=True, nature=event.nature)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        """

        :param id: int, id of order
        :param s1: str, ticker1
        :param s2: str, ticker2
        :param spread_shift: float, leeway for order acceptance in bp
        :param action: int, direction for ticker1
        :param target_size1: float
        :param target_size2: float
        :param prices: Tuple(float, float)
        :param comment: str
        :return: bool
        """

        p1, p2 = prices

        if 'LS' in comment and self.long_short_split:
            nature1 = 'exit' if nature[0] == 'x' else 'entry'
            nature2 = 'exit' if nature[1] == 'x' else 'entry'
            tasks = []
            self.logger.info('splitting order into 2 manual orders')
            tasks.append(asyncio.create_task(self.send_simple_order(id + '-1', coin=s1, action=action, price=p1,
                                                                    target_quantity=target_size1,
                                                                    comment='leg1_' + nature1 + '_' + comment,
                                                                    translate_qty_incontracts=True, use_algo=True,
                                                                    nature=nature[0])))
            tasks.append(asyncio.create_task(self.send_simple_order(id + '-2', coin=s2, action=-action, price=p2,
                                                                    target_quantity=target_size2,
                                                                    comment='leg2_' + nature2 + '_' + comment,
                                                                    translate_qty_incontracts=True, use_algo=True,
                                                                    nature=nature[1])))

            all_responses = await asyncio.gather(*tasks)
            try:
                response = all_responses[0] & all_responses[1]
            except:
                response = False
            return response

        with_tg_message = self._tg_channel != ''

        spread = spread_shift

        # spread_s = np.format_float_positional(spread, 6, unique=False, fractional=False)
        # target_size_s = np.format_float_positional(target_size1, 6, unique=False, fractional=False)
        # max_order_size_s = np.format_float_positional(max_order_size1, 6, unique=False, fractional=False)

        direction = 'BUY_LEG1' if action > 0 else 'SELL_LEG1'

        symbol1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1, universal=False)
        symbol2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2, universal=False)
        self.logger.info(f'Preparing order for {s1},{s2} with symbols {symbol1},{symbol2}')

        p1 /= factor1
        p2 /= factor2
        target_size1 *= factor1
        amount1 = p1 * target_size1

        amount_threshold = 1000

        if amount1 > amount_threshold:
            max_amount = np.random.uniform(low=amount_threshold * 0.8, high=amount_threshold * 1.6)
            max_order_size1 = max_amount / p1
            if max_order_size1 > target_size1:
                max_order_size1 = target_size1
        else:
            max_order_size1 = amount_threshold / p1
        spread = round(spread, 8)
        target_size1 = round(target_size1, 6)
        max_order_size1 = round(max_order_size1, 6)

        config = {
            'legOneSymbol': symbol1, 'legTwoSymbol': symbol2,
            'targetSize': target_size1,  # coin leg1
            'maxOrderSize': max_order_size1,  # pour découpage,
            'spreadDirection': direction
        }
        config.update(self.pair_config)

        if self.use_spread:
            config.update({'targetSpread': spread})
            request = {'clientOrderId': id, 'orderMsg': 0, 'strategyType': 'SpreaderLimit',
                       'spreaderLimitConfiguration': config}
        elif self.exchange == 'bin_spot':
            if nature[0] == 'x':
                side_effect1 = 'AUTO_REPAY'
            else:
                side_effect1 = 'MARGIN_BUY'
            if nature[1] == 'x':
                side_effect2 = 'AUTO_REPAY'
            else:
                side_effect2 = 'MARGIN_BUY'

            config.update({'targetSpread': spread,
                           'targetPrice1': p1,
                           'targetPrice2': p2,
                           'legOneSideEffect': side_effect1,
                           'legTwoSideEffect': side_effect2,
                           'legOneUseMargin': True,
                           'legTwoUseMargin': True,
                           'clientRef': comment})
            request = {'clientOrderId': id, 'orderMsg': 0, 'spreaderConfiguration': config}
        else:
            reduce1 = nature[0] == 'x'
            reduce2 = nature[1] == 'x'
            config.update({'targetSpread': spread,
                           'targetPrice1': p1,
                           'targetPrice2': p2,
                           'legOneReduceOnly': reduce1,
                           'legTwoReduceOnly': reduce2,
                           'clientRef': comment})
            request = {'clientOrderId': id, 'orderMsg': 0, 'spreaderConfiguration': config}

        message = ''
        response_broker = None
        self.logger.info(f'sending order to {WebSpreaderBroker.AMAZON_UPI_CREATE}: {request}')

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    tg_rez = await TGMessenger.send_message_async(
                        session=session, comment=f'{self.exchange}/{self.account_name}:{comment}',s1=symbol1, s2=symbol2,
                        direction=direction, spread=spread, p1=p1, p2=p2, chat_channel=self._tg_channel)
                else:
                    tg_rez = {'ok': True}
                response_broker = await post_any(session, WebSpreaderBroker.AMAZON_UPI_CREATE, json_req=request)

        except aiohttp.ClientConnectorError as e:
            message = f'web broker exception: {e}'
        except Exception as e:
            message = f'web broker exception: {e}'

        if not tg_rez.get('ok', True):
            self.logger.warning(f'TG problem')
        if response_broker is None or 'status_code' not in response_broker:
            message = f'broker null or ill-formated response' + message
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        elif response_broker['status_code'] != 200:
            message = f'broker response: {response_broker["status_code"]}, {response_broker["text"]}' + message
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=False, nature=''):
        """, coin, direction, quantity

        :param order_id: int, id of order
        :param coin: str, ticker
        :param action: int, direction for ticker
        :param price: float, price for LIM order
        :param target_quantity: float
        :return: bool
        """

        with_tg_message = self._tg_channel != ''
        direction = 'BUY' if action > 0 else 'SELL'
        symbol, factor = self.broker_handler.symbol_to_market_with_factor(coin, universal=False)
        ticker, _ = self.broker_handler.symbol_to_market_with_factor(coin, universal=True)

        self.logger.info(f'Preparing order for {symbol} with ticker {ticker}')

        if price is not None:
            price /= factor
        target_quantity *= factor

        if self.exchange != 'bin_spot' and translate_qty_incontracts:
            qty = self.broker_handler.get_contract_qty_from_coin(ticker, target_quantity)
            self.logger.info(f'Converted {target_quantity} {ticker} to {qty} contracts')
        else:
            qty = target_quantity

        if price is None:
            max_order_size = qty
            amount = None
        else:
            amount = qty * price
            amount_threshold = 1000

            if amount > amount_threshold:
                max_amount = np.random.uniform(low=amount_threshold * 0.8, high=amount_threshold * 1.6)
                max_order_size = max_amount / price
                if max_order_size > qty:
                    max_order_size = qty
            else:
                max_order_size = amount_threshold / price

        # max_order_size = 37

        min_notional = self.broker_handler.get_min_notional(symbol)
        self.logger.info(f'Received minimum notional of {min_notional} for {symbol}')

        target_quantity = round(qty, 6)
        max_order_size = round(max_order_size, 6)

        config = {'symbol': symbol,
                  'exchange': self.exchange,
                  'account': self.account_name,
                  'offset': 'Open',
                  'useMargin': True,
                  'targetSize': target_quantity,
                  'maxOrderSize': max_order_size,  # pour découpage,
                  'maxAliveOrderTime': 0,
                  'direction': direction,
                  'childOrderDelay': 3000,
                  'start': 'Automatic',
                  'marginMode': 'cross',
                  }
        if (self.exchange == 'bin_fut' and min_notional is not None and
                amount is not None and amount < min_notional and 'entry' not in comment):
            config.update(
                {
                    'reduceOnly': True
                })
        if self.exchange == 'bin_spot':
            if nature == 'n' and action < 0:
                side_effect = 'MARGIN_BUY'
                config['sideEffectType'] = side_effect
            elif nature == 'x' and action > 0:
                side_effect = 'AUTO_REPAY'
                config['sideEffectType'] = side_effect

        if use_algo:
            childOrderDelay = 1000 * np.random.uniform(0.2, 1)
            maxAliveOrderTime = 1000 * np.random.uniform(10, 20)
            config.update(
                {
                    'type': 'LIMIT_WITH_LEEWAY',
                    'maxStratTime': 30 * 60 * 1000,  # 30 minutes
                    'timeInForce': 'GTC',
                    'maxAliveOrderTime': maxAliveOrderTime,  # 15 secondes
                    'childOrderDelay': childOrderDelay
                })
        elif price is not None and amount > 10:
            config.update(
                {
                    'price': price,
                    'type': 'LIMIT',
                    # 'maxAliveOrderTime': 13 * 1000,  # 15 secondes
                    'timeInForce': 'GTC'
                })
        else:
            childOrderDelay = 1000 * np.random.uniform(0.2, 1)
            config = {'symbol': coin,
                      'exchange': self.exchange,
                      'account': self.account_name,
                      'offset': 'Open',
                      'useMargin': True,
                      'targetSize': target_quantity,
                      'maxAliveOrderTime': 0,
                      'direction': direction,
                      'childOrderDelay': childOrderDelay,
                      'start': 'Automatic',
                      'type': 'MARKET',  # no price for market order
                      }

        request = {'clientOrderId': order_id, 'orderMsg': 0, 'manualOrderConfiguration': config}

        self.logger.info(f'sending order to {WebSpreaderBroker.AMAZON_UPI_SINGLE}: {request}')
        response_broker = None
        message = ''

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    tg_rez = await TGMessenger.send_message_async(
                        session,
                        f'{self.exchange}/{self.account_name}:{comment}',
                        coin, '',
                        direction,0, price, '', chat_channel=self._tg_channel)
                else:
                    tg_rez = {'ok': True}

                response_broker = await post_any(session, WebSpreaderBroker.AMAZON_UPI_SINGLE, json_req=request)
        except (aiohttp.ClientConnectorError, TimeoutError) as e:
            message = f'web broker exception: {e}'
        except Exception as e:
            message = f'web broker exception: {e}'

        if not tg_rez.get('ok', True):
            self.logger.warning(f'TG prem')
        if response_broker is None or 'status_code' not in response_broker:
            message = f'broker null or ill-formated response' + message
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        elif response_broker['status_code'] != 200:
            message = f'broker response: {response_broker["status_code"]}{response_broker["text"]}{message}'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        return True

    async def stop_order(self, order_id):
        code = 0

        try:
            upi = WebSpreaderBroker.AMAZON_UPI_STOP + f'/{order_id}'
            async with aiohttp.ClientSession() as session:
                response_broker = await get_any(session, upi)
            code = response_broker.get('status_code', 400)
            message = response_broker['text']
        except aiohttp.ClientConnectorError as e:
            message = f'Web_broker ClientConnectorError in stop_order: {e}'
        except Exception as e:
            message = f'web_broker exception in stop_order: {e}'

        if code != 200:
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        return True

    async def stop_simple_order(self, order_id):
        code = 0

        try:
            upi = WebSpreaderBroker.AMAZON_UPI_SINGLE_STOP + f'/{order_id}'
            async with aiohttp.ClientSession() as session:
                response_broker = await get_any(session, upi)
            code = response_broker.get('status_code', 400)
            message = response_broker['text']
        except aiohttp.ClientConnectorError as e:
            message = f'Web_broker ClientConnectorError in stop_simple_order: {e}'
        except Exception as e:
            message = f'web_broker exception in stop_simple_order: {e}'

        if code != 200:
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        return True

    @property
    def get_id(self):
        self.id = str(uuid.uuid4())
        return self.id


class EdoSpreaderBroker:
    ACCOUNT_DICT = {
        'okx':
            {
                'edo1': 'edo1',
                'edo3': 'edo3'
            }
    }

    def __init__(self, market, broker_handler, account=1, chat_channel=None):
        self._tg_channel = chat_channel
        self.id = None
        self.broker_handler = broker_handler

        if 'ok' in market:
            self.exchange = 'okx'
        else:
            raise ValueError('Bad market for Edo broker')

        if (self.exchange not in EdoSpreaderBroker.ACCOUNT_DICT or
                account not in EdoSpreaderBroker.ACCOUNT_DICT[self.exchange]):
            raise ValueError('Bad exchange/account for Edo broker')
        self.account_name = EdoSpreaderBroker.ACCOUNT_DICT[self.exchange][account]
        self.logger = logging.getLogger(f'broker_edo_{market}_{account}')

    @staticmethod
    async def stop_all():
        return

    @staticmethod
    async def delete_all():
        return

    def get_contract_qty_from_coin(self, coin, qty):
        return self.broker_handler.get_contract_qty_from_coin(coin, qty)

    async def send_order(self, id, event, spread):
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, coin=event.ticker, action=event.action,
                                                price=event.price, target_quantity=event.quantity,
                                                comment=event.comment)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        """

        :param id: int, id of order
        :param s1: str, ticker1
        :param s2: str, ticker2
        :param spread_shift: float, leeway for order acceptance in bp
        :param action: int, direction for ticker1
        :param target_size1: float
        :param target_size2: float
        :param prices: Tuple(float, float)
        :param comment: str
        :return: bool
        """

        with_tg_message = self._tg_channel != ''

        p1, p2 = prices
        spread = spread_shift
        # spread_s = np.format_float_positional(spread, 6, unique=False, fractional=False)
        # target_size_s = np.format_float_positional(target_size1, 6, unique=False, fractional=False)
        # max_order_size_s = np.format_float_positional(max_order_size1, 6, unique=False, fractional=False)

        spread = round(spread, 8)
        target_size1 = round(target_size1, 6)
        target_size2 = round(target_size2, 6)

        direction = 'BUY_LEG1' if action > 0 else 'SELL_LEG1'
        s1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1, universal=True)
        s2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2, universal=True)
        p1 /= factor1
        p2 /= factor2
        target_size1 *= factor1
        target_size2 *= factor2

        config = {
            'legOneSymbol': s1, 'legTwoSymbol': s2,
            'targetSizeOne': target_size1,
            'targetSizeTwo': target_size2,
            'spreadDirection': direction,
            'comment': comment
        }
        tasks = []
        self.logger.info(f'Received spread order: {config}')

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    await TGMessenger.send_message_async(session, f'{self.exchange}/{self.account_name}:{comment}',
                                                         s1, s2, direction,
                                                         spread, p1, p2, chat_channel=self._tg_channel)
        except aiohttp.ClientConnectorError as e:
            print(f'web_broker: {e}')
        except Exception as e:
            print(f'web_broker: {e}')

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=True):
        """, coin, direction, quantity

        :param order_id: int, id of order
        :param coin: str, ticker
        :param action: int, direction for ticker
        :param price: float, price for LIM order
        :param target_quantity: float
        :return: bool
        """
        direction = 'BUY_LEG' if action > 0 else 'SELL_LEG'
        symbol, factor = self.broker_handler.symbol_to_market_with_factor(coin)
        price /= factor
        target_quantity *= factor
        config = {
            'legSymbol': symbol,
            'targetSize': target_quantity,
            'direction': direction,
            'comment': comment
        }

        self.logger.info(f'Received simple order: {config}')

        return True

    @property
    def get_id(self):
        self.id = str(uuid.uuid4())
        return self.id

    async def stop_order(self, order_id):
        return True


class MelanionSpreaderBroker:
    ACCOUNT_DICT = {
        'bin_spot':
            {
                'dummy': 'dummy',
            }
    }

    def __init__(self, market, broker_handler, account=1, chat_channel=None):
        self._tg_channel = chat_channel
        self.id = None
        self.broker_handler = broker_handler
        working_dir = 'output_melanion/'
        output = 'order.log'

        if 'bin' in market:
            self.exchange = 'bin_fut'
        else:
            raise ValueError('Bad market for Melanion broker')

        if (self.exchange not in MelanionSpreaderBroker.ACCOUNT_DICT or
                account not in MelanionSpreaderBroker.ACCOUNT_DICT[self.exchange]):
            raise ValueError('Bad exchange/account for Melanion broker')
        self.account_name = MelanionSpreaderBroker.ACCOUNT_DICT[self.exchange][account]
        self.logger = logging.getLogger('Melanion')
        handler = logging.FileHandler(filename=os.path.join(working_dir, output))
        handler.setFormatter(logging.Formatter('{asctime}:{levelname}:{name}:{message}', style='{'))
        self.logger.addHandler(handler)

    def get_contract_qty_from_coin(self, coin, qty):
        return self.broker_handler.get_contract_qty_from_coin(coin, qty)

    async def send_order(self, id, event, spread):
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, event.coin_name, spread, event.action, event.quantity,
                                                event.nature, event.comment)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        """

        :param id: int, id of order
        :param s1: str, ticker1
        :param s2: str, ticker2
        :param spread_shift: float, leeway for order acceptance in bp
        :param action: int, direction for ticker1
        :param target_size1: float
        :param target_size2: float
        :param prices: Tuple(float, float)
        :param comment: str
        :return: bool
        """

        with_tg_message = self._tg_channel != ''

        p1, p2 = prices
        spread = spread_shift
        max_order_size1 = target_size1 * 0.4  # TODO later
        # spread_s = np.format_float_positional(spread, 6, unique=False, fractional=False)
        # target_size_s = np.format_float_positional(target_size1, 6, unique=False, fractional=False)
        # max_order_size_s = np.format_float_positional(max_order_size1, 6, unique=False, fractional=False)

        spread = round(spread, 8)
        target_size1 = round(target_size1, 6)
        max_order_size1 = round(max_order_size1, 6)

        direction = 'BUY_LEG1' if action > 0 else 'SELL_LEG1'
        s1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1)
        s2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2)
        p1 /= factor1
        p2 /= factor2

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    await TGMessenger.send_message_async(session, f'{self.exchange}/{self.account_name}:{comment}',
                                                         s1, s2, direction,
                                                         spread, p1, p2, chat_channel=self._tg_channel)
        except aiohttp.ClientConnectorError as e:
            self.logger.warning(f'web_broker: {e}')
        except Exception as e:
            self.logger.warning(f'web_broker: {e}')

        self.logger.info(f'{direction},{s1},{s2},{p1},{p2},{comment}')

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=True):
        """, coin, direction, quantity

        :param order_id: int, id of order
        :param coin: str, ticker
        :param action: int, direction for ticker
        :param price: float, price for LIM order
        :param target_quantity: float
        :return: bool
        """

        return True

    @property
    def get_id(self):
        self.id = str(uuid.uuid4())
        return self.id

    async def stop_order(self, order_id):
        return True


class DummyBroker:
    def __init__(self, market, account=1):
        self.logger = logging.getLogger(f'broker_dummy_{market}_{account}')

    @staticmethod
    async def stop_all():
        return

    @staticmethod
    async def delete_all():
        return
    async def send_order(self, id, event, spread):
        self.logger.info(event, {'order': event})
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, event.ticker, spread, event.action, event.quantity,
                                                event.comment)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, comment):
        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=False):
        return True

    async def stop_order(self, order_id):
        return True

    @property
    def get_id(self):
        return 'xyz'

async def test_pair_bin():
    params = {
        'exchange_trade': 'binancefut',
        'account_trade': 'mel_cm3'
    }
    bh = BrokerHandler(market_watch='binancefut', strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market='bin_fut', broker_handler=bh, account='mel_cm3', chat_channel='')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    s1 = 'XRPUSDT'
    s2 = 'ADAUSDT'
    price1 = 0.5809
    price2 = 0.5815
    target_size1 = 35
    target_size2 = target_size1 * price1 / price2
    action = -1

    await wb.send_pair_order(id, s1, s2, 10.0, action=action, target_size1=target_size1,
                             target_size2=target_size2, prices=(price1, price2), nature='xx', comment='test')
    index = 0
    wsl.subscribe(id)
    while index < 100:
        await asyncio.sleep(1)
        result = wsl.get_strat_result(id)
        print(result)
        if 'STOPPED' in result.get('state', '') or 'reached' in result.get('info', '') and 'TERMINATED' in result.get(
                'state', ''):
            index = 1e5
        index = index + 1
    await wsl.stop_listener(id)
    await wb.stop_order(id)


async def test_single():
    # TODO : modifier params
    bh = BrokerHandler('bybit', 'bybit', None, 'bybit')
    wb = WebSpreaderBroker('bybit', bh, 1, 'CM')
    wsl = WebSpreaderListener(logging.getLogger())
    id = wb.get_id
    print(id)
    await wb.send_simple_order(id, 'STORJUSDT', 1, 0.639, 824.3, 'test', False)
    wsl.subscribe(id)
    index = 0
    while index < 600:
        await asyncio.sleep(1)
        print(wsl.get_strat_result(id))
        index = index + 1
    await wsl.stop_listener(id)
    await wb.stop_order(id)


async def test_single_bin():
    exchange_trade = 'binancefut'#'binancefut' 'bybit'
    account_trade = 'mel_cm3'#'mel_cm3' 1
    params = {
        'exchange_trade': exchange_trade, #'binancefut',
        'account_trade': account_trade #'mel_cm3'
    }
    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    target_amnt = 1000
    coin = 'VETUSDT' #'XRPUSDT'
    action = 1
    nature = 'x'
    ticker = await ep._exchange_async.fetch_ticker(coin)
    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    target_quantity = target_amnt / price
    await wb.send_simple_order(id, coin=coin, action=action, price=price, target_quantity=target_quantity,
                               comment='test', nature=nature,
                               translate_qty_incontracts=False, use_algo=True)
    wsl.subscribe(id)
    index = 0
    max_time = 3000 / 5
    while index < max_time:
        await asyncio.sleep(5)
        result = wsl.get_strat_result(id)
        state = result.get('state', '')
        exq = float(result.get('execQty', 0))
        print(state, ':', exq)
        if np.abs(exq - action * target_quantity) < 1e-3:
            index = max_time
        print(100 * np.abs(exq) / target_quantity, '%')
        if 'topped' in state:
            index = max_time
        index = index + 1
    await wsl.stop_listener()
    await wb.stop_simple_order(id)
    await ep._exchange_async.close()

async def do_single_binspot():
    exchange_trade = 'binance'#'binancefut' 'bybit'
    account_trade = 2
    params = {
        'exchange_trade': exchange_trade,
        'account_trade': account_trade
    }
    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    target_amnt = 10
    coin = 'BNBUSDT' #'XRPUSDT'
    action = 1
    nature = 'n'
    ticker = await ep._exchange_async.fetch_ticker(coin)
    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    target_quantity = target_amnt / price
    await wb.send_simple_order(id, coin=coin, action=action, price=price, target_quantity=target_quantity,
                               comment='test', nature=nature,
                               translate_qty_incontracts=False, use_algo=True)
    wsl.subscribe(id)
    index = 0
    max_time = 3000 / 5
    while index < max_time:
        await asyncio.sleep(5)
        result = wsl.get_strat_result(id)
        state = result.get('state', '')
        exq = float(result.get('execQty', 0))
        prc = float(result.get('avgPrc', 0))
        print(state, ':', exq, prc)
        if np.abs(exq - action * target_quantity) < 1e-3:
            index = max_time
        print(100 * np.abs(exq) / target_quantity, '%')
        if 'topped' in state:
            index = max_time
        index = index + 1
    await wsl.stop_listener()
    await wb.stop_simple_order(id)
    await ep._exchange_async.close()

async def do_single_bitget():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)
    exchange_trade = 'bitget'#'binancefut' 'bybit'
    account_trade = 1
    params = {
        'exchange_trade': exchange_trade,
        'account_trade': account_trade
    }
    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    #target_amnt = 10
    coin = 'ARCUSDT' #'XRPUSDT'
    action = 1
    nature = 'x'
    ticker = await ep._exchange_async.fetch_ticker(coin)
    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    target_quantity = 567 # target_amnt / price
    await wb.send_simple_order(id, coin=coin, action=action, price=price, target_quantity=target_quantity,
                               comment='test', nature=nature,
                               translate_qty_incontracts=False, use_algo=True)
    wsl.subscribe(id)
    index = 0
    max_time = 3000 / 5
    while index < max_time:
        await asyncio.sleep(5)
        result = wsl.get_strat_result(id)
        state = result.get('state', '')
        exq = float(result.get('execQty', 0))
        prc = float(result.get('avgPrc', 0))
        print(state, ':', exq, prc)
        if np.abs(exq - action * target_quantity) < 1e-3:
            index = max_time
        print(100 * np.abs(exq) / target_quantity, '%')
        if 'topped' in state:
            index = max_time
        index = index + 1
    await wsl.stop_listener()
    await wb.stop_simple_order(id)
    await ep._exchange_async.close()

async def test_listener():
    wsl = WebSpreaderListener(logger=logging.getLogger())
    wsl.subscribe('r2d2')
    await asyncio.sleep(10000)

def test_okx():
    import ccxt
    ep = ccxt.okx({
                'apiKey': 'f73fcf80-9654-4120-85d4-2034a9ce9138',
                'secret': '5C79D5110D67520374E8B1A536E9A2D1',
                'password': 'S5ljB4t*a%lGVS3&MA',
                'enableRateLimit': True,
                'timeout': 60000
        })

    # sc1
    # ep = ccxt.okx({
    #     'apiKey': '5b511367-9044-4624-985b-d7a93258e4e5',
    #     'secret': '36F7A564EA41A2F5079DA61E4D37AF8F',
    #     'password': 'H$aZd!ung%7wK3Pg',
    #     'enableRateLimit': True,
    #     'timeout': 60000
    # })
    # sc2
    # ep = ccxt.okx({
    #     'apiKey': '3c4dac35-ef1a-4d76-b9d3-2317e4ed154e',
    #     'secret': "F3DFB6300923484D04D2A8E55E8AAC4A",
    #     'password': 'A7&AUWgJp679',
    #     'enableRateLimit': True,
    #     'timeout': 60000
    # })
    # sc3
    # ep = ccxt.okx({
    #     'apiKey': '9cd2f4c9-5d9e-4a25-8c89-5f5b588309ba',
    #     'secret': '876987F6F1475D9DD0BA6BD7D8726A47',
    #     'password': '7pfeFKcgWFgGDYJ2!',
    #     'enableRateLimit': True,
    #     'timeout': 60000
    # })
    # rez = ep.private_post_account_bills_history_archive(params={'year': '2022', 'quarter': 'Q1'})
    rez = ep.private_post_account_bills_history_archive(params={'year': '2022', 'quarter': 'Q1'})
    print(rez)

if __name__ == '__main__':
    asyncio.run(do_single_bitget())
    # asyncio.run(do_single_binspot)
'''

SB
    IP:8080/wsapi/strat/update/ab15f001-d541-4b5c-b413-f5fb6e77cb1d
    /api/strat/start/{strat UID}
    /api/strat/stop/{strat UIID}
    /api/strat/delete/{strat UID}

export interface ManualOrderConfiguration {
    name: string,
    strategyName: string,
    symbol: string,
    exchange: string,
    account: string,
    offset: Offset,
    useMargin: boolean,
    targetSize: number,
    price: number,
    maxOrderSize: number,
    maxAliveOrderTime: number,
    direction:  Side,
    type: OrderType,
    childOrderDelay: number,
    start: Start
    }
export interface StrategyOrderRequest {
  clientOrderId: string,
  orderMsg: OrderMsg,
  spreaderConfiguration?: SpreaderConfiguration
  manualOrderConfiguration?: ManualOrderConfiguration
}
{'avgPrc': '0', 'state': 'LiveOrder', 'execQty': '0', 'info': ''}
{'avgPrc': '0.5914', 'state': 'RetriedLiveOrder', 'execQty': '17', 'info': ''}
{'avgPrc': '0.591267', 'state': 'RetriedLiveOrder', 'execQty': '51', 'info': ''}
{'avgPrc': '0.592283', 'state': 'Stopped', 'execQty': '-102', 'info': 'targetSize reached'}

{}
{'leg2AvgExecPrc': '0', 'pricingTimestamp': '1712160079707', 'premium': '-6.9036', 'leg2ExecQty': '0', 'leg1ExecQty': '0', 'delayed': 'false', 'state': 'BOTH_LEG_SENT', 'leg1AvgExecPrc': '0', 'info': ''}

{'leg2AvgExecPrc': '0', 'pricingTimestamp': '1712159785707', 'premium': '-37.7173', 
'leg2ExecQty': '0', 'leg1ExecQty': '0', 
'delayed': 'false', 'state': 'WAITING_SIGNAL', 'leg1AvgExecPrc': '0', 'info': ''}
{'leg2AvgExecPrc': '0.5811', 'pricingTimestamp': '1712159641707', 'premium': '-6.8923', 
'leg2ExecQty': '-258', 'leg1ExecQty': '259', 'delayed': 'false', 'state': 'TERMINATED', 
'leg1AvgExecPrc': '0.5801', 'info': 'targetSize reached'}


Process finished with exit code 0
    
'''
