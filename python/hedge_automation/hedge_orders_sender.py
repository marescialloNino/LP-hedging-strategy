import asyncio
import aiohttp
import logging
import uuid
import numpy as np
import json
import sys
import os
from dotenv import load_dotenv

load_dotenv()

EXECUTION_IP = os.getenv("EXECUTION_IP")

# Fix for Windows event loop issue
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from .data_handler import BrokerHandler

class BitgetOrderSender:
    AMAZON_URL = f'http://{EXECUTION_IP}:8080/api'
    AMAZON_UPI_SINGLE = AMAZON_URL + '/manualOrder/createOrUpdate'
    
    def __init__(self, broker_handler):
        """
        Initialize the Bitget order sender with a pre-configured BrokerHandler
        
        :param broker_handler: Instance of BrokerHandler configured for Bitget
        """
        self.broker_handler = broker_handler
        self.exchange = 'bitget_fut'
        self.account_name = 'hedge1'
        self.logger = logging.getLogger(f'bitget_order_sender-exchange:{self.exchange}-account_name:{self.account_name}')
        
    async def _post_request(self, url, json_data):
        """Helper method to send POST requests"""
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=json_data) as resp:
                response = await resp.read()
                return {'status_code': resp.status, 'text': response}

    async def _fetch_last_price(self, ticker):
        """Fetch the last closing price for the given ticker"""
        try:
            symbol, _ = self.broker_handler.symbol_to_market_with_factor(ticker, universal=False)
            # Wrap the fetch call in a task to ensure there’s an active task context.
            fetch_task = asyncio.create_task(
                self.broker_handler._end_point_trade._exchange_async.fetch_ohlcv(
                    symbol, timeframe='1m', limit=1
                )
            )
            ohlcv = await fetch_task
            if ohlcv and len(ohlcv) > 0:
                last_price = ohlcv[0][4]  # Close price
                self.logger.info(f"Fetched last price for {ticker}: ${last_price}")
                return last_price
            else:
                self.logger.error(f"No OHLCV data returned for {ticker}")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching price for {ticker}: {str(e)}")
            return None

    async def send_order(self, ticker, direction, hedge_qty, price=None):
        """
        Send a single order to Bitget using leeway algorithm with a $1000 threshold
        """
        print("DEBUG: Entering send_order method", flush=True)
        order_id = str(uuid.uuid4())
        direction_str = 'BUY' if direction > 0 else 'SELL'
        
        symbol, factor = self.broker_handler.symbol_to_market_with_factor(ticker, universal=False)
        target_quantity = hedge_qty * factor

        if price is None:
            price = await self._fetch_last_price(ticker)
            if price is None:
                self.logger.warning(f"Using full quantity as max_order_size due to missing price")
                print(f"Warning: Could not fetch price for {ticker}", flush=True)
        
        if price is not None:
            price /= factor

        qty = self.broker_handler.get_contract_qty_from_coin(ticker, target_quantity)
        self.logger.info(f'Converted {target_quantity} {ticker} to {qty} contracts')

        print("DEBUG: Before detailed output", flush=True)

        child_order_delay = 1000 * np.random.uniform(0.2, 1)
        max_alive_order_time = 1000 * np.random.uniform(5, 8)
        
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

        config = {
            'symbol': symbol,
            'exchange': 'bitget_fut',
            'account': self.account_name,
            'offset': 'Open',
            'useMargin': True,
            'targetSize': round(qty, 6),
            'maxOrderSize': round(max_order_size, 6),
            'maxAliveOrderTime': max_alive_order_time,
            'direction': direction_str,
            'childOrderDelay': child_order_delay,
            'start': 'Manual',
            'type': 'LIMIT_WITH_LEEWAY',
            'maxStratTime': 30 * 60 * 1000,
            'timeInForce': 'GTC',
            'clientRef': 'hedge automation',
            'maxRetryAsLimitOrder': 10,
            'marginMode': 'ISOLATED'
        }
        
        request = {
            'clientOrderId': order_id,
            'orderMsg': 0,
            'manualOrderConfiguration': config
        }

        # Print statements NOW, after all variables are defined
        print("\n=== Sending Order ===", flush=True)
        print(f"Ticker: {ticker}", flush=True)
        print(f"Direction: {direction_str}", flush=True)
        print(f"Quantity: {qty} contracts", flush=True)
        print(f"Max Order Size: {max_order_size} contracts", flush=True)
        if price:
            print(f"Price: ${price:.2f}", flush=True)
            print(f"Total Amount: ${amount:.2f}", flush=True)
        print("\nPOST Request:", flush=True)
        print(json.dumps(request, indent=2), flush=True)

        print("DEBUG: Before dummy check", flush=True)
        
        if self.broker_handler._destination == 'dummy':
            self.logger.info(f"Dummy order: {symbol} {direction_str} {qty} contracts (ID: {order_id})")
            print(f"Dummy order: {symbol} {direction_str} {qty} contracts (ID: {order_id})", flush=True)
            return True, request
        
        self.logger.info(f"Sending order: {symbol} {direction_str} {qty} contracts perspectivas chunk: {max_order_size})")
        try:
            response = await self._post_request(self.AMAZON_UPI_SINGLE, request)
            if response['status_code'] != 200:
                self.logger.error(f"Order failed: {response['status_code']} - {response['text']}")
                print(f"Order failed: {response['status_code']} - {response['text'].decode()}", flush=True)
                return False, request
                
            self.logger.info(f"Order successfully submitted: {order_id}")
            print(f"Order successfully submitted: {order_id}", flush=True)
            return True, request
            
        except Exception as e:
            self.logger.error(f"Error sending order: {str(e)}")
            print(f"Error sending order: {str(e)}", flush=True)
            return False
        

    async def close(self):
        """Close the exchange connection"""
        await self.broker_handler.close_exchange_async()

async def test_order_sender():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Set up BrokerHandler for Bitget in real mode
    params = {
        'exchange_trade': 'bitget',
        'account_trade': 'hedge1',
        'send_orders': 'bitget'  # Change from 'dummy' to 'bitget' or remove this key
    }
    
    end_point = BrokerHandler.build_end_point('bitget', account='H1')  # 'H1' matches BITGET_HEDGE1_API_KEY
    bh = BrokerHandler(
        market_watch='bitget',
        strategy_param=params,
        end_point_trade=end_point,
        logger_name='bitget_order_sender'
    )
    
    # Create order sender
    order_sender = BitgetOrderSender(bh)
    
    try:
        # Test parameters
        ticker = 'ETHUSDT'
        direction = 1  # Buy
        hedge_qty = 0.3  # 0.5 ETH
        
        # Send the order to the real execution machine
        success, request = await order_sender.send_order(ticker, direction, hedge_qty)
        print(f"\nTest completed with result: Success={success}, Request={request}")
        
        if success:
            print("Order sent successfully to the execution machine!")
        else:
            print("Failed to send order. Check logs for details.")
    
    finally:
        # Cleanup
        await order_sender.close()

if __name__ == "__main__":
    asyncio.run(test_order_sender())
