import asyncio
import logging
import math
from hedge_automation.ws_listener import WebSpreaderListener 

logger = logging.getLogger('ws_manager')

class WebSocketManager:
    def __init__(self):
        self.listener = None
        self.active_orders = set()
        self.order_timeouts = {}
        self.task = None
        self.order_statuses = []
        self.update_callback = None

    async def initialize_listener(self):
        """Initialize WebSocket listener with retries."""
        for attempt in range(3):
            try:
                self.listener = WebSpreaderListener(logger)
                logger.info("WebSocket listener initialized")
                return True
            except Exception as e:
                logger.error(f"Failed to initialize WebSocket listener (attempt {attempt + 1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
        logger.error("Failed to initialize WebSocket listener after 3 attempts")
        return False

    async def start_listener(self, update_callback):
        """Start the WebSocket listener if not running."""
        if self.task is None:
            logger.info("Starting WebSocket listener")
            if not await self.initialize_listener():
                logger.error("Cannot start listener: initialization failed")
                return None
            self.update_callback = update_callback
            self.task = asyncio.create_task(self.monitor_orders())
        return self.task

    async def subscribe_order(self, order_data):
        """Subscribe a new order and update timeout."""
        order_id = order_data['orderId']
        if order_id in self.active_orders:
            logger.info(f"Order {order_id} already subscribed")
            return

        if self.listener is None or not self.listener._connected:
            logger.warning(f"WebSocket listener not connected, attempting to reinitialize for order {order_id}")
            if not await self.initialize_listener():
                raise Exception("WebSocket listener initialization failed")

        try:
            self.listener.subscribe(order_id)
            self.active_orders.add(order_id)
            logger.info(f"Subscribed to order: {order_id}")
        except Exception as e:
            logger.error(f"Failed to subscribe order {order_id}: {e}")
            raise

        result = self.listener.get_strat_result(order_id)
        if not result:
            await asyncio.sleep(1)
            result = self.listener.get_strat_result(order_id)
        max_time = 1800
        if result:
            target_size = result.get('targetSize', 0)
            max_order_size = result.get('maxOrderSize', 0)
            max_alive_order_time = result.get('maxAliveOrderTime', 0)
            child_order_delay = result.get('childOrderDelay', 0)
            max_retry = result.get('maxRetryAsLimitOrder', 0)
            if max_order_size > 0:
                num_child_orders = math.ceil(target_size / max_order_size)
                max_time_ms = (num_child_orders * (max_alive_order_time + child_order_delay) * max_retry) + 10000
                max_time = max_time_ms / 1000
                logger.info(f"Order {order_id}: targetSize={target_size}, maxOrderSize={max_order_size}, num_child_orders={num_child_orders}, max_time={max_time}s")
        self.order_timeouts[order_id] = {'timeout': max_time, 'start_time': asyncio.get_event_loop().time()}
        
        self.order_statuses.append(order_data)
        logger.info(f"Added order {order_id} with timeout {max_time}s")

    async def monitor_orders(self):
        """Monitor subscribed orders until all resolve."""
        try:
            while self.active_orders:
                if self.listener is None or not self.listener._connected:
                    logger.error("WebSocket listener disconnected, attempting to reinitialize")
                    if not await self.initialize_listener():
                        logger.error("WebSocket listener reinitialization failed")
                        break

                for order_id in list(self.active_orders):
                    result = self.listener.get_strat_result(order_id)
                    if not result:
                        continue
                    
                    exec_qty = result.get('execQty', 0)
                    target_size = result.get('targetSize', 0)
                    fill_percentage = abs(exec_qty / target_size) if target_size != 0 else 0.0
                    is_filled = target_size != 0 and abs(fill_percentage - 1.0) <= 0.03
                    status = "SUCCESS" if is_filled else "EXECUTING"
                    
                    if order_id in self.order_timeouts:
                        elapsed = asyncio.get_event_loop().time() - self.order_timeouts[order_id]['start_time']
                        if elapsed > self.order_timeouts[order_id]['timeout']:
                            status = "EXECUTION_ERROR"
                    
                    for order_info in self.order_statuses:
                        if order_info["orderId"] == order_id:
                            order_info["status"] = status
                            order_info["fillPercentage"] = fill_percentage
                            if self.update_callback:
                                await self.update_callback(order_info.copy())
                            break
                    
                    if status in ["SUCCESS", "EXECUTION_ERROR"]:
                        self.active_orders.discard(order_id)
                        self.order_timeouts.pop(order_id, None)
                        logger.info(f"Order {order_id} resolved: {status}")
                
                await asyncio.sleep(30)
            
            summary = {
                "Total Orders": len(self.order_statuses),
                "Received": sum(1 for o in self.order_statuses if o["status"] == "RECEIVED"),
                "Executing": sum(1 for o in self.order_statuses if o["status"] == "EXECUTING"),
                "Success": sum(1 for o in self.order_statuses if o["status"] == "SUCCESS"),
                "Submission Error": sum(1 for o in self.order_statuses if o["status"] == "SUBMISSION_ERROR"),
                "Execution Error": sum(1 for o in self.order_statuses if o["status"] == "EXECUTION_ERROR")
            }
            logger.info(f"Run Summary: {summary}")
        
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"WebSocket listener error: {str(e)}")
        finally:
            await self.stop_listener()

    async def stop_listener(self):
        """Stop the WebSocket listener if running."""
        if self.task is not None:
            self.task.cancel()
            if self.listener:
                await self.listener.stop_listener()
                self.listener = None
            self.task = None
            self.active_orders.clear()
            self.order_timeouts.clear()
            self.order_statuses = []
            self.update_callback = None
            logger.info("WebSocket listener stopped")

ws_manager = WebSocketManager()