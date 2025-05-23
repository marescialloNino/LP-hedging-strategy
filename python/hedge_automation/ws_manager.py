import asyncio
import logging
from datetime import datetime
from .ws_listener import WebSpreaderListener

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self):
        self.listener = None
        self.active_orders = {}
        self.order_timeouts = {}
        self.update_callback = None
        self.monitor_task = None
        self.running = False

    async def start_listener(self, update_callback):
        logger.info("Starting WebSocket listener")
        if self.listener is None or not self.listener._connected:
            self.listener = WebSpreaderListener()
            self.update_callback = update_callback
            self.running = True
            if self.monitor_task is None or self.monitor_task.done():
                self.monitor_task = asyncio.create_task(self.monitor_orders())
        else:
            logger.info("WebSocket listener already running")

    async def stop_listener(self):
        logger.info("Stopping WebSocket listener")
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                logger.info("Monitor task cancelled")
        if self.listener:
            await self.listener.stop()
        self.listener = None
        self.active_orders.clear()
        self.order_timeouts.clear()
        logger.info("WebSocket listener stopped, all orders cleared")

    async def subscribe_order(self, order_data):
        order_id = order_data.get("orderId")
        if not order_id:
            logger.error("No orderId provided for subscription")
            raise ValueError("No orderId provided")

        if not self.listener or not self.listener._connected:
            logger.warning(f"WebSocket listener not connected, attempting to reinitialize for order {order_id}")
            self.listener = WebSpreaderListener()
            await self.listener._initialize()
            logger.info("WebSocket listener initialized")

        logger.info(f"Subscribing to strat_id: {order_id}")
        await self.listener.subscribe(order_id)
        logger.info(f"Subscribed to order: {order_id}")

        target_size = order_data.get("Rebalance Value", 0.0)
        max_time = 90.0  # Default timeout
        self.active_orders[order_id] = {
            "order_data": order_data,
            "target_size": target_size,
            "max_time": max_time
        }
        self.order_timeouts[order_id] = datetime.now().timestamp() + max_time
        logger.info(f"Order {order_id}: targetSize={target_size}, max_time={max_time}s")
        logger.info(f"Added order {order_id} with timeout {max_time}s")

    async def monitor_orders(self):
        logger.info("Starting WebSocket monitor_orders")
        while self.running or self.active_orders:
            try:
                if not self.listener or not self.listener._connected:
                    logger.warning("WebSocket listener not connected, attempting reinitialization")
                    self.listener = WebSpreaderListener()
                    await self.listener._initialize()
                    for order_id in self.active_orders:
                        await self.listener.subscribe(order_id)
                    logger.info("WebSocket listener reinitialized and subscriptions restored")

                orders_to_remove = []
                for order_id, order_info in self.active_orders.items():
                    order_data = order_info["order_data"]
                    target_size = order_info["target_size"]
                    max_time = order_info["max_time"]

                    try:
                        result = await self.listener.get_strat_result(order_id)
                        if not result:
                            logger.debug(f"No results yet for order {order_id}")
                            continue

                        exec_qty = float(result.get("execQty", 0.0))
                        state = result.get("state", "Unknown")
                        info = result.get("info", "")

                        fill_percentage = abs(exec_qty / target_size) if target_size != 0 else 0.0
                        status = "EXECUTING"
                        current_time = datetime.now().timestamp()

                        if abs(fill_percentage - 1.0) <= 0.03:
                            status = "SUCCESS"
                            orders_to_remove.append(order_id)
                        elif current_time > self.order_timeouts.get(order_id, 0):
                            status = "EXECUTION_ERROR"
                            orders_to_remove.append(order_id)

                        order_data.update({
                            "status": status,
                            "fillPercentage": fill_percentage,
                            "Token": order_data.get("Token", "UNKNOWN"),
                            "Rebalance Action": order_data.get("Rebalance Action", ""),
                            "Rebalance Value": target_size
                        })

                        if self.update_callback:
                            await self.update_callback(order_data)

                        logger.info(f"Order {order_id} updated: status={status}, fillPercentage={fill_percentage:.2%}, state={state}, info={info}")

                    except Exception as e:
                        logger.error(f"Error processing order {order_id}: {e}")

                for order_id in orders_to_remove:
                    self.active_orders.pop(order_id, None)
                    self.order_timeouts.pop(order_id, None)
                    logger.info(f"Removed resolved order {order_id}")

                await asyncio.sleep(30)

            except asyncio.CancelledError:
                logger.info("Monitor_orders task cancelled")
                break
            except Exception as e:
                logger.error(f"Monitor_orders error: {e}", exc_info=True)
                await asyncio.sleep(1)

        logger.info("Monitor_orders task completed: no active orders remaining")

ws_manager = WebSocketManager()