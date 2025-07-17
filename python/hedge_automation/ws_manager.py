import asyncio
import logging
import math
from datetime import datetime
from .ws_listener import WebSpreaderListener

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self):
        self.listener = WebSpreaderListener()
        self.active_orders = {}
        self.order_timeouts = {}
        self.update_callback = None
        self.monitor_task = None
        self.running = False
        self.POLL_INTERVAL = 20  # seconds

    async def start_listener(self, update_callback):
        logger.info("Starting WebSocket listener")
        if not self.running:
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
        await self.listener.stop_listener()
        self.active_orders.clear()
        self.order_timeouts.clear()
        logger.info("WebSocket listener stopped, all orders cleared")

    async def subscribe_order(self, order_data):
        order_id = order_data.get("orderId")
        if not order_id:
            logger.error("No orderId provided for subscription")
            raise ValueError("No orderId provided")

        logger.info(f"Subscribing to order: {order_id}")
        self.listener.subscribe(order_id)

        # Extract parameters for max_time calculation
        target_size = order_data.get("Rebalance Value", 0.0)
        max_time = 600.0  # Default timeout

        try:
            result = self.listener.get_strat_result(order_id)
            if result and "manualOrderConfiguration" in result:
                config = result["manualOrderConfiguration"]
                max_order_size = float(config.get("maxOrderSize", 0.0))
                max_alive_order_time = float(config.get("maxAliveOrderTime", 6000))
                child_order_delay = float(config.get("childOrderDelay", 0))
                max_retry_as_limit_order = float(config.get("maxRetryAsLimitOrder", 0))

                if target_size > 0 and max_order_size > 0:
                    max_time = (
                        math.ceil(max_order_size / target_size) *
                        (max_alive_order_time + child_order_delay) / 1000 *
                        (max_retry_as_limit_order + 1) +
                        20
                    )
                    logger.debug(
                        f"Calculated max_time for order {order_id}: "
                        f"ceil({max_order_size}/{target_size}) * "
                        f"({max_alive_order_time} + {child_order_delay})/1000 * "
                        f"({max_retry_as_limit_order} + 1) + 20 = {max_time}s"
                    )
                else:
                    logger.warning(
                        f"Invalid sizes for max_time calculation for {order_id}: "
                        f"target_size={target_size}, max_order_size={max_order_size}"
                    )
        except Exception as e:
            logger.warning(f"Failed to calculate max_time for {order_id}: {e}, using default {max_time}s")

        self.active_orders[order_id] = {
            "order_data": order_data,
            "target_size": target_size,
            "max_time": max_time
        }
        self.order_timeouts[order_id] = datetime.now().timestamp() + max_time
        logger.info(f"Order {order_id}: targetSize={target_size}, max_time={max_time}s")
        logger.info(f"Added order {order_id} with timeout {self.order_timeouts[order_id]}")

    async def monitor_orders(self):
        logger.info("Starting WebSocket monitor_orders")
        while self.running or self.active_orders:
            try:
                orders_to_remove = []
                for order_id, order_info in self.active_orders.items():
                    order_data = order_info["order_data"]
                    target_size = order_info["target_size"]
                    max_time = order_info["max_time"]

                    try:
                        result = self.listener.get_strat_result(order_id)
                        if not result:
                            logger.debug(f"No results yet for order {order_id}")
                            continue

                        exec_qty = float(result.get("execQty", 0.0))
                        state = result.get("state", "Unknown")
                        info = result.get("info", "")
                        average_price = float(result.get("avgPrc", 0.0))

                        fill_percentage = abs(exec_qty / target_size) if target_size != 0 else 0.0
                        status = "EXECUTING"
                        current_time = datetime.now().timestamp()

                        if abs(fill_percentage - 1.0) <= 0.03 or info == "targetSize reached":
                            status = "SUCCESS"
                            orders_to_remove.append(order_id)
                        elif current_time > self.order_timeouts.get(order_id, 0):
                            status = "EXECUTION_ERROR"
                            orders_to_remove.append(order_id)

                        order_data.update({
                            "status": status,
                            "fillPercentage": fill_percentage,
                            "Token": order_data.get("Token", "UNKNOWN"),
                            "avgPrice": average_price,
                            "Rebalance Action": order_data.get("Rebalance Action", ""),
                            "Rebalance Value": target_size
                        })

                        if self.update_callback:
                            # Run callback in a new task to ensure proper context
                            asyncio.create_task(self.update_callback(order_data))

                        logger.info(f"Order {order_id} updated: status={status}, fillPercentage={fill_percentage:.2%}, state={state}, info={info}, avgPrice={average_price}")

                    except Exception as e:
                        logger.error(f"Error processing order {order_id}: {e}", exc_info=True)

                for order_id in orders_to_remove:
                    await self.listener.stop_listener(order_id)
                    self.active_orders.pop(order_id, None)
                    self.order_timeouts.pop(order_id, None)
                    logger.info(f"Removed and unsubscribed resolved order {order_id}")

                await asyncio.sleep(self.POLL_INTERVAL)

            except asyncio.CancelledError:
                logger.info("Monitor_orders task cancelled")
                break
            except Exception as e:
                logger.error(f"Monitor_orders error: {e}", exc_info=True)
                await asyncio.sleep(1)

        logger.info("Monitor_orders task completed: no active orders remaining")

ws_manager = WebSocketManager()