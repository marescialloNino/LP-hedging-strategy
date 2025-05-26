import logging
import os
from datetime import datetime
import pandas as pd
from pathlib import Path
from pywebio.output import put_markdown, put_code, toast
import asyncio
import json
from common.utils import execute_hedge_trade
from common.path_config import HEDGING_LATEST_CSV, MANUAL_ORDER_MONITOR_CSV, ORDER_HISTORY_CSV
from hedge_automation.ws_manager import WebSocketManager
from dotenv import load_dotenv
from common.bot_reporting import TGMessenger

load_dotenv()

logger = logging.getLogger('hedge_execution')
ws_manager = WebSocketManager()

def send_telegram_alert(message):
    """Send alert message to configured Telegram channel."""
    print(f"Sending alert: {message}")
    try:
        response = TGMessenger.send(message, 'LP eagle')
        if isinstance(response, dict) and not response.get("ok", False):
            logger.error(f"Telegram response error: {response}")
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")

async def append_to_order_history(order_data, source):
    """Append a resolved order to order_history.csv."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status", "Source"]
    order_data["Source"] = source
    df_new = pd.DataFrame([order_data], columns=headers)
    
    if ORDER_HISTORY_CSV.exists():
        df = pd.read_csv(ORDER_HISTORY_CSV)
        df = pd.concat([df, df_new], ignore_index=True)
    else:
        df = df_new
    
    df.to_csv(ORDER_HISTORY_CSV, index=False)
    logger.info(f"Appended to {ORDER_HISTORY_CSV}: {order_data['orderId']} ({source})")

async def update_manual_order_monitor_csv(order_data):
    """Update manual_order_monitor.csv by modifying or appending a row."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status", "fillPercentage"]
    
    if MANUAL_ORDER_MONITOR_CSV.exists():
        df = pd.read_csv(MANUAL_ORDER_MONITOR_CSV)
        mask = df["orderId"] == order_data["orderId"]
        if mask.any():
            for key, value in order_data.items():
                if key in df.columns:
                    df.loc[mask, key] = value
        else:
            df = pd.concat([df, pd.DataFrame([order_data])], ignore_index=True)
    else:
        df = pd.DataFrame([order_data], columns=headers)
    
    df.to_csv(MANUAL_ORDER_MONITOR_CSV, index=False)
    logger.info(f"Updated {MANUAL_ORDER_MONITOR_CSV} for {order_data['Token']}: {order_data['status']}")

class HedgeActions:
    def __init__(self, order_sender):
        self.order_sender = order_sender
        self.hedge_processing = {}
        self.active_orders = set()
        self.SUBSCRIPTION_RETRIES = 3
        self.SUBSCRIPTION_RETRY_DELAY = 2  # seconds

    async def on_order_update(self, order_info):
        """Handle WebSocket order update messages from ws_manager."""
        try:
            logger.debug(f"Received WebSocket update: {order_info}")
            order_id = order_info.get("orderId")
            status = order_info.get("status")
            fill_percentage = float(order_info.get("fillPercentage", 0.0))
            token = order_info.get("Token", "UNKNOWN")

            if not order_id or not status:
                logger.warning(f"Invalid WebSocket update: {order_info}")
                return

            # Update order data
            order_data = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Token": token,
                "Rebalance Action": order_info.get("Rebalance Action", ""),
                "Rebalance Value": float(order_info.get("Rebalance Value", 0.0)),
                "orderId": order_id,
                "status": status,
                "fillPercentage": round(fill_percentage * 100, 2)
            }

            # Update manual_order_monitor.csv
            await update_manual_order_monitor_csv(order_data)

            # Send Telegram alert for status changes
            alert_message = (
                f"Order Update:\n"
                f"Token: {token}\n"
                f"Action: {order_data['Rebalance Action']}\n"
                f"Quantity: {order_data['Rebalance Value']:.5f}\n"
                f"Order ID: {order_id}\n"
                f"Status: {status}\n"
                f"Fill Percentage: {order_data['fillPercentage']:.2f}%"
            )
            send_telegram_alert(alert_message)

            # If order is resolved, append to history and clean up
            if status in ["SUCCESS", "EXECUTION_ERROR"]:
                await append_to_order_history(order_data, "Manual")
                if MANUAL_ORDER_MONITOR_CSV.exists():
                    df = pd.read_csv(MANUAL_ORDER_MONITOR_CSV)
                    df = df[df["orderId"] != order_id]
                    df.to_csv(MANUAL_ORDER_MONITOR_CSV, index=False)
                    logger.info(f"Removed resolved order {order_id} from {MANUAL_ORDER_MONITOR_CSV}")
                if order_id in self.active_orders:
                    self.active_orders.remove(order_id)
                    logger.info(f"Order {order_id} removed from active_orders. Remaining: {len(self.active_orders)}")
                    if not self.active_orders:
                        logger.info("All manual orders resolved")
                toast(f"Order {order_id} for {token} {status.lower()}", duration=5, color="success" if status == "SUCCESS" else "error")

        except Exception as e:
            logger.error(f"Error processing WebSocket update: {e}", exc_info=True)
            send_telegram_alert(f"WebSocket Error:\nToken: {token}\nOrder ID: {order_id}\nError: {str(e)}")

    async def process_manual_order_result(self, result, token, action, quantity, timestamp):
        """Process the result of a manual order, update CSV, and subscribe to listener."""
        order_id = result['request']['clientOrderId'] if 'request' in result and 'clientOrderId' in result['request'] else ""
        logger.debug(f"Processing manual order result for {token}: order_id={order_id}, success={result['success']}")

        if not order_id:
            logger.warning(f"No clientOrderId in result for {token}: {result}")
            toast(f"Order ID missing for {token}, cannot track", duration=5, color="error")
            return

        order_data = {
            "Timestamp": timestamp,
            "Token": token,
            "Rebalance Action": action,
            "Rebalance Value": abs(quantity),
            "orderId": order_id,
            "status": "RECEIVED",
            "fillPercentage": 0.0
        }

        await update_manual_order_monitor_csv(order_data)
        logger.info(f"Logged manual order to {MANUAL_ORDER_MONITOR_CSV}: {order_data}")

        if result['success']:
            order_data["status"] = "EXECUTING"
            await update_manual_order_monitor_csv(order_data)
            logger.info(f"Order {order_id} for {token} marked as EXECUTING")
            self.active_orders.add(order_id)
            logger.info(f"Order {order_id} added to active_orders. Total active: {len(self.active_orders)}")
            
            subscribed = False
            for attempt in range(self.SUBSCRIPTION_RETRIES):
                try:
                    if not ws_manager.running:
                        await ws_manager.start_listener(update_callback=self.on_order_update)
                        logger.info("WebSocket listener started")
                    await asyncio.sleep(self.SUBSCRIPTION_RETRY_DELAY)
                    await ws_manager.subscribe_order(order_data)
                    logger.info(f"Subscribed to order {order_id}")
                    subscribed = True
                    break
                except Exception as e:
                    logger.warning(f"Subscription attempt {attempt + 1} failed for order {order_id}: {e}")
                    send_telegram_alert(
                        f"WebSocket Monitor Warning:\n"
                        f"Token: {token}\n"
                        f"Order ID: {order_id}\n"
                        f"Warning: WebSocket subscription failed (attempt {attempt + 1}): {str(e)}"
                    )
                    if attempt < self.SUBSCRIPTION_RETRIES - 1:
                        await asyncio.sleep(self.SUBSCRIPTION_RETRY_DELAY)
            if not subscribed:
                logger.error(f"Failed to subscribe to order {order_id} after {self.SUBSCRIPTION_RETRIES} attempts")
                send_telegram_alert(
                    f"WebSocket Monitor Error:\n"
                    f"Token: {token}\n"
                    f"Order ID: {order_id}\n"
                    f"Error: Failed to subscribe to WebSocket after {self.SUBSCRIPTION_RETRIES} attempts"
                )
                order_data["status"] = "EXECUTION_ERROR"
                order_data["fillPercentage"] = 0.0
                await update_manual_order_monitor_csv(order_data)
                await self.on_order_update(order_data)
            
            put_markdown(f"### Hedge Order Request for {result['token']}")
            put_code(json.dumps(result['request'], indent=2), language='json')
            toast(f"Hedge trade triggered for {result['token']}", duration=5, color="success")
        else:
            error_message = result.get('error', 'Unknown error')
            order_data["status"] = "SUBMISSION_ERROR"
            order_data["fillPercentage"] = 0.0
            error_alert = (
                f"Manual Order Error Alert:\n"
                f"Token: {token}\n"
                f"Action: {action}\n"
                f"Quantity: {abs(quantity):.5f}\n"
                f"Order ID: {order_id}\n"
                f"Status: SUBMISSION_ERROR\n"
                f"Error: {error_message}"
            )
            send_telegram_alert(error_alert)
            if MANUAL_ORDER_MONITOR_CSV.exists():
                df = pd.read_csv(MANUAL_ORDER_MONITOR_CSV)
                mask = df["orderId"] == order_id
                df = df[~mask]
                df.to_csv(MANUAL_ORDER_MONITOR_CSV, index=False)
                logger.info(f"Removed order {order_id} from {MANUAL_ORDER_MONITOR_CSV}")
            await append_to_order_history(order_data, "Manual")
            toast(f"Failed to generate hedge order for {result['token']}: {error_message}", duration=5, color="error")

    async def handle_hedge_click(self, token: str, rebalance_value: float, action: str):
        logger.debug(f"handle_hedge_click called for token: {token}, rebalance_value: {rebalance_value}, action: {action}")
        toast(f"Hedge order initiated for {token}", duration=5, color="warning")
        if self.hedge_processing.get(token, False):
            toast(f"Hedge already in progress for {token}", duration=5, color="warning")
            return

        self.hedge_processing[token] = True
        try:
            signed_rebalance_value = rebalance_value if action == "buy" else -rebalance_value if action == "sell" else 0.0
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            result = await execute_hedge_trade(token, signed_rebalance_value, self.order_sender)
            await self.process_manual_order_result(result, token, action, rebalance_value, timestamp)
        except Exception as e:
            logger.error(f"Exception in handle_hedge_click for {token}: {str(e)}")
            toast(f"Error processing hedge for {token}: {str(e)}", duration=5, color="error")
        finally:
            self.hedge_processing[token] = False
            logger.debug(f"hedge_processing reset for {token}")

    async def handle_custom_hedge(self, inputs):
        token = inputs["token"]
        quantity = inputs["quantity"]
        action = inputs["action"]
        logger.debug(f"handle_custom_hedge called for token: {token}, quantity: {quantity}, action: {action}")
        toast(f"Custom order initiated for {token}", duration=5, color="warning")
        if self.hedge_processing.get(token, False):
            toast(f"Hedge already in progress for {token}", duration=5, color="warning")
            return

        self.hedge_processing[token] = True
        try:
            signed_quantity = quantity if action == "buy" else -quantity
            if signed_quantity == 0.0:
                logger.debug("Invalid quantity detected")
                toast(f"Invalid quantity for {token}", duration=5, color="error")
                return
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            task = asyncio.create_task(execute_hedge_trade(token, signed_quantity, self.order_sender))
            result = await task
            await self.process_manual_order_result(result, token, action, quantity, timestamp)
        except Exception as e:
            logger.error(f"Exception in handle_custom_hedge for {token}: {str(e)}")
            toast(f"Error processing custom hedge for {token}: {str(e)}", duration=5, color="error")
        finally:
            self.hedge_processing[token] = False
            logger.debug(f"hedge_processing reset for {token}")

    async def handle_close_hedge(self, token, hedged_qty, hedging_df=None):
        logger.debug(f"handle_close_hedge called for token: {token}, hedged_qty: {hedged_qty}")
        toast(f"Close order initiated for {token}", duration=5, color="warning")
        if self.hedge_processing.get(token, False):
            toast(f"Close hedge already in progress for {token}", duration=5, color="warning")
            return

        self.hedge_processing[token] = True
        try:
            close_qty = -hedged_qty
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            action = "buy" if close_qty > 0 else "sell"
            result = await execute_hedge_trade(token, close_qty, self.order_sender)
            await self.process_manual_order_result(result, token, action, abs(close_qty), timestamp)

            if result['success'] and hedging_df is not None:
                ticker = f"{token}USDT"
                hedging_df.loc[hedging_df["symbol"] == ticker, "quantity"] = 0
                hedging_df.loc[hedging_df["symbol"] == ticker, "amount"] = 0
                hedging_df.loc[hedging_df["symbol"] == ticker, "funding_rate"] = 0
                hedging_df.to_csv(HEDGING_LATEST_CSV, index=False)
                logger.info(f"Updated {HEDGING_LATEST_CSV} for {ticker}")
        except Exception as e:
            logger.error(f"Exception in handle_close_hedge for {token}: {str(e)}")
            toast(f"Error processing close hedge for {token}: {str(e)}", duration=5, color="error")
        finally:
            self.hedge_processing[token] = False
            logger.debug(f"hedge_processing reset for {token}")

    async def handle_close_all_hedges(self, token_summary, hedging_df, hedging_error=False):
        logger.debug("handle_close_all_hedges called")
        if any(self.hedge_processing.values()):
            toast("Hedge or close operation in progress", duration=5, color="warning")
            return

        if hedging_error:
            toast("Cannot close hedges due to hedging data fetch error", duration=5, color="error")
            logger.warning("Skipped close all hedges due to hedging_error")
            return

        if token_summary.empty or token_summary["quantity"].eq(0).all():
            toast("No hedge positions to close", duration=5, color="info")
            return

        results = []
        for _, row in token_summary.iterrows():
            token = row["Token"].replace("USDT", "").strip()
            hedged_qty = row["quantity"]
            if hedged_qty != 0:
                self.hedge_processing[token] = True
                try:
                    close_qty = -hedged_qty
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    action = "buy" if close_qty > 0 else "sell"
                    result = await execute_hedge_trade(token, close_qty, self.order_sender)
                    await self.process_manual_order_result(result, token, action, abs(close_qty), timestamp)
                    if result['success'] and hedging_df is not None:
                        ticker = f"{token}USDT"
                        hedging_df.loc[hedging_df["symbol"] == ticker, "quantity"] = 0
                        hedging_df.loc[hedging_df["symbol"] == ticker, "amount"] = 0
                        hedging_df.loc[hedging_df["symbol"] == ticker, "funding_rate"] = 0
                        hedging_df.to_csv(HEDGING_LATEST_CSV, index=False)
                        logger.info(f"Updated {HEDGING_LATEST_CSV} for {ticker}")
                    results.append(result)
                except Exception as e:
                    logger.error(f"Exception closing hedge for {token}: {str(e)}")
                    results.append({'success': False, 'token': token})
                finally:
                    self.hedge_processing[token] = False

        success_count = sum(1 for r in results if r['success'])
        if success_count == len(results) and results:
            toast("All hedge positions closed successfully", duration=5, color="success")
        elif results:
            toast(f"Closed {success_count}/{len(results)} hedge positions", duration=5, color="error")
        else:
            toast("No hedge positions were processed", duration=5, color="info")
        
        for result in results:
            if result['success']:
                put_markdown(f"### Close Hedge Order Request for {result['token']}")
                put_code(json.dumps(result['request'], indent=2), language='json')