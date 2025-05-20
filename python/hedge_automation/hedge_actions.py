import logging
import os
from datetime import datetime
import pandas as pd
from pathlib import Path
from pywebio.output import put_markdown, put_code, toast
import asyncio
import json
from telegram import Bot
from telegram.error import TelegramError
from common.utils import execute_hedge_trade
from common.path_config import HEDGING_LATEST_CSV, MANUAL_ORDER_MONITOR_CSV, ORDER_HISTORY_CSV
from hedge_automation.ws_manager import ws_manager
from dotenv import load_dotenv
from common.bot_reporting import TGMessenger  # adjust path if needed
import aiohttp


load_dotenv()

logger = logging.getLogger('hedge_execution')

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
            
            # Start or reuse listener and subscribe order
            await ws_manager.start_listener()
            await ws_manager.subscribe_order(order_data)
            
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

    async def handle_close_all_hedges(self, token_summary, hedging_df, hedging_error):
        logger.debug("handle_close_all_hedges called")
        if any(self.hedge_processing.values()):
            toast("Hedge or close operation in progress, please wait", duration=5, color="warning")
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