import pandas as pd
import logging
import sys
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from common.path_config import LOG_DIR, REBALANCING_LATEST_CSV, AUTOMATIC_ORDER_MONITOR_CSV, MANUAL_ORDER_MONITOR_CSV, ORDER_HISTORY_CSV
from common.data_loader import load_data
from hedge_automation.order_manager import OrderManager
from common.utils import execute_hedge_trade
from hedge_automation.ws_manager import ws_manager
from common.bot_reporting import TGMessenger  

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'auto_hedge.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize OrderManager
order_manager = OrderManager()
order_sender = order_manager.get_order_sender()

# Configuration
SUBSCRIPTION_RETRIES = 3
SUBSCRIPTION_RETRY_DELAY = 2  # seconds
MAX_EXECUTION_TIME = 1800  # 1 hour fallback timeout

async def send_telegram_alert(message):
    """Send alert message to configured Telegram channel asynchronously."""
    logger.info(f"Sending Telegram alert: {message}")
    try:
        async with aiohttp.ClientSession() as session:
            response = await TGMessenger.send_async(session, message, 'LP eagle')
            if not response.get("ok", False):
                logger.error(f"Telegram response error: {response}")
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")

async def append_to_order_history(order_data, source):
    """Append a resolved order to order_history.csv."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status", "Source"]
    order_data["Source"] = source
    order_data["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_new = pd.DataFrame([order_data], columns=headers)
    
    try:
        if ORDER_HISTORY_CSV.exists():
            df = pd.read_csv(ORDER_HISTORY_CSV)
            df = pd.concat([df, df_new], ignore_index=True)
        else:
            df = df_new
        df.to_csv(ORDER_HISTORY_CSV, index=False)
        logger.info(f"Appended to {ORDER_HISTORY_CSV}: {order_data['orderId']} ({source})")
    except Exception as e:
        logger.error(f"Failed to append to {ORDER_HISTORY_CSV}: {e}")

async def update_order_monitor_csv(order_data, is_manual=False):
    """Update order_monitor.csv by modifying existing row or adding new."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status", "fillPercentage"]
    csv_file = MANUAL_ORDER_MONITOR_CSV if is_manual else AUTOMATIC_ORDER_MONITOR_CSV
    
    try:
        if csv_file.exists():
            df = pd.read_csv(csv_file)
            mask = (df["Token"] == order_data["Token"]) & (df["Rebalance Action"] == order_data["Rebalance Action"])
            if mask.any():
                for key, value in order_data.items():
                    if key in headers:
                        df.loc[mask, key] = value
            else:
                df = pd.concat([df, pd.DataFrame([order_data], columns=headers)], ignore_index=True)
        else:
            df = pd.DataFrame([order_data], columns=headers)
        df.to_csv(csv_file, index=False)
        logger.info(f"Updated {csv_file} for {order_data['Token']}")
    except Exception as e:
        logger.error(f"Failed to update {csv_file}: {e}")

async def remove_from_order_monitor(order_data, is_manual=False):
    """Remove an order from order_monitor.csv."""
    csv_file = MANUAL_ORDER_MONITOR_CSV if is_manual else AUTOMATIC_ORDER_MONITOR_CSV
    try:
        if csv_file.exists():
            df = pd.read_csv(csv_file)
            mask = (df["Token"] == order_data["Token"]) & (df["Rebalance Action"] == order_data["Rebalance Action"])
            if mask.any():
                df = df[~mask]
                df.to_csv(csv_file, index=False)
                logger.info(f"Removed order {order_data['orderId']} from {csv_file}")
            else:
                logger.warning(f"Order {order_data['orderId']} not found in {csv_file}")
    except Exception as e:
        logger.error(f"Failed to remove order from {csv_file}: {e}")

async def build_auto_orders():
    """Read REBALANCING_LATEST_CSV and create automatic_order_monitor.csv."""
    logger.info("Building auto orders from REBALANCING_LATEST_CSV...")
    
    if not REBALANCING_LATEST_CSV.exists():
        logger.error(f"{REBALANCING_LATEST_CSV} not found, cannot build orders.")
        return False

    try:
        df = pd.read_csv(REBALANCING_LATEST_CSV)
        if df.empty:
            logger.info("No rebalancing data available.")
            return False

        order_data = []
        for _, row in df.iterrows():
            if row.get("Auto Hedge", False) and row.get("Trigger Auto Order", False) and float(row["Rebalance Value"]) > 0:
                order_data.append({
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Token": row["Token"],
                    "Rebalance Action": row["Rebalance Action"],
                    "Rebalance Value": float(row["Rebalance Value"]),
                    "orderId": "",
                    "status": "RECEIVED",
                    "fillPercentage": 0.0
                })

        if order_data:
            order_df = pd.DataFrame(order_data)
            order_df = order_df.drop_duplicates(subset=["Token", "Rebalance Action"], keep="last")
            order_df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
            logger.info(f"Generated {AUTOMATIC_ORDER_MONITOR_CSV} with {len(order_data)} orders")
            return True
        else:
            logger.info("No auto-hedge orders to process.")
            return False

    except Exception as e:
        logger.error(f"Error building auto orders: {e}")
        return False

async def handle_order_update(order_data):
    """Handle WebSocket order updates from ws_manager."""
    order_id = order_data["orderId"]
    token = order_data["Token"]
    status = order_data["status"]
    fill_percentage = float(order_data.get("fillPercentage", 0.0))

    try:
        logger.info(f"Received update for order {order_id} ({token}): status={status}, fillPercentage={fill_percentage}")
        
        await update_order_monitor_csv(order_data)
        
        if status == "SUCCESS":
            logger.info(f"Order {order_id} for {token} completed successfully")
            await send_telegram_alert(
                f"Auto Order Success:\n"
                f"Token: {token}\n"
                f"Order ID: {order_id}\n"
                f"Action: {order_data['Rebalance Action']}\n"
                f"Quantity: {order_data['Rebalance Value']:.5f}\n"
                f"Status: SUCCESS"
            )
            await append_to_order_history(order_data, "Auto")
            await remove_from_order_monitor(order_data)

        elif status == "EXECUTION_ERROR":
            logger.error(f"Order {order_id} for {token} timed out")
            await send_telegram_alert(
                f"Auto Order Error Alert: \n"
                f"Token: {token} \n"
                f"Order ID: {order_id} \n"
                f"Action: {order_data['Rebalance Action']} \n"
                f"Quantity: {order_data['Rebalance Value']:.5f} \n"
                f"Status: EXECUTION_ERROR \n"
                f"Error: Order timed out"
            )
            await append_to_order_history(order_data, "Auto")
            await remove_from_order_monitor(order_data)

    except Exception as e:
        logger.error(f"Error handling order update for {order_id}: {e}")

async def check_order_status_fallback(order_data):
    """Fallback to check order status via API if WebSocket fails."""
    order_id = order_data["orderId"]
    token = order_data["Token"]
    try:
        result = await order_sender.get_order_status(order_id)
        if result.get("status") == "filled":
            order_data.update({
                "status": "SUCCESS",
                "fillPercentage": 1.0
            })
            await handle_order_update(order_data)
        elif result.get("status") in ["cancelled", "rejected"]:
            order_data.update({
                "status": "EXECUTION_ERROR",
                "fillPercentage": 0.0
            })
            await handle_order_update(order_data)
        logger.info(f"Fallback status for {order_id}: {result.get('status')}")
    except Exception as e:
        logger.error(f"Fallback status check failed for {order_id}: {e}")

async def process_auto_hedge():
    """Process auto-hedge actions from automatic_order_monitor.csv."""
    logger.info("Starting auto-hedge process...")
    await send_telegram_alert("Starting auto-hedge process...")
    
    data = load_data()
    error_flags = data['error_flags']
    errors = data['errors']
    
    if (errors.get('has_error', False) or 
        error_flags.get('krystal_error', False) or 
        error_flags.get('meteora_error', False) or 
        error_flags.get('hedging_error', False)):
        logger.error("Workflow errors detected, suspending auto-hedging.")
        logger.error(f"Error flags: {error_flags}")
        logger.error(f"Errors: {errors.get('messages', [])}")
        await send_telegram_alert(
            f"Auto-Hedge Error:\n"
            f"Workflow errors detected, auto-hedging suspended.\n"
            f"Error flags: {error_flags}\n"
            f"Errors: {errors.get('messages', [])}"
        )
        return

    if not await build_auto_orders():
        logger.info("No orders to process, exiting.")
        return

    try:
        if not AUTOMATIC_ORDER_MONITOR_CSV.exists():
            logger.error(f"{AUTOMATIC_ORDER_MONITOR_CSV} not found, cannot process orders.")
            return

        df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
        if df.empty:
            logger.info("No orders available in automatic_order_monitor.csv.")
            return

        # Check for stale EXECUTING orders
        for index, row in df[df["status"] == "EXECUTING"].iterrows():
            order_data = row.to_dict()
            await check_order_status_fallback(order_data)
        df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)  # Reload after fallback
        df = df.drop_duplicates(subset=["Token", "Rebalance Action"], keep="last")
        df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
        logger.info(f"Cleaned duplicates in {AUTOMATIC_ORDER_MONITOR_CSV}")

        # Start WebSocket listener
        await ws_manager.start_listener(handle_order_update)
        logger.info("WebSocket listener started")

        for index, row in df.iterrows():
            token = row["Token"]
            action = row["Rebalance Action"]
            quantity = float(row["Rebalance Value"])
            current_status = row["status"]
            order_id = row.get("orderId", "")

            if current_status == "EXECUTING" and pd.notna(order_id) and order_id:
                logger.info(f"Order for {token} already submitted with ID {order_id}, checking status")
                order_data = {
                    "Timestamp": row["Timestamp"],
                    "Token": token,
                    "Rebalance Action": action,
                    "Rebalance Value": quantity,
                    "orderId": order_id,
                    "status": current_status,
                    "fillPercentage": row["fillPercentage"]
                }
                subscribed = False
                for attempt in range(SUBSCRIPTION_RETRIES):
                    try:
                        await ws_manager.subscribe_order(order_data)
                        logger.info(f"Successfully subscribed to existing order {order_id} for {token}")
                        subscribed = True
                        break
                    except Exception as sub_e:
                        logger.warning(f"Subscription attempt {attempt + 1} failed for order {order_id}: {sub_e}")
                        await send_telegram_alert(
                            f"Auto Order Monitoring Warning:\n"
                            f"Token: {token}\n"
                            f"Order ID: {order_id}\n"
                            f"Warning: WebSocket subscription failed (attempt {attempt + 1}): {sub_e}"
                        )
                        if attempt < SUBSCRIPTION_RETRIES - 1:
                            await asyncio.sleep(SUBSCRIPTION_RETRY_DELAY)
                if not subscribed:
                    logger.error(f"Failed to subscribe to order {order_id} after {SUBSCRIPTION_RETRIES} attempts")
                    await send_telegram_alert(
                        f"Auto Order Monitoring Error:\n"
                        f"Token: {token}\n"
                        f"Order ID: {order_id}\n"
                        f"Error: Failed to subscribe to WebSocket after {SUBSCRIPTION_RETRIES} attempts"
                    )
                    await check_order_status_fallback(order_data)
                continue

            if current_status != "RECEIVED":
                logger.info(f"Skipping order for {token}: status is {current_status}")
                continue

            direction = 1 if action == "buy" else -1
            max_retries = 3
            retry_count = 0
            order_id = None
            status = "EXECUTING"

            order_data = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Token": token,
                "Rebalance Action": action,
                "Rebalance Value": quantity,
                "orderId": order_id or "",
                "status": status,
                "fillPercentage": 0.0
            }
            await update_order_monitor_csv(order_data)

            while retry_count < max_retries:
                try:
                    logger.info(f"Attempting order for {token}: {action} {quantity:.6f} (Attempt {retry_count + 1})")
                    result = await execute_hedge_trade(token, quantity * direction, order_sender)
                    
                    logger.info(f"Order submission result for {token}: {result}")
                    order_id = result['request']['clientOrderId'] if 'request' in result and 'clientOrderId' in result['request'] else ""
                    if not order_id:
                        logger.warning(f"No clientOrderId in result for {token}: {result}")
                        await send_telegram_alert(
                            f"Auto Order Warning:\n"
                            f"Token: {token}\n"
                            f"Action: {action}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Warning: No clientOrderId in result: {result}"
                        )
                    
                    if result['success']:
                        logger.info(f"Order successfully submitted for {token}: Order ID {order_id}")
                        await send_telegram_alert(
                            f"Order successfully submitted:\n"
                            f"Token: {token}\n"
                            f"Order ID: {order_id}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Action: {action}"
                        )
                        order_data.update({
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "orderId": order_id,
                            "status": status,
                            "fillPercentage": 0.0
                        })
                        await update_order_monitor_csv(order_data)
                        
                        subscribed = False
                        for attempt in range(SUBSCRIPTION_RETRIES):
                            try:
                                await asyncio.sleep(2)
                                await ws_manager.subscribe_order(order_data)
                                logger.info(f"Successfully subscribed to order {order_id} for {token}")
                                subscribed = True
                                break
                            except Exception as sub_e:
                                logger.warning(f"Subscription attempt {attempt + 1} failed for order {order_id}: {sub_e}")
                                await send_telegram_alert(
                                    f"Auto Order Monitoring Warning:\n"
                                    f"Token: {token}\n"
                                    f"Order ID: {order_id}\n"
                                    f"Warning: WebSocket subscription failed (attempt {attempt + 1}): {sub_e}"
                                )
                                if attempt < SUBSCRIPTION_RETRIES - 1:
                                    await asyncio.sleep(SUBSCRIPTION_RETRY_DELAY)
                        if not subscribed:
                            logger.error(f"Failed to subscribe to order {order_id} after {SUBSCRIPTION_RETRIES} attempts")
                            await send_telegram_alert(
                                f"Auto Order Monitoring Error:\n"
                                f"Token: {token}\n"
                                f"Order ID: {order_id}\n"
                                f"Error: Failed to subscribe to WebSocket after {SUBSCRIPTION_RETRIES} attempts"
                            )
                            await check_order_status_fallback(order_data)
                        break
                    else:
                        logger.warning(f"Order submission failed for {token}: {result}")
                        raise Exception(f"Order submission failed: {result.get('error', 'Unknown error')}")

                except Exception as e:
                    retry_count += 1
                    error_message = str(e)
                    logger.error(f"Failed to send order for {token}: {error_message}")
                    if retry_count < max_retries:
                        order_data.update({
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "orderId": order_id or "",
                            "status": "EXECUTING",
                            "fillPercentage": 0.0
                        })
                        await update_order_monitor_csv(order_data)
                        await asyncio.sleep(1)
                    else:
                        status = "SUBMISSION_ERROR"
                        logger.error(f"Max retries reached for {token}, marking as SUBMISSION_ERROR")
                        order_data.update({
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "orderId": order_id or "N/A",
                            "status": status,
                            "fillPercentage": 0.0
                        })
                        await send_telegram_alert(
                            f"Auto Order Error Alert :\n"
                            f"Token: {token} \n"
                            f"Action: {action} \n"
                            f"Quantity: {quantity:.5f} \n"
                            f"Order ID: {order_id or 'N/A'} \n"
                            f"Status: {status} \n"
                            f"Error: {error_message}"
                        )
                        await update_order_monitor_csv(order_data)
                        await append_to_order_history(order_data, "Auto")
                        await remove_from_order_monitor(order_data)
                        break

        # Fallback timeout check for stuck orders
        start_time = asyncio.get_event_loop().time()
        while df[df["status"] == "EXECUTING"].any().any():
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > MAX_EXECUTION_TIME:
                logger.error("Timeout: Some orders stuck in EXECUTING")
                await send_telegram_alert("Auto Order Error: Orders stuck in EXECUTING after timeout")
                for index, row in df[df["status"] == "EXECUTING"].iterrows():
                    order_data = {
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Token": row["Token"],
                        "Rebalance Action": row["Rebalance Action"],
                        "Rebalance Value": row["Rebalance Value"],
                        "orderId": row["orderId"],
                        "status": "EXECUTION_ERROR",
                        "fillPercentage": 0.0
                    }
                    await handle_order_update(order_data)
                break
            await asyncio.sleep(30)
            df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)  # Reload to check for updates

    except (OSError, pd.errors.EmptyDataError) as e:
        logger.error(f"Error accessing {AUTOMATIC_ORDER_MONITOR_CSV}: {e}")
        await send_telegram_alert(
            f"Auto-Hedge Error:\n"
            f"Failed to process {AUTOMATIC_ORDER_MONITOR_CSV}: {e}"
        )
        return

async def main():
    """Main function to run auto-hedge."""
    try:
        await process_auto_hedge()
    except Exception as e:
        logger.error(f"Main error: {e}")
        await send_telegram_alert(f"Auto-Hedge Main Error:\nError: {str(e)}")
    finally:
        await ws_manager.stop_listener()
        await order_manager.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())