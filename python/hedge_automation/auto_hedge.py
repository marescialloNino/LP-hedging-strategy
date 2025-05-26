import pandas as pd
import logging
import sys
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from pywebio.output import toast
from common.path_config import LOG_DIR, REBALANCING_LATEST_CSV, AUTOMATIC_ORDER_MONITOR_CSV, ORDER_HISTORY_CSV
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
LISTENER_RETRIES = 3
LISTENER_RETRY_DELAY = 2  # seconds

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

async def update_order_monitor_csv(order_data, match_by_token_action=False):
    """Update AUTOMATIC_ORDER_MONITOR_CSV by modifying or adding a row."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status", "fillPercentage"]
    
    try:
        if AUTOMATIC_ORDER_MONITOR_CSV.exists():
            df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
            if match_by_token_action:
                mask = (df["Token"] == order_data["Token"]) & (df["Rebalance Action"] == order_data["Rebalance Action"])
            else:
                mask = df["orderId"] == order_data["orderId"]
            
            if mask.any():
                for key, value in order_data.items():
                    if key in headers:
                        df.loc[mask, key] = value
            else:
                df = pd.concat([df, pd.DataFrame([order_data], columns=headers)], ignore_index=True)
        else:
            df = pd.DataFrame([order_data], columns=headers)
        
        df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
        logger.info(f"Updated {AUTOMATIC_ORDER_MONITOR_CSV} for {order_data['Token']}: {order_data['status']}")
    except Exception as e:
        logger.error(f"Failed to update {AUTOMATIC_ORDER_MONITOR_CSV}: {e}")

async def remove_from_order_monitor(order_data):
    """Remove an order from AUTOMATIC_ORDER_MONITOR_CSV using orderId."""
    try:
        if AUTOMATIC_ORDER_MONITOR_CSV.exists():
            df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
            mask = df["orderId"] == order_data["orderId"]
            if mask.any():
                df = df[~mask]
                df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
                logger.info(f"Removed order {order_data['orderId']} from {AUTOMATIC_ORDER_MONITOR_CSV}")
            else:
                logger.warning(f"Order {order_data['orderId']} not found in {AUTOMATIC_ORDER_MONITOR_CSV}")
    except Exception as e:
        logger.error(f"Failed to remove order from {AUTOMATIC_ORDER_MONITOR_CSV}: {e}")

async def build_auto_orders():
    """Read REBALANCING_LATEST_CSV and create AUTOMATIC_ORDER_MONITOR_CSV."""
    logger.info("Building auto orders from REBALANCING_LATEST_CSV...")
    
    if not REBALANCING_LATEST_CSV.exists():
        logger.error(f"{REBALANCING_LATEST_CSV} not found, cannot build orders.")
        try:
            toast(f"REBALANCING_LATEST_CSV not found, cannot build orders", duration=5, color="error")
        except Exception as e:
            logger.warning(f"Failed to display toast: {e}")
        return False

    try:
        df = pd.read_csv(REBALANCING_LATEST_CSV)
        if df.empty:
            logger.info("No rebalancing data available.")
            try:
                toast("No rebalancing data available", duration=5, color="info")
            except Exception as e:
                logger.warning(f"Failed to display toast: {e}")
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
            
            if AUTOMATIC_ORDER_MONITOR_CSV.exists():
                existing_df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
                existing_df = existing_df[existing_df["status"].isin(["EXECUTING", "SUCCESS", "EXECUTION_ERROR"])]
                order_df = pd.concat([existing_df, order_df], ignore_index=True)
                order_df = order_df.drop_duplicates(subset=["Token", "Rebalance Action"], keep="last")
            
            order_df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
            logger.info(f"Generated {AUTOMATIC_ORDER_MONITOR_CSV} with {len(order_data)} new orders")
            try:
                toast(f"Generated {len(order_data)} new auto-hedge orders", duration=5, color="success")
            except Exception as e:
                logger.warning(f"Failed to display toast: {e}")
            return True
        else:
            logger.info("No auto-hedge orders to process.")
            try:
                toast("No auto-hedge orders to process", duration=5, color="info")
            except Exception as e:
                logger.warning(f"Failed to display toast: {e}")
            return False

    except Exception as e:
        logger.error(f"Error building auto orders: {e}")
        try:
            toast(f"Error building auto orders: {e}", duration=5, color="error")
        except Exception as e_toast:
            logger.warning(f"Failed to display toast: {e_toast}")
        return False

async def handle_order_update(order_data):
    """Handle WebSocket order updates from ws_manager."""
    try:
        logger.debug(f"Received WebSocket update: {order_data}")
        order_id = order_data.get("orderId")
        status = order_data.get("status")
        fill_percentage = float(order_data.get("fillPercentage", 0.0))
        token = order_data.get("Token", "UNKNOWN")

        if not order_id or not status:
            logger.warning(f"Invalid WebSocket update: {order_data}")
            return

        order_data_updated = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Token": token,
            "Rebalance Action": order_data.get("Rebalance Action", ""),
            "Rebalance Value": float(order_data.get("Rebalance Value", 0.0)),
            "orderId": order_id,
            "status": status,
            "fillPercentage": round(fill_percentage * 100, 2)
        }

        FILL_THRESHOLD = 90.0
        if order_data_updated["fillPercentage"] >= FILL_THRESHOLD and status not in ["SUCCESS", "EXECUTION_ERROR"]:
            order_data_updated["status"] = "SUCCESS"
            logger.info(f"Order {order_id} reached {FILL_THRESHOLD}% fill, marking as SUCCESS")

        await update_order_monitor_csv(order_data_updated, match_by_token_action=False)

        alert_message = (
            f"Auto Order Update:\n"
            f"Token: {token}\n"
            f"Action: {order_data_updated['Rebalance Action']}\n"
            f"Quantity: {order_data_updated['Rebalance Value']:.5f}\n"
            f"Order ID: {order_id}\n"
            f"Status: {status}\n"
            f"Fill Percentage: {order_data_updated['fillPercentage']:.2f}%"
        )
        await send_telegram_alert(alert_message)

        if status in ["SUCCESS", "EXECUTION_ERROR"]:
            await append_to_order_history(order_data_updated, "Auto")
            await remove_from_order_monitor(order_data_updated)
            logger.info(f"Order {order_id} for {token} resolved: {status}")

    except Exception as e:
        logger.error(f"Error processing WebSocket update for {order_id}: {e}")
        await send_telegram_alert(
            f"Auto Order WebSocket Error:\n"
            f"Token: {token}\n"
            f"Order ID: {order_id}\n"
            f"Error: {str(e)}"
        )

async def process_auto_hedge():
    """Process auto-hedge actions from AUTOMATIC_ORDER_MONITOR_CSV."""
    logger.info("Starting auto-hedge process...")
    try:
        toast("Starting auto-hedge process", duration=5, color="info")
    except Exception as e:
        logger.warning(f"Failed to display toast: {e}")
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
        try:
            toast("Workflow errors detected, auto-hedging suspended", duration=5, color="error")
        except Exception as e:
            logger.warning(f"Failed to display toast: {e}")
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
            try:
                toast(f"{AUTOMATIC_ORDER_MONITOR_CSV} not found", duration=5, color="error")
            except Exception as e:
                logger.warning(f"Failed to display toast: {e}")
            return

        df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
        if df.empty:
            logger.info("No orders available in AUTOMATIC_ORDER_MONITOR_CSV.")
            try:
                toast("No orders available in AUTOMATIC_ORDER_MONITOR_CSV", duration=5, color="info")
            except Exception as e:
                logger.warning(f"Failed to display toast: {e}")
            return

        listener_started = False
        for attempt in range(LISTENER_RETRIES):
            try:
                await ws_manager.start_listener(handle_order_update)
                logger.info("WebSocket listener started")
                try:
                    toast("WebSocket listener started", duration=5, color="success")
                except Exception as e:
                    logger.warning(f"Failed to display toast: {e}")
                listener_started = True
                break
            except Exception as e:
                logger.warning(f"Failed to start WebSocket listener (attempt {attempt + 1}): {e}")
                await send_telegram_alert(
                    f"Auto-Hedge Warning:\n"
                    f"WebSocket listener failed to start (attempt {attempt + 1}): {str(e)}"
                )
                if attempt < LISTENER_RETRIES - 1:
                    await asyncio.sleep(LISTENER_RETRY_DELAY)
        if not listener_started:
            logger.error(f"Failed to start WebSocket listener after {LISTENER_RETRIES} attempts")
            try:
                toast(f"Failed to start WebSocket listener after {LISTENER_RETRIES} attempts", duration=5, color="error")
            except Exception as e:
                logger.warning(f"Failed to display toast: {e}")
            await send_telegram_alert(
                f"Auto-Hedge Error:\n"
                f"Failed to start WebSocket listener after {LISTENER_RETRIES} attempts"
            )
            return

        for index, row in df.iterrows():
            token = row["Token"]
            action = row["Rebalance Action"]
            quantity = float(row["Rebalance Value"])
            current_status = row["status"]
            order_id = row.get("orderId", "")

            if current_status == "EXECUTING" and pd.notna(order_id) and order_id:
                logger.info(f"Order for {token} already submitted with ID {order_id}, subscribing to updates")
                order_data = {
                    "Timestamp": row["Timestamp"],
                    "Token": token,
                    "Rebalance Action": action,
                    "Rebalance Value": quantity,
                    "orderId": order_id,
                    "status": current_status,
                    "fillPercentage": float(row["fillPercentage"])
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
                    order_data.update({
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "status": "EXECUTION_ERROR",
                        "fillPercentage": 0.0
                    })
                    await update_order_monitor_csv(order_data, match_by_token_action=False)
                    await handle_order_update(order_data)
                continue

            if current_status != "RECEIVED":
                logger.info(f"Skipping order for {token}: status is {current_status}")
                continue

            order_data = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Token": token,
                "Rebalance Action": action,
                "Rebalance Value": quantity,
                "orderId": "",
                "status": "RECEIVED",
                "fillPercentage": 0.0
            }
            await update_order_monitor_csv(order_data, match_by_token_action=True)

            direction = 1 if action == "buy" else -1
            max_retries = 3
            retry_count = 0
            success = False

            while retry_count < max_retries:
                try:
                    logger.info(f"Attempting order for {token}: {action} {quantity:.6f} (Attempt {retry_count + 1})")
                    result = await execute_hedge_trade(token, quantity * direction, order_sender)
                    
                    logger.info(f"Order submission result for {token}: {result}")
                    order_id = result['request']['clientOrderId'] if 'request' in result and 'clientOrderId' in result['request'] else ""
                    if not order_id:
                        logger.warning(f"No clientOrderId in result for {token}: {result}")
                        try:
                            toast(f"No clientOrderId for {token}, cannot track", duration=5, color="error")
                        except Exception as e:
                            logger.warning(f"Failed to display toast: {e}")
                        await send_telegram_alert(
                            f"Auto Order Warning:\n"
                            f"Token: {token}\n"
                            f"Action: {action}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Warning: No clientOrderId in result: {result}"
                        )
                    
                    order_data.update({
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "orderId": order_id,
                        "status": "EXECUTING",
                        "fillPercentage": 0.0
                    })
                    await update_order_monitor_csv(order_data, match_by_token_action=True)

                    if result['success']:
                        logger.info(f"Order successfully submitted for {token}: Order ID {order_id}")
                        try:
                            toast(f"Auto-hedge order triggered for {token}", duration=5, color="success")
                        except Exception as e:
                            logger.warning(f"Failed to display toast: {e}")
                        await send_telegram_alert(
                            f"Auto Order Submitted:\n"
                            f"Token: {token}\n"
                            f"Order ID: {order_id}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Action: {action}"
                        )
                        success = True
                        break
                    else:
                        logger.warning(f"Order submission failed for {token}: {result}")
                        raise Exception(f"Order submission failed: {result.get('error', 'Unknown error')}")

                except Exception as e:
                    retry_count += 1
                    error_message = str(e)
                    logger.error(f"Failed to send order for {token}: {error_message}")
                    if retry_count < max_retries:
                        await asyncio.sleep(1)
                    else:
                        logger.error(f"Max retries reached for {token}, marking as SUBMISSION_ERROR")
                        order_data.update({
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "orderId": order_id or "N/A",
                            "status": "SUBMISSION_ERROR",
                            "fillPercentage": 0.0
                        })
                        await update_order_monitor_csv(order_data, match_by_token_action=True)
                        try:
                            toast(f"Failed to submit auto-hedge order for {token}: {error_message}", duration=5, color="error")
                        except Exception as e_toast:
                            logger.warning(f"Failed to display toast: {e_toast}")
                        await send_telegram_alert(
                            f"Auto Order Error Alert:\n"
                            f"Token: {token}\n"
                            f"Action: {action}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Order ID: {order_id or 'N/A'}\n"
                            f"Status: SUBMISSION_ERROR\n"
                            f"Error: {error_message}"
                        )
                        await append_to_order_history(order_data, "Auto")
                        await remove_from_order_monitor(order_data)
                        break

            if success:
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
                    order_data.update({
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "status": "EXECUTION_ERROR",
                        "fillPercentage": 0.0
                    })
                    await update_order_monitor_csv(order_data, match_by_token_action=False)
                    await handle_order_update(order_data)

        logger.info("Waiting for all orders to be resolved...")
        while True:
            try:
                if not AUTOMATIC_ORDER_MONITOR_CSV.exists():
                    logger.info("Order monitor CSV no longer exists, all orders processed.")
                    try:
                        toast("All auto-hedge orders processed", duration=5, color="success")
                    except Exception as e:
                        logger.warning(f"Failed to display toast: {e}")
                    break
                df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
                if df.empty:
                    logger.info("All orders resolved.")
                    try:
                        toast("All auto-hedge orders resolved", duration=5, color="success")
                    except Exception as e:
                        logger.warning(f"Failed to display toast: {e}")
                    break
                logger.info(f"Pending orders: {len(df)}")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error checking orders: {e}")
                try:
                    toast(f"Error checking auto-hedge orders: {e}", duration=5, color="error")
                except Exception as e_toast:
                    logger.warning(f"Failed to display toast: {e_toast}")
                await asyncio.sleep(5)

    except (OSError, pd.errors.EmptyDataError) as e:
        logger.error(f"Error accessing {AUTOMATIC_ORDER_MONITOR_CSV}: {e}")
        try:
            toast(f"Error accessing {AUTOMATIC_ORDER_MONITOR_CSV}: {e}", duration=5, color="error")
        except Exception as e_toast:
            logger.warning(f"Failed to display toast: {e_toast}")
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
        try:
            toast(f"Auto-hedge main error: {e}", duration=5, color="error")
        except Exception as e_toast:
            logger.warning(f"Failed to display toast: {e_toast}")
        await send_telegram_alert(f"Auto-Hedge Main Error:\nError: {str(e)}")
    finally:
        await ws_manager.stop_listener()
        await order_manager.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())