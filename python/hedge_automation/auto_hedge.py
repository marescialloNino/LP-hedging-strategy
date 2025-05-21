import pandas as pd
import logging
import sys
import asyncio
from pathlib import Path
from datetime import datetime
from common.path_config import LOG_DIR, REBALANCING_LATEST_CSV, AUTOMATIC_ORDER_MONITOR_CSV, MANUAL_ORDER_MONITOR_CSV, ORDER_HISTORY_CSV
from common.data_loader import load_data
from hedge_automation.order_manager import OrderManager
from common.utils import execute_hedge_trade
from hedge_automation.ws_manager import ws_manager
from common.bot_reporting import TGMessenger  
import aiohttp

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

async def update_order_monitor_csv(order_data, is_manual=False):
    """Update order_monitor.csv by modifying existing row."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status", "fillPercentage"]
    csv_file = MANUAL_ORDER_MONITOR_CSV if is_manual else AUTOMATIC_ORDER_MONITOR_CSV
    
    if csv_file.exists():
        df = pd.read_csv(csv_file)
        mask = (df["Token"] == order_data["Token"]) & (df["Rebalance Action"] == order_data["Rebalance Action"])
        if mask.any():
            for key, value in order_data.items():
                if key in headers:
                    df.loc[mask, key] = value
        else:
            df = pd.concat([df, pd.DataFrame([order_data])], ignore_index=True)
    else:
        df = pd.DataFrame([order_data], columns=headers)
    
    df.to_csv(csv_file, index=False)
    logger.info(f"Updated {csv_file} for {order_data['Token']}")

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
            order_df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
            logger.info(f"Generated {AUTOMATIC_ORDER_MONITOR_CSV} with {len(order_data)} orders")
            return True
        else:
            logger.info("No auto-hedge orders to process.")
            return False

    except Exception as e:
        logger.error(f"Error building auto orders: {e}")
        return False

async def process_auto_hedge():
    """Process auto-hedge actions from automatic_order_monitor.csv."""
    logger.info("Starting auto-hedge process...")

    send_telegram_alert("Starting auto-hedge process...")
    
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

        for index, row in df.iterrows():
            token = row["Token"]
            action = row["Rebalance Action"]
            quantity = float(row["Rebalance Value"])
            current_status = row["status"]

            # Check for existing successful order
            if current_status == "EXECUTING" and pd.notna(row["orderId"]):
                logger.info(f"Order for {token} already submitted with ID {row['orderId']}, skipping")
                # Attempt to subscribe if not already subscribed
                try:
                    await ws_manager.subscribe_order({
                        "Token": token,
                        "Rebalance Action": action,
                        "Rebalance Value": quantity,
                        "orderId": row["orderId"],
                        "status": current_status,
                        "fillPercentage": row["fillPercentage"]
                    })
                    logger.info(f"Successfully subscribed to existing order {row['orderId']} for {token}")
                except Exception as sub_e:
                    logger.warning(f"Failed to subscribe to existing order {row['orderId']} for {token}: {sub_e}. Order is not being monitored.")
                    send_telegram_alert(
                        f"Auto Order Monitoring Warning:\n"
                        f"Token: {token}\n"
                        f"Order ID: {row['orderId']}\n"
                        f"Warning: Failed to subscribe to WebSocket: {sub_e}. Order is not being monitored."
                    )
                continue

            if current_status != "RECEIVED":
                logger.info(f"Skipping order for {token}: status is {current_status}")
                continue

            direction = 1 if action == "buy" else -1
            max_retries = 3
            retry_count = 0
            order_id = None
            status = "EXECUTING"

            while retry_count < max_retries:
                try:
                    logger.info(f"Attempting order for {token}: {action} {quantity:.5f} (Attempt {retry_count + 1})")
                    result = await execute_hedge_trade(token, quantity * direction, order_sender)
                    
                    logger.info(f"Order submission result for {token}: {result}")
                    order_id = result['request']['clientOrderId'] if 'request' in result and 'clientOrderId' in result['request'] else ""
                    if not order_id:
                        logger.warning(f"No clientOrderId in result for {token}: {result}")
                        send_telegram_alert(
                            f"Auto Order Warning:\n"
                            f"Token: {token}\n"
                            f"Action: {action}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Warning: No clientOrderId in result: {result}"
                        )
                    
                    if result['success']:
                        logger.info(f"Order successfully submitted for {token}: Order ID {order_id}")
                        order_data = {
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Token": token,
                            "Rebalance Action": action,
                            "Rebalance Value": quantity,
                            "orderId": order_id,
                            "status": status,
                            "fillPercentage": 0.0
                        }
                        await update_order_monitor_csv(order_data)
                        
                        # Attempt WebSocket subscription
                        try:
                            await ws_manager.start_listener()  # Ensure listener is running
                            await ws_manager.subscribe_order(order_data)
                            logger.info(f"Successfully subscribed to order {order_id} for {token}")
                        except Exception as sub_e:
                            logger.warning(f"Failed to subscribe to order {order_id} for {token}: {sub_e}. Order is not being monitored.")
                            send_telegram_alert(
                                f"Auto Order Monitoring Warning:\n"
                                f"Token: {token}\n"
                                f"Order ID: {order_id}\n"
                                f"Warning: Failed to subscribe to WebSocket: {sub_e}. Order is not being monitored."
                            )
                        break  # Exit retry loop since order was successfully submitted
                    else:
                        logger.warning(f"Order submission failed for {token}: {result}")
                        raise Exception(f"Order submission failed: {result.get('error', 'Unknown error')}")

                except Exception as e:
                    retry_count += 1
                    error_message = str(e)
                    logger.error(f"Failed to send order for {token}: {error_message}")
                    if retry_count < max_retries:
                        order_data = {
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Token": token,
                            "Rebalance Action": action,
                            "Rebalance Value": quantity,
                            "orderId": order_id,
                            "status": "RECEIVED",
                            "fillPercentage": 0.0
                        }
                        await update_order_monitor_csv(order_data)
                        await asyncio.sleep(1)
                    else:
                        status = "SUBMISSION_ERROR"
                        logger.error(f"Max retries reached for {token}, marking as SUBMISSION_ERROR")
                        order_data = {
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Token": token,
                            "Rebalance Action": action,
                            "Rebalance Value": quantity,
                            "orderId": order_id,
                            "status": status,
                            "fillPercentage": 0.0
                        }
                        error_alert = (
                            f"Auto Order Error Alert:\n"
                            f"Token: {token}\n"
                            f"Action: {action}\n"
                            f"Quantity: {quantity:.5f}\n"
                            f"Order ID: {order_id}\n"
                            f"Status: {status}\n"
                            f"Error: {error_message}"
                        )
                        send_telegram_alert(error_alert)
                        if AUTOMATIC_ORDER_MONITOR_CSV.exists():
                            df = pd.read_csv(AUTOMATIC_ORDER_MONITOR_CSV)
                            mask = (df["Token"] == token) & (df["Rebalance Action"] == action)
                            df = df[~mask]
                            df.to_csv(AUTOMATIC_ORDER_MONITOR_CSV, index=False)
                            logger.info(f"Removed order {order_id} from {AUTOMATIC_ORDER_MONITOR_CSV}")
                        await append_to_order_history(order_data, "Auto")

        # Wait for listener to complete
        if ws_manager.task:
            await ws_manager.task

    except Exception as e:
        logger.error(f"Error processing auto-hedge: {e}")
        send_telegram_alert(f"Auto-Hedge Process Error:\nError: {str(e)}")

async def main():
    """Main function to run auto-hedge."""
    try:
        # Start WebSocket listener
        try:
            await ws_manager.start_listener()
            logger.info("WebSocket listener started successfully")
        except Exception as e:
            logger.warning(f"Failed to start WebSocket listener: {e}. Orders will not be monitored.")        
        await process_auto_hedge()
    except Exception as e:
        logger.error(f"Main error: {e}")   
    finally:
        await ws_manager.stop_listener()
        await order_manager.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())