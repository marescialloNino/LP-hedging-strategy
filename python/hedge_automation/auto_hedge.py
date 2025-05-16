import pandas as pd
import logging
import sys
import asyncio
import websockets
import json
from pathlib import Path
from datetime import datetime
from common.path_config import LOG_DIR, REBALANCING_LATEST_CSV, ORDER_MONITOR_CSV
from common.data_loader import load_data
from hedge_automation.order_manager import OrderManager
from common.utils import execute_hedge_trade

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

# Placeholder for Telegram alert
def send_telegram_alert(message):
    logger.warning(f"TELEGRAM ALERT PLACEHOLDER: {message}")

async def build_auto_orders():
    """Read REBALANCING_LATEST_CSV and create order_monitor.csv for auto-hedged orders."""
    logger.info("Building auto orders from REBALANCING_LATEST_CSV...")
    
    if not REBALANCING_LATEST_CSV.exists():
        logger.error(f"{REBALANCING_LATEST_CSV} not found, cannot build orders.")
        return False

    try:
        df = pd.read_csv(REBALANCING_LATEST_CSV)
        if df.empty:
            logger.info("No rebalancing data available.")
            return False

        # Extract orders for auto-hedged tokens with Trigger Auto Order = True
        order_data = []
        for _, row in df.iterrows():
            if row.get("Auto Hedge", False) and row.get("Trigger Auto Order", False) and float(row["Rebalance Value"]) > 0:
                order_data.append({
                    "Timestamp": row["Timestamp"],
                    "Token": row["Token"],
                    "Rebalance Action": row["Rebalance Action"],
                    "Rebalance Value": float(row["Rebalance Value"]),
                    "orderId": "",
                    "status": "RECEIVED"
                })

        if order_data:
            order_df = pd.DataFrame(order_data)
            order_df.to_csv(ORDER_MONITOR_CSV, index=False)
            logger.info(f"Generated {ORDER_MONITOR_CSV} with {len(order_data)} orders")
            return True
        else:
            logger.info("No auto-hedge orders to process.")
            return False

    except Exception as e:
        logger.error(f"Error building auto orders: {e}")
        return False

async def update_order_monitor_csv(order_data):
    """Update order_monitor.csv by modifying existing row based on Token and Rebalance Action."""
    headers = ["Timestamp", "Token", "Rebalance Action", "Rebalance Value", "orderId", "status"]
    
    if ORDER_MONITOR_CSV.exists():
        df = pd.read_csv(ORDER_MONITOR_CSV)
        # Match row by Token and Rebalance Action
        mask = (df["Token"] == order_data["Token"]) & (df["Rebalance Action"] == order_data["Rebalance Action"])
        if mask.any():
            for key, value in order_data.items():
                if key in df.columns:
                    df.loc[mask, key] = value
        else:
            # If no match, append new row (shouldn't happen after build_auto_orders)
            df = pd.concat([df, pd.DataFrame([order_data])], ignore_index=True)
    else:
        df = pd.DataFrame([order_data], columns=headers)
    
    df.to_csv(ORDER_MONITOR_CSV, index=False)
    logger.info(f"Updated {ORDER_MONITOR_CSV} for {order_data['Token']}")

async def websocket_order_listener(order_ids):
    """Placeholder for WebSocket monitoring of order execution."""
    logger.info("WebSocket monitoring not implemented. Placeholder for order IDs: %s", order_ids)
    # TODO: Implement WebSocket logic to monitor order execution
    # Expected: Update order_monitor.csv with EXECUTION_ERROR for rejected/canceled orders
    pass

async def process_auto_hedge():
    """Process auto-hedge actions from order_monitor.csv."""
    logger.info("Starting auto-hedge process...")
    
    # Check for errors in workflow
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

    # Build order_monitor.csv
    if not await build_auto_orders():
        logger.info("No orders to process, exiting.")
        return

    try:
        if not ORDER_MONITOR_CSV.exists():
            logger.error(f"{ORDER_MONITOR_CSV} not found, cannot process orders.")
            return

        df = pd.read_csv(ORDER_MONITOR_CSV)
        if df.empty:
            logger.info("No orders available in order_monitor.csv.")
            return

        # Process orders sequentially
        order_ids = []
        for index, row in df.iterrows():
            token = row["Token"]
            action = row["Rebalance Action"]
            quantity = float(row["Rebalance Value"])
            current_status = row["status"]

            if current_status != "RECEIVED":
                logger.info(f"Skipping order for {token}: status is {current_status}")
                continue

            direction = 1 if action == "buy" else -1

            max_retries = 1
            retry_count = 0
            order_id = None
            status = "EXECUTING"

            while retry_count < max_retries:
                try:
                    logger.info(f"Attempting order for {token}: {action} {quantity:.5f} (Attempt {retry_count + 1})")
                    result = await execute_hedge_trade(token, quantity * direction, order_sender)
                    
                    # Print result for debugging
                    print(f"DEBUG: Result for {token} attempt {retry_count + 1}: {result}", flush=True)
                    
                    # Capture clientOrderId, even for failed submissions
                    order_id = result['request']['clientOrderId'] if 'request' in result and 'clientOrderId' in result['request'] else ""
                    print(f"DEBUG: Extracted order_id for {token}: {order_id}", flush=True)
                    if not order_id:
                        logger.warning(f"No clientOrderId in result for {token}: {result}")
                    
                    if result['success']:
                        logger.info(f"Order sent for {token}: Order ID {order_id}")
                        order_data = {
                            "Timestamp": row["Timestamp"],
                            "Token": token,
                            "Rebalance Action": action,
                            "Rebalance Value": quantity,
                            "orderId": order_id,
                            "status": status
                        }
                        print(f"DEBUG: Updating CSV for {token} with order_data: {order_data}", flush=True)
                        await update_order_monitor_csv(order_data)
                        order_ids.append(order_id)
                        break
                    else:
                        raise Exception("Order submission failed")

                except Exception as e:
                    retry_count += 1
                    error_message = str(e)
                    logger.error(f"Failed to send order for {token}: {error_message}")
                    if retry_count < max_retries:
                        # Update with current order_id even on failure
                        order_data = {
                            "Timestamp": row["Timestamp"],
                            "Token": token,
                            "Rebalance Action": action,
                            "Rebalance Value": quantity,
                            "orderId": order_id,
                            "status": "RECEIVED"
                        }
                        print(f"DEBUG: Updating CSV for {token} retry {retry_count} with order_data: {order_data}", flush=True)
                        await update_order_monitor_csv(order_data)
                        await asyncio.sleep(1)
                    else:
                        status = "SUBMISSION_ERROR"
                        logger.error(f"Max retries reached for {token}, marking as SUBMISSION_ERROR")
                        send_telegram_alert(f"Auto-hedge submission failed for {token} after {max_retries} attempts: {error_message}")
                        order_data = {
                            "Timestamp": row["Timestamp"],
                            "Token": token,
                            "Rebalance Action": action,
                            "Rebalance Value": quantity,
                            "orderId": order_id,
                            "status": status
                        }
                        print(f"DEBUG: Updating CSV for {token} final failure with order_data: {order_data}", flush=True)
                        await update_order_monitor_csv(order_data)

        if order_ids:
            logger.info(f"Starting WebSocket listener for order IDs: {order_ids}")
            await websocket_order_listener(order_ids)

        # Summarize run and send Telegram alerts for errors
        if ORDER_MONITOR_CSV.exists():
            tracking_df = pd.read_csv(ORDER_MONITOR_CSV)
            summary = {
                "Total Orders": len(tracking_df),
                "Received": len(tracking_df[tracking_df["status"] == "RECEIVED"]),
                "Executing": len(tracking_df[tracking_df["status"] == "EXECUTING"]),
                "Success": len(tracking_df[tracking_df["status"] == "SUCCESS"]),
                "Submission Error": len(tracking_df[tracking_df["status"] == "SUBMISSION_ERROR"]),
                "Execution Error": len(tracking_df[tracking_df["status"] == "EXECUTION_ERROR"])
            }
            logger.info(f"Run Summary: {summary}")

            # Send Telegram alerts for orders with errors
            error_orders = tracking_df[tracking_df["status"].isin(["SUBMISSION_ERROR", "EXECUTION_ERROR"])]
            for _, row in error_orders.iterrows():
                error_message = (
                    f"Order Error Alert:\n"
                    f"Token: {row['Token']}\n"
                    f"Action: {row['Rebalance Action']}\n"
                    f"Quantity: {row['Rebalance Value']:.5f}\n"
                    f"Order ID: {row['orderId']}\n"
                    f"Status: {row['status']}"
                )
                send_telegram_alert(error_message)

    except Exception as e:
        logger.error(f"Error processing auto-hedge: {e}")

async def main():
    """Main function to run the auto-hedge process."""
    try:
        await process_auto_hedge()
    finally:
        await order_manager.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())