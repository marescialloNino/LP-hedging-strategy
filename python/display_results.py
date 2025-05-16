import pandas as pd
import numpy as np
import sys
import asyncio
import logging
import atexit
from pywebio import start_server, config
from pywebio.output import *
from pywebio.session import run_async
from common.data_loader import load_data
from common.path_config import WORKFLOW_SHELL_SCRIPT, PNL_SHELL_SCRIPT, HEDGE_SHELL_SCRIPT, LOG_DIR
from hedge_automation.order_manager import OrderManager
from hedge_automation.hedge_actions import HedgeActions
from common.utils import run_shell_script
from ui.table_renderer import render_wallet_positions, render_pnl_tables, render_hedging_dashboard, save_auto_hedge_tokens, load_auto_hedge_tokens

# Fix for Windows event loop issue
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'display_results.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize OrderManager
order_manager = OrderManager()
hedge_actions = HedgeActions(order_manager.get_order_sender())

@config(theme="dark")
async def main():
    put_markdown("# 🔮 🧙‍♂️ 🧪 💸 CM's Hedging Dashboard 💸 🧪 🧙‍♂️ 🔮")

    # Load data
    data = load_data()
    dataframes = data['dataframes']
    error_flags = data['error_flags']
    errors = data['errors']

    # Display errors
    if errors['has_error']:
        error_text = "\n".join([f"- {msg}" for msg in errors['messages']])
        put_error(f"Data Fetching Errors:\n{error_text}", scope=None)
        logger.info(f"Displayed error messages: {error_text}")
    else:
        logger.info("No data fetching errors detected")

    # Wallet Positions
    put_markdown("## Wallet Positions")
    meteora_updated = error_flags['lp'].get("last_meteora_lp_update", "Not available")
    krystal_updated = error_flags['lp'].get("last_krystal_lp_update", "Not available")
    put_markdown(f"**Last Meteora LP Update:** {meteora_updated}  \n**Last Krystal LP Update:** {krystal_updated}")
    render_wallet_positions(dataframes, data)

    # PnL Section
    put_markdown("# LP positions PnL")
    async def handle_calculate_pnl():
        logger.info(f"Calculate PnL button clicked, executing {PNL_SHELL_SCRIPT}")
        toast("Running pnl calculations...could take a (long) while, you can find some new shitcoins in the meantime 📈", duration=10, color="warning")
        success, output = await run_shell_script(PNL_SHELL_SCRIPT)
        if success:
            toast("PnL calculations executed successfully, how did it go? can you buy a Lambo 🚗 or a scooter 🛵?", duration=10, color="success")
        else:
            toast(f"Failed to execute PnL calculations: {output}", duration=5, color="error")
    put_buttons(
        [{'label': 'Calculate PnL 💰', 'value': 'calculate_pnl', 'color': 'primary'}],
        onclick=lambda _: run_async(handle_calculate_pnl())
    )
    render_pnl_tables(dataframes, data)

    # Hedging Dashboard
    if "Rebalancing" in dataframes or "Hedging" in dataframes:
        put_markdown("# Hedging Dashboard")
        meteora_updated = error_flags['lp'].get("last_meteora_lp_update", "Not available")
        krystal_updated = error_flags['lp'].get("last_krystal_lp_update", "Not available")
        hedge_updated = error_flags['hedge'].get("last_updated_hedge", "Not available")
        put_markdown(f"**Last Meteora LP Update:** {meteora_updated}  \n**Last Krystal LP Update:** {krystal_updated}  \n**Last Hedge Data Update:** {hedge_updated}")
        logger.info(f"Displayed timestamps: Meteora={meteora_updated}, Krystal={krystal_updated}, Hedge={hedge_updated}")

        async def handle_run_workflow():
            logger.info(f"Run Workflow button clicked, executing {WORKFLOW_SHELL_SCRIPT}")
            toast("Running workflow...could take a while, you can check BTC dominance in the meantime 🟠", duration=10, color="warning")
            success, output = await run_shell_script(WORKFLOW_SHELL_SCRIPT)
            if success:
                toast("Workflow executed successfully", duration=5, color="success")
            else:
                toast(f"Failed to execute workflow: {output}", duration=5, color="error")
        put_buttons(
            [{'label': 'Update Data 🎲', 'value': 'run_workflow', 'color': 'primary'}],
            onclick=lambda _: run_async(handle_run_workflow()))

        async def handle_run_hedge():
            logger.info(f"Update Hedge button clicked, executing {HEDGE_SHELL_SCRIPT}")
            toast("Running hedge workflow... keep your hedge fresh 🧊", duration=10, color="warning")
            success, output = await run_shell_script(HEDGE_SHELL_SCRIPT)
            if success:
                toast("Workflow executed successfully", duration=5, color="success")
            else:
                toast(f"Failed to execute workflow: {output}", duration=5, color="error")
        put_buttons(
            [{'label': 'Update Hedge Data ❄️', 'value': 'run_hedge_calculations', 'color': 'primary'}],
            onclick=lambda _: run_async(handle_run_hedge()))

        # Compute token_summary for Close All Hedges
        if "Rebalancing" in dataframes:
            rebalancing_df = dataframes["Rebalancing"]
            token_agg = rebalancing_df.groupby("Token").agg({
                "LP Qty": "sum",
                "Hedged Qty": "sum",
                "Rebalance Value": "sum",
                "Rebalance Action": "first"
            }).reset_index()
            if "Hedging" in dataframes:
                hedging_df = dataframes["Hedging"]
                hedging_agg = hedging_df.groupby("symbol").agg({
                    "quantity": "sum",
                    "amount": "sum",
                    "funding_rate": "mean"
                }).reset_index().rename(columns={"symbol": "Token"})
                token_summary = pd.merge(token_agg, hedging_agg, on="Token", how="left")
                token_summary["quantity"] = token_summary["quantity"].fillna(0)
                token_summary["amount"] = token_summary["amount"].fillna(0)
                token_summary["funding_rate"] = token_summary["funding_rate"].fillna(0)
            else:
                token_summary = token_agg
                token_summary["quantity"] = 0
                token_summary["amount"] = 0
                token_summary["funding_rate"] = 0
        elif "Hedging" in dataframes:
            hedging_df = dataframes["Hedging"]
            token_summary = hedging_df.groupby("symbol").agg({
                "quantity": "sum",
                "amount": "sum",
                "funding_rate": "mean"
            }).reset_index().rename(columns={"symbol": "Token"})
            token_summary["LP Qty"] = 0
            token_summary["Hedged Qty"] = token_summary["quantity"]
            token_summary["Rebalance Value"] = 0
            token_summary["Rebalance Action"] = ""
        else:
            token_summary = pd.DataFrame(columns=["Token", "LP Qty", "Hedged Qty", "Rebalance Value", "Rebalance Action", "quantity", "amount", "funding_rate"])

        put_buttons(
            [{'label': 'Close All Hedges 🦍', 'value': 'all', 'color': 'danger'}],
            onclick=lambda _: run_async(hedge_actions.handle_close_all_hedges(
                token_summary, dataframes.get("Hedging"), errors['hedging_error']
            ))
        )

        render_hedging_dashboard(dataframes, data, hedge_actions)

def cleanup():
    asyncio.run(order_manager.close())

atexit.register(cleanup)

if __name__ == "__main__":
    start_server(main, port=8080, host="0.0.0.0", debug=True)