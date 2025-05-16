import pandas as pd
import numpy as np
import sys
import asyncio
import logging
import atexit
from pathlib import Path
from pywebio import start_server, config
from pywebio.input import checkbox, input_group, actions
from pywebio.output import use_scope, put_markdown, put_error, put_text, put_table, put_buttons, toast, put_html
from pywebio.session import run_async
from common.data_loader import load_data
from common.constants import HEDGABLE_TOKENS
from common.path_config import WORKFLOW_SHELL_SCRIPT, PNL_SHELL_SCRIPT, HEDGE_SHELL_SCRIPT, LOG_DIR
from hedge_automation.order_manager import OrderManager
from hedge_automation.hedge_actions import HedgeActions
from common.utils import run_shell_script
from ui.table_renderer import (
    render_wallet_positions,
    render_pnl_tables,
    render_hedging_table,
    render_hedge_automation,
    save_auto_hedge_tokens,
    load_auto_hedge_tokens
)

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

# Initialize OrderManager and HedgeActions
order_manager = OrderManager()
hedge_actions = HedgeActions(order_manager.get_order_sender())

@config(theme="dark")
async def main():
    # Scope the entire dashboard
    with use_scope('dashboard', clear=True):
        put_markdown("# 🔮 🧙‍♂️ 🧪 💸 CM's Hedging Dashboard 💸 🧪 🧙‍♂️ 🔮")

        # Load data
        data = load_data()
        dataframes = data['dataframes']
        error_flags = data['error_flags']
        errors = data['errors']

        # Display errors
        if errors.get('has_error', False):
            error_text = "\n".join([f"- {msg}" for msg in errors.get('messages', [])])
            put_error(f"Data Fetching Errors:\n{error_text}")
            logger.info(f"Displayed error messages: {error_text}")
        else:
            logger.info("No data fetching errors detected")

        # Wallet Positions
        put_markdown("## Wallet Positions")
        meteora_updated = error_flags['lp'].get("last_meteora_lp_update", "Not available")
        krystal_updated = error_flags['lp'].get("last_krystal_lp_update", "Not available")
        put_markdown(f"**Last Meteora LP Update:** {meteora_updated}  \n**Last Krystal LP Update:** {krystal_updated}")
        render_wallet_positions(dataframes, error_flags)

        # PnL Section
        put_markdown("## LP Positions PnL")
        async def handle_calculate_pnl():
            logger.info(f"Calculate PnL button clicked, executing {PNL_SHELL_SCRIPT}")
            toast("Running pnl calculations... this may take a while 📈", duration=10, color="warning")
            success, output = await run_shell_script(PNL_SHELL_SCRIPT)
            toast(
                "PnL calculations completed successfully 🎉" if success else f"PnL calc failed: {output}",
                duration=5,
                color="success" if success else "error"
            )
        put_buttons(
            [{'label': 'Calculate PnL 💰', 'value': 'calculate_pnl', 'color': 'primary'}],
            onclick=lambda _: run_async(handle_calculate_pnl())
        )
        render_pnl_tables(dataframes, error_flags)

        # Hedging Dashboard
        put_markdown("## Hedging Dashboard")
        if "Rebalancing" in dataframes or "Hedging" in dataframes:
            meteora_updated = error_flags['lp'].get("last_meteora_lp_update", "Not available")
            krystal_updated = error_flags['lp'].get("last_krystal_lp_update", "Not available")
            hedge_updated = error_flags['hedge'].get("last_updated_hedge", "Not available")
            put_markdown(
                f"**Last Meteora LP Update:** {meteora_updated}  \n"
                f"**Last Krystal LP Update:** {krystal_updated}  \n"
                f"**Last Hedge Data Update:** {hedge_updated}"
            )
            # Update & Hedge buttons
            async def handle_run_workflow():
                logger.info(f"Run Workflow clicked: {WORKFLOW_SHELL_SCRIPT}")
                toast("Updating data... 🟠", duration=10, color="warning")
                success, output = await run_shell_script(WORKFLOW_SHELL_SCRIPT)
                toast(
                    "Data updated successfully ✅" if success else f"Update failed: {output}",
                    duration=5,
                    color="success" if success else "error"
                )
            put_buttons(
                [{'label': 'Update Data 🎲', 'value': 'run_workflow', 'color': 'primary'}],
                onclick=lambda _: run_async(handle_run_workflow())
            )

            async def handle_run_hedge():
                logger.info(f"Run Hedge clicked: {HEDGE_SHELL_SCRIPT}")
                toast("Running hedge workflow... 🧊", duration=10, color="warning")
                success, output = await run_shell_script(HEDGE_SHELL_SCRIPT)
                toast(
                    "Hedge workflow successful ✅" if success else f"Hedge failed: {output}",
                    duration=5,
                    color="success" if success else "error"
                )
            put_buttons(
                [{'label': 'Update Hedge Data ❄️', 'value': 'run_hedge', 'color': 'primary'}],
                onclick=lambda _: run_async(handle_run_hedge())
            )

            # Render hedging table
            render_hedging_table(dataframes, error_flags, hedge_actions)
        else:
            put_text("No rebalancing or hedging data available.")

        put_markdown("## Hedge Automation")
        options, auto_hedge_tokens = render_hedge_automation()
        hedgable_tokens = sorted([ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()])
        
        put_text(f"Debug: Number of checkbox options: {len(options)}")  # Debug output
        
        async def handle_config_change():
            if options:
                form_data = await input_group("Auto-hedge Tokens", [
                    checkbox(
                        name="auto_hedge_tokens",
                        options=options,
                        inline=True,
                        help_text="Select tokens to auto-hedge"
                    ),
                    actions(
                        name="submit",
                        buttons=[{'label': 'Save Configuration', 'value': 'save', 'color': 'success'}]
                    )
                ])
                
                if form_data['submit'] == 'save':
                    save_auto_hedge_tokens({
                        token: token in form_data['auto_hedge_tokens'] for token in hedgable_tokens
                    })
                    toast("Configuration saved successfully!", duration=3, color="success")
            else:
                toast("No hedgeable tokens available.", duration=3, color="warning")
        
        if options:
            put_buttons(
                [{'label': 'Change Automation Configuration', 'value': 'config', 'color': 'primary'}],
                onclick=lambda _: run_async(handle_config_change())
            )
        else:
            put_text("No hedgeable tokens available.")


def cleanup():
    asyncio.run(order_manager.close())

atexit.register(cleanup)

if __name__ == "__main__":
    start_server(main, port=8080, host="0.0.0.0", debug=True)
