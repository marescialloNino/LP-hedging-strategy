import pandas as pd
import numpy as np
import sys
import asyncio
import logging
import atexit
import yaml
from pathlib import Path
from pywebio import start_server, config
from pywebio.input import checkbox, input_group, actions, select, input
from pywebio.output import use_scope, put_markdown, put_error, put_text, put_table, put_buttons, toast, put_html, clear
from pywebio.session import run_async
from common.data_loader import load_data, load_hedgeable_tokens
from common.path_config import WORKFLOW_SHELL_SCRIPT, PNL_SHELL_SCRIPT, HEDGE_SHELL_SCRIPT, LOG_DIR, LPMONITOR_YAML_CONFIG_PATH
from hedge_automation.order_manager import OrderManager
from hedge_automation.hedge_actions import HedgeActions
from common.utils import run_shell_script
from ui.ticker_mapping import render_add_token_mapping_section
from ui.table_renderer import (
    render_wallet_positions,
    render_pnl_tables,
    render_hedging_table,
    render_hedge_automation,
    save_auto_hedge_tokens,
    load_auto_hedge_tokens,
    render_custom_hedge_section
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

def format_usd(value):
    """Format USD value with commas and 2 decimal places."""
    return f"${value:,.2f}" if pd.notna(value) else "N/A"


async def handle_vault_share_change():
    """Handle updating vault share in config.yaml via a form."""
    yaml_file = LPMONITOR_YAML_CONFIG_PATH
    logger.info("Change Vault Share button clicked")

    # Chain ID to name mapping
    chain_id_mapping = {
        1: "ethereum",
        56: "bsc",
        137: "polygon",
        146: "sonic",
        42161: "arbitrum",
        8453: "base",
    }

    try:
        # Read YAML file
        with yaml_file.open('r') as f:
            config = yaml.safe_load(f)
        if not config or 'krystal_vault_wallet_chain_ids' not in config:
            raise ValueError("Invalid or missing krystal_vault_wallet_chain_ids in config.yaml")

        # Prepare wallet options for dropdown
        wallet_options = []
        for entry in config['krystal_vault_wallet_chain_ids']:
            # Convert chain IDs to names
            chain_names = []
            for chain_id in entry['chains']:
                try:
                    chain_id_int = int(chain_id)
                    chain_name = chain_id_mapping.get(chain_id_int, chain_id)
                except ValueError:
                    chain_name = chain_id
                chain_names.append(chain_name)
            logger.debug(f"Wallet {entry['wallet']}: Raw chains {entry['chains']}, Mapped: {chain_names}")
            wallet_options.append({
                "label": f"{entry['wallet']} (Chains: {', '.join(chain_names)}, Current Share: {entry['vault_share']})",
                "value": entry['wallet']
            })

        if not wallet_options:
            toast("No vault wallets found in config.yaml.", duration=5, color="warning")
            logger.warning("No vault wallets found in config.yaml")
            return

        # Validation function for vault share
        def validate_vault_share(value):
            try:
                share = float(value)
                if not 0 <= share <= 1:
                    return "Must be between 0 and 1"
                return None
            except ValueError:
                return "Must be a valid number (e.g., 0.915)"

        # Render form
        form_data = await input_group("Update Vault Share", [
            select(
                name="wallet",
                options=wallet_options,
                help_text="Select a vault wallet to update its share."
            ),
            input(
                name="vault_share",
                type="text",
                value="0.9",
                required=True,
                help_text="Enter new vault share (0 to 1, e.g., 0.915)",
                validate=validate_vault_share
            ),
            actions(
                name="submit",
                buttons=[{'label': 'Save Vault Share', 'value': 'save', 'color': 'success'}]
            )
        ])

        if form_data['submit'] == 'save':
            selected_wallet = form_data['wallet']
            new_vault_share = float(form_data['vault_share'])
            logger.debug(f"Input vault share: {form_data['vault_share']}, Parsed: {new_vault_share}")

            # Update vault share in config
            for entry in config['krystal_vault_wallet_chain_ids']:
                if entry['wallet'] == selected_wallet:
                    entry['vault_share'] = new_vault_share
                    break

            # Write back to YAML file
            with yaml_file.open('w') as f:
                yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)
            
            toast(f"Vault share for {selected_wallet[:8]}... updated to {new_vault_share}!", duration=3, color="success")
            logger.info(f"Updated vault share for wallet {selected_wallet} to {new_vault_share}")

    except FileNotFoundError:
        toast("Config file not found.", duration=5, color="error")
        logger.error(f"Config file not found: {yaml_file}")
    except yaml.YAMLError as e:
        toast(f"Invalid YAML format: {str(e)}", duration=5, color="error")
        logger.error(f"YAML parsing error: {str(e)}")
    except Exception as e:
        toast(f"Error updating vault share: {str(e)}", duration=5, color="error")
        logger.error(f"Error updating vault share: {str(e)}")

async def render_lp_summary(dataframes, error_flags):
    """Render LP summary with total value, chain dropdown, protocol dropdown, and protocol/pool breakdowns."""
    with use_scope('lp_summary_content', clear=True):
        krystal_error = error_flags.get('krystal_error', False)
        meteora_error = error_flags.get('meteora_error', False)

        # Initialize data structures
        lp_data = []

        # Process Krystal data
        if "Krystal" in dataframes and not krystal_error:
            krystal_df = dataframes["Krystal"].copy()
            for _, row in krystal_df.iterrows():
                usd_value = float(row["Actual Value USD"]) if pd.notna(row["Actual Value USD"]) else 0
                chain = row["Chain"].lower() if isinstance(row["Chain"], str) else "unknown"
                protocol = row["Protocol"] if isinstance(row["Protocol"], str) else "Krystal"
                pair = f"{row['Token X Symbol']}-{row['Token Y Symbol']}" if pd.notna(row["Token X Symbol"]) and pd.notna(row["Token Y Symbol"]) else "Unknown"
                pool_address = row["Pool Address"] if isinstance(row["Pool Address"], str) else "unknown"
                lp_data.append({
                    "Chain": chain,
                    "Protocol": protocol,
                    "Pool Address": pool_address,
                    "Pair": pair,
                    "USD Value": usd_value
                })

        # Process Meteora data
        if "Meteora" in dataframes and not meteora_error:
            meteora_df = dataframes["Meteora"].copy()
            for _, row in meteora_df.iterrows():
                qty_x = float(row["Token X Qty"]) if pd.notna(row["Token X Qty"]) else 0
                price_x = float(row["Token X Price USD"]) if pd.notna(row["Token X Price USD"]) else 0
                qty_y = float(row["Token Y Qty"]) if pd.notna(row["Token Y Qty"]) else 0
                price_y = float(row["Token Y Price USD"]) if pd.notna(row["Token Y Price USD"]) else 0
                usd_value = (qty_x * price_x) + (qty_y * price_y)
                chain = "solana"  # Meteora is Solana-only
                protocol = "Meteora"
                pair = f"{row['Token X Symbol']}-{row['Token Y Symbol']}" if pd.notna(row["Token X Symbol"]) and pd.notna(row["Token Y Symbol"]) else "Unknown"
                pool_address = row["Pool Address"] if isinstance(row["Pool Address"], str) else "unknown"
                lp_data.append({
                    "Chain": chain,
                    "Protocol": protocol,
                    "Pool Address": pool_address,
                    "Pair": pair,
                    "USD Value": usd_value
                })

        if not lp_data:
            put_text("No LP data available.")
            logger.warning("No LP data available for summary")
            return

        # Create DataFrame
        lp_df = pd.DataFrame(lp_data)
        logger.debug(f"LP DataFrame: {lp_df.head().to_string()}")
        total_lp_value = lp_df["USD Value"].sum()

        # Get unique chains
        chains = sorted([c for c in lp_df["Chain"].unique().tolist() if c != "unknown"])
        chain_options = [{"label": "All Chains", "value": "all"}] + [{"label": chain.capitalize(), "value": chain} for chain in chains]

        # Render total LP value
        put_markdown(f"**Total LP Value Across All Chains:** {format_usd(total_lp_value)}")

        # Render chain dropdown
        selected_chain = await select(
            "Select Chain",
            options=chain_options,
            value="all",
            help_text="Choose a chain to view detailed LP breakdown."
        )

        # Clear details scope and render updated data
        with use_scope('lp_details', clear=True):
            if selected_chain == "all":
                # Protocol breakdown for all chains
                protocol_totals = lp_df.groupby("Protocol")["USD Value"].sum().reset_index()
                protocol_table = [
                    [row["Protocol"], format_usd(row["USD Value"])]
                    for _, row in protocol_totals.iterrows()
                ]
                put_markdown("### LP Value by Protocol (All Chains)")
                put_table(protocol_table, header=["Protocol", "USD Value"])

                # Pool breakdown for all chains
                pool_totals = lp_df.groupby(["Pool Address", "Pair"])["USD Value"].sum().reset_index()
                pool_table = [
                    [row["Pair"], row["Pool Address"][:8] + "..." if row["Pool Address"] != "unknown" else "Unknown", format_usd(row["USD Value"])]
                    for _, row in pool_totals.iterrows()
                ]
                put_markdown("### LP Value by Pool (All Chains)")
                put_table(pool_table, header=["Pair", "Pool Address", "USD Value"])

            else:
                # Filter for selected chain
                chain_df = lp_df[lp_df["Chain"] == selected_chain]
                chain_total = chain_df["USD Value"].sum()
                put_markdown(f"**Total LP Value for {selected_chain.capitalize()}:** {format_usd(chain_total)}")

                # Protocol breakdown
                protocol_totals = chain_df.groupby("Protocol")["USD Value"].sum().reset_index()
                protocol_table = [
                    [row["Protocol"], format_usd(row["USD Value"])]
                    for _, row in protocol_totals.iterrows()
                ]
                put_markdown(f"### LP Value by Protocol ({selected_chain.capitalize()})")
                put_table(protocol_table, header=["Protocol", "USD Value"])

                # Get unique protocols for the selected chain
                protocols = sorted(chain_df["Protocol"].unique().tolist())
                protocol_options = [{"label": "All Protocols", "value": "all"}] + [{"label": protocol, "value": protocol} for protocol in protocols]

                # Render protocol dropdown
                selected_protocol = await select(
                    "Select Protocol",
                    options=protocol_options,
                    value="all",
                    help_text="Choose a protocol to view LP positions for the selected chain."
                )

                # Clear protocol details scope and render pool breakdown
                with use_scope('lp_protocol_details', clear=True):
                    if selected_protocol == "all":
                        # Pool breakdown for all protocols on the chain
                        pool_totals = chain_df.groupby(["Pool Address", "Pair"])["USD Value"].sum().reset_index()
                    else:
                        # Filter for selected protocol
                        pool_totals = chain_df[chain_df["Protocol"] == selected_protocol].groupby(["Pool Address", "Pair"])["USD Value"].sum().reset_index()

                    pool_table = [
                        [row["Pair"], row["Pool Address"][:8] + "..." if row["Pool Address"] != "unknown" else "Unknown", format_usd(row["USD Value"])]
                        for _, row in pool_totals.iterrows()
                    ]
                    put_markdown(f"### LP Value by Pool ({selected_chain.capitalize()}" + (f", {selected_protocol})" if selected_protocol != "all" else ")"))
                    put_table(pool_table, header=["Pair", "Pool Address", "USD Value"])

@config(theme="yeti")
async def main():
    with use_scope('dashboard', clear=True):
        put_markdown("# 🔮 🧙‍♂️ 🧪 💸 CM's Hedging Dashboard 💸 🧪 🧙‍♂️ 🔮")
        put_text("\n My wife's boyfriend says Bitcoin has no intrinsic value.")

        HEDGABLE_TOKENS = load_hedgeable_tokens()
        data = load_data()
        dataframes = data['dataframes']
        error_flags = data['error_flags']
        errors = data['errors']

        if errors.get('has_error', False):
            error_text = "\n".join([f"- {msg}" for msg in errors.get('messages', [])])
            put_error(error_text, f"Data Fetching Errors: {error_text}")
            logger.info(f"Successfully displayed error messages: {error_text}")
        else:
            logger.info("No data fetching errors detected")

        
        # Hedging Dashboard
        put_markdown("## Hedging Dashboard")
        if "Rebalanced" in dataframes or "Hedging" in dataframes:
            meteora_updated = error_flags['lp'].get("last_meteora_lp_update", "Not available")
            krystal_updated = error_flags['lp'].get("last_krystal_lp_update", "Not available")
            hedge_updated = error_flags['hedge'].get("last_updated_hedge", "Not available")
            put_markdown(
                f"**Last Meteora LP Update:** {meteora_updated}  \n"
                f"**Last Krystal LP Update:** {krystal_updated}  \n"
                f"**Last Hedge Data Update:** {hedge_updated}"
            )
            async def handle_run_workflow():
                logger.info(f"Run Workflow clicked: {WORKFLOW_SHELL_SCRIPT}")
                toast("Updating data... you can check BTC dominance in the meanwhile 🟠", duration=10, color="warning")
                success, output = await run_shell_script(WORKFLOW_SHELL_SCRIPT)
                toast(
                    "Data updated successfully ✅" if success else f"Update failed: {output}",
                    duration=5,
                    color="success" if success else "error"
                )
            put_buttons(
                [{'label': 'Update LP and Hedge Data 🌊', 'value': 'run_workflow', 'color': 'primary'}],
                onclick=lambda _: run_async(handle_run_workflow())
            )

            async def handle_run_hedge():
                logger.info(f"Run Hedge clicked: {HEDGE_SHELL_SCRIPT}")
                toast("Running hedge workflow... always keep your hedge fresh 🧊", duration=10, color="warning")
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

            render_hedging_table(dataframes, data['errors'], hedge_actions)
        else:
            put_text("No rebalancing or hedging data available.")


        put_markdown("## Vault Share Configuration")
        put_text("You can change the vault share for your Krystal vault wallets. \nFetch updated data after saving new vault shares.")
        put_buttons(
            [{'label': 'Change Vault Share 💸', 'value': 'vault_share', 'color': 'primary'}],
            onclick=lambda _: run_async(handle_vault_share_change())
        )

        put_markdown("## Hedge Automation")
        put_text("Select only the tokens you want to put on auto-hedge mode, and save the configuration. \nRemember to refresh the page so it can reload the data and the changes take effect.")
        options, auto_hedge_tokens = render_hedge_automation()
        hedgable_tokens = sorted([ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()])
        
        async def handle_config_change():
            if options:
                logger.debug(f"Rendering form with options: {options}")
                logger.debug(f"Current auto_hedge_tokens: {auto_hedge_tokens}")
                pre_selected = [token for token in hedgable_tokens if auto_hedge_tokens.get(token, False)]
                form_data = await input_group("Auto-hedge Tokens", [
                    checkbox(
                        name="auto_hedge_tokens",
                        options=options,
                        value=pre_selected,
                        inline=True,
                        help_text="Select tokens to auto-hedge"
                    ),
                    actions(
                        name="submit",
                        buttons=[{'label': 'Save Configuration', 'value': 'save', 'color': 'success'}]
                    )
                ])
                if form_data['submit'] == 'save':
                    new_auto_hedge_tokens = {token: token in form_data['auto_hedge_tokens'] for token in hedgable_tokens}
                    logger.debug(f"Saving new auto_hedge_tokens: {new_auto_hedge_tokens}")
                    try:
                        save_auto_hedge_tokens(new_auto_hedge_tokens)
                        toast("Configuration saved successfully!", duration=3, color="success")
                    except Exception as e:
                        logger.error(f"Error saving auto_hedge_tokens: {str(e)}")
                        toast(f"Error saving configuration: {str(e)}", duration=5, color="error")
            else:
                toast("No hedgeable tokens available.", duration=3, color="warning")
        
        if options:
            put_buttons(
                [{'label': 'Change Automation Configuration', 'value': 'config', 'color': 'primary'}],
                onclick=lambda _: run_async(handle_config_change())
            )
        else:
            put_text("No hedgeable tokens available.")

        await render_custom_hedge_section(hedge_actions)
        await render_add_token_mapping_section()


        # Wallet Positions
        put_markdown("## Wallet Positions")
        meteora_updated = error_flags['lp'].get("last_meteora_lp_update", "Not available")
        krystal_updated = error_flags['lp'].get("last_krystal_lp_update", "Not available")
        put_markdown(f"**Last Meteora LP Update:** {meteora_updated}  \n**Last Krystal LP Update:** {krystal_updated}")
        render_wallet_positions(dataframes, error_flags)

        # PnL Section
        put_markdown("## LP Positions P&L")
        async def handle_calculate_pnl():
            logger.info(f"Calculating PL button clicked, execute {PNL_SHELL_SCRIPT}")
            toast("Running P&L calculations... this might take a while, you can search for some new shitcoin in the meantime 📈", duration=10, color="warning")
            success, output = await run_shell_script(PNL_SHELL_SCRIPT)
            toast("P&L calculations completed successfully, it'a Lambo 🚗 or a scooter 🛴???" if success else f"P&L calc failed: {output}", duration=5, color=("success" if success else "error"))
        put_buttons(
            [{'label': 'Calculate P&L 💰', 'value': 'calculate_pl', 'color': 'primary'}],
            onclick=lambda x: run_async(handle_calculate_pnl())
        )
        render_pnl_tables(dataframes, error_flags)

        # LP Summary Section (at the end)
        put_markdown("## LP Summary")
        async def handle_lp_summary():
            logger.info("LP Summary button clicked")
            await render_lp_summary(dataframes, error_flags)
        put_buttons(
            [{'label': 'View LP Summary 📊', 'value': 'lp_summary', 'color': 'primary'}],
            onclick=lambda _: run_async(handle_lp_summary())
        )

def cleanup():
    asyncio.run(order_manager.close())

atexit.register(cleanup)

if __name__ == "__main__":
    start_server(main, port=8080, host="0.0.0.0", debug=True)