import json
import logging
import re
from pathlib import Path
from pywebio.input import input, select, input_group
from pywebio.output import put_text, toast, put_markdown, put_buttons
from pywebio.session import run_async
from common.data_loader import load_ticker_mappings, save_ticker_mappings, load_hedgeable_tokens

# Configure logging
logger = logging.getLogger(__name__)

CONFIG_DIR = Path("config")
HEDGEABLE_TOKENS_PATH = CONFIG_DIR / "hedgeable_tokens.json"

def add_token_mapping(onchain_ticker: str, bitget_ticker: str, factor: str = None) -> bool:
    """
    Add a token mapping to SYMBOL_MAP or BITGET_TOKENS_WITH_FACTOR_1000/10000.
    
    Args:
        onchain_ticker: On-chain ticker (e.g., "NEWTOKEN", "WPOL").
        bitget_ticker: Bitget ticker (e.g., "NEWTOKEN", "1000NEWTOKEN", "POL").
        factor: "1000", "10000", or None for direct mapping.
    
    Returns:
        bool: True if successful, False if validation fails.
    """
    # Validate inputs
    if not onchain_ticker or not bitget_ticker:
        logger.error("On-chain ticker and Bitget ticker cannot be empty")
        toast("Error: Ticker fields cannot be empty", color="error")
        return False

    onchain_ticker = onchain_ticker.strip().upper()
    bitget_ticker = bitget_ticker.strip().upper()

    if not re.match(r'^[A-Z0-9]+$', onchain_ticker) or not re.match(r'^[A-Z0-9]+$', bitget_ticker):
        logger.error(f"Invalid ticker format: onchain={onchain_ticker}, bitget={bitget_ticker}")
        toast("Error: Tickers must contain only letters and numbers", color="error")
        return False

    # Load current mappings
    mappings = load_ticker_mappings()
    symbol_map = mappings["SYMBOL_MAP"]
    bitget_tokens_1000 = mappings["BITGET_TOKENS_WITH_FACTOR_1000"]
    bitget_tokens_10000 = mappings["BITGET_TOKENS_WITH_FACTOR_10000"]

    # Check for duplicates
    if (onchain_ticker in symbol_map or 
        onchain_ticker in bitget_tokens_1000 or 
        onchain_ticker in bitget_tokens_10000):
        logger.error(f"Duplicate on-chain ticker: {onchain_ticker}")
        toast(f"Error: {onchain_ticker} already mapped", color="error")
        return False

    # Update mappings
    try:
        if factor == "1000":
            bitget_tokens_1000[onchain_ticker] = bitget_ticker
            logger.info(f"Added to BITGET_TOKENS_WITH_FACTOR_1000: {onchain_ticker} -> {bitget_ticker}")
        elif factor == "10000":
            bitget_tokens_10000[onchain_ticker] = bitget_ticker
            logger.info(f"Added to BITGET_TOKENS_WITH_FACTOR_10000: {onchain_ticker} -> {bitget_ticker}")
        else:
            symbol_map[onchain_ticker] = bitget_ticker
            logger.info(f"Added to SYMBOL_MAP: {onchain_ticker} -> {bitget_ticker}")

        # Save updated mappings
        save_ticker_mappings(mappings)

        toast(f"Successfully added mapping: {onchain_ticker} -> {bitget_ticker}", color="success")
        return True
    except Exception as e:
        logger.error(f"Error adding token mapping: {e}")
        toast("Error: Failed to add token mapping", color="error")
        return False

async def render_add_token_mapping_section():
    """
    Render a UI section to add a new token mapping.
    """
    put_markdown("## Add Token Mapping")
    try:
        logger.debug("Rendering add token mapping section")
        put_buttons(
            [{'label': 'Add New Token Mapping', 'value': 'add_mapping', 'color': 'primary'}],
            onclick=lambda _: run_async(show_add_token_mapping_form())
        )
    except Exception as e:
        logger.error(f"Error rendering token mapping section: {str(e)}")
        toast(f"Error rendering token mapping section: {str(e)}", duration=5, color="error")

async def show_add_token_mapping_form():
    """
    Show a form to input token mapping details.
    """
    logger.debug("Showing add token mapping form")
    inputs = await input_group(
        "Enter token mapping details",
        [
            input(
                "On-chain Ticker",
                name="onchain_ticker",
                placeholder="e.g., NEWTOKEN or WPOL",
                required=True
            ),
            input(
                "Bitget Ticker",
                name="bitget_ticker",
                placeholder="e.g., NEWTOKEN, 1000NEWTOKEN, or POL",
                required=True
            ),
            select(
                "Factor",
                name="factor",
                options=[
                    {"label": "None", "value": "None"},
                    {"label": "1000", "value": "1000"},
                    {"label": "10000", "value": "10000"}
                ],
                value="None"
            )
        ],
        cancelable=True
    )
    if inputs is None:
        logger.debug("Token mapping input cancelled")
        toast("Token mapping cancelled", duration=5, color="info")
        return

    logger.debug(f"Token mapping inputs: {inputs}")
    # Convert factor to None if "None" is selected
    factor = inputs["factor"] if inputs["factor"] != "None" else None
    success = add_token_mapping(
        onchain_ticker=inputs["onchain_ticker"],
        bitget_ticker=inputs["bitget_ticker"],
        factor=factor
    )

    if success:
        put_text(f"Added mapping: {inputs['onchain_ticker']} -> {inputs['bitget_ticker']}")
    else:
        put_text("Failed to add mapping. Check logs for details.")

def add_hedgeable_token_mapping(onchain_ticker: str, bitget_ticker: str, factor: str = None, contract_address: str = None, chain: str = None) -> bool:
    """
    Add a new hedgeable token mapping to SYMBOL_MAP or BITGET_TOKENS_WITH_FACTOR_1000/10000,
    optionally updating hedgeable_tokens.json with contract address and chain.
    
    Args:
        onchain_ticker: On-chain ticker (e.g., "NEWTOKEN").
        bitget_ticker: Bitget ticker (e.g., "NEWTOKEN", "1000NEWTOKEN").
        factor: "1000", "10000", or None for direct mapping.
        contract_address: Optional contract address for hedgeable_tokens.json.
        chain: Optional blockchain (e.g., "solana", "polygon").
    
    Returns:
        bool: True if successful, False if validation fails.
    """
    # Validate inputs
    if not onchain_ticker or not bitget_ticker:
        logger.error("On-chain ticker and Bitget ticker cannot be empty")
        toast("Error: Ticker fields cannot be empty", color="error")
        return False

    onchain_ticker = onchain_ticker.strip().upper()
    bitget_ticker = bitget_ticker.strip().upper()

    if not re.match(r'^[A-Z0-9]+$', onchain_ticker) or not re.match(r'^[A-Z0-9]+$', bitget_ticker):
        logger.error(f"Invalid ticker format: onchain={onchain_ticker}, bitget={bitget_ticker}")
        toast("Error: Tickers must contain only letters and numbers", color="error")
        return False

    # Load current mappings
    mappings = load_ticker_mappings()
    symbol_map = mappings["SYMBOL_MAP"]
    bitget_tokens_1000 = mappings["BITGET_TOKENS_WITH_FACTOR_1000"]
    bitget_tokens_10000 = mappings["BITGET_TOKENS_WITH_FACTOR_10000"]

    # Check for duplicates
    if (onchain_ticker in symbol_map or 
        onchain_ticker in bitget_tokens_1000 or 
        onchain_ticker in bitget_tokens_10000):
        logger.error(f"Duplicate on-chain ticker: {onchain_ticker}")
        toast(f"Error: {onchain_ticker} already mapped", color="error")
        return False

    # Update mappings
    try:
        if factor == "1000":
            bitget_tokens_1000[onchain_ticker] = bitget_ticker
            logger.info(f"Added to BITGET_TOKENS_WITH_FACTOR_1000: {onchain_ticker} -> {bitget_ticker}")
        elif factor == "10000":
            bitget_tokens_10000[onchain_ticker] = bitget_ticker
            logger.info(f"Added to BITGET_TOKENS_WITH_FACTOR_10000: {onchain_ticker} -> {bitget_ticker}")
        else:
            symbol_map[onchain_ticker] = bitget_ticker
            logger.info(f"Added to SYMBOL_MAP: {onchain_ticker} -> {bitget_ticker}")

        # Save updated mappings
        save_ticker_mappings(mappings)

        # Update hedgeable_tokens.json if CA and chain provided
        if contract_address and chain:
            try:
                contract_address = contract_address.strip().lower()
                chain = chain.strip().lower()
                if not re.match(r'^[a-zA-Z0-9]+$', contract_address):
                    logger.error(f"Invalid contract address: {contract_address}")
                    toast("Error: Invalid contract address format", color="error")
                    return False
                if chain not in ["solana", "polygon", "ethereum", "bsc"]:
                    logger.error(f"Unsupported chain: {chain}")
                    toast(f"Error: Unsupported chain {chain}", color="error")
                    return False

                hedgeable_tokens = load_hedgeable_tokens()
                bitget_symbol = f"{bitget_ticker}USDT_UMCBL"
                if bitget_symbol not in hedgeable_tokens:
                    hedgeable_tokens[bitget_symbol] = {}
                if chain not in hedgeable_tokens[bitget_symbol]:
                    hedgeable_tokens[bitget_symbol][chain] = []
                if contract_address not in hedgeable_tokens[bitget_symbol][chain]:
                    hedgeable_tokens[bitget_symbol][chain].append(contract_address)
                    with HEDGEABLE_TOKENS_PATH.open('w') as f:
                        json.dump(hedgeable_tokens, f, indent=2)
                    logger.info(f"Added to hedgeable_tokens.json: {bitget_symbol}, chain={chain}, CA={contract_address}")
            except Exception as e:
                logger.error(f"Error updating hedgeable_tokens.json: {e}")
                toast("Error: Failed to update hedgeable tokens", color="error")
                return False

        toast(f"Successfully added mapping: {onchain_ticker} -> {bitget_ticker}", color="success")
        return True
    except Exception as e:
        logger.error(f"Error adding token mapping: {e}")
        toast("Error: Failed to add token mapping", color="error")
        return False

def render_add_token_form():
    """Render a UI form to add a new hedgeable token mapping."""
    form_data = input_group("Add New Hedgeable Token", [
        input("On-chain Ticker", name="onchain_ticker", placeholder="e.g., NEWTOKEN", required=True),
        input("Bitget Ticker", name="bitget_ticker", placeholder="e.g., NEWTOKEN or 1000NEWTOKEN", required=True),
        select("Factor", options=["None", "1000", "10000"], name="factor", value="None"),
        input("Contract Address (optional)", name="contract_address", placeholder="e.g., Hjw6bEcHtbHGpQr8onG3izfJY5DJiWdt7uk2BfdSpump"),
        select("Chain (optional)", options=["", "solana", "polygon", "ethereum", "bsc"], name="chain", value="")
    ])

    factor = form_data["factor"] if form_data["factor"] != "None" else None
    contract_address = form_data["contract_address"] or None
    chain = form_data["chain"] or None

    success = add_hedgeable_token_mapping(
        onchain_ticker=form_data["onchain_ticker"],
        bitget_ticker=form_data["bitget_ticker"],
        factor=factor,
        contract_address=contract_address,
        chain=chain
    )

    if success:
        put_text(f"Added mapping: {form_data['onchain_ticker']} -> {form_data['bitget_ticker']}")
    else:
        put_text("Failed to add mapping. Check logs for details.")