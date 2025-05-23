import pandas as pd
import json
import logging
import os
from pathlib import Path
from common.path_config import (
    REBALANCING_LATEST_CSV, KRYSTAL_LATEST_CSV, METEORA_LATEST_CSV, HEDGING_LATEST_CSV,
    METEORA_PNL_CSV, KRYSTAL_POOL_PNL_CSV, LOG_DIR, HEDGEABLE_TOKENS_JSON, ENCOUNTERED_TOKENS_JSON
)


logger = logging.getLogger(__name__)

def load_data():
    """
    Load CSV and JSON error flag files, handling errors and returning structured data.

    Returns:
        dict: {
            'dataframes': {str: pd.DataFrame},  # Loaded CSVs
            'error_flags': {
                'hedge': dict,  # Hedge error flags
                'lp': dict      # LP error flags
            },
            'errors': {
                'has_error': bool,
                'krystal_error': bool,
                'meteora_error': bool,
                'hedging_error': bool,
                'messages': list[str]
            }
        }
    """
    HEDGE_ERROR_FLAGS_PATH = Path(LOG_DIR) / 'hedge_fetching_errors.json'
    LP_ERROR_FLAGS_PATH = Path(LOG_DIR) / 'lp_fetching_errors.json'

    dataframes = {}
    error_flags = {'hedge': {}, 'lp': {}}
    errors = {
        'has_error': False,
        'krystal_error': False,
        'meteora_error': False,
        'hedging_error': False,
        'messages': []
    }

    # Load error flags
    try:
        if HEDGE_ERROR_FLAGS_PATH.exists():
            with HEDGE_ERROR_FLAGS_PATH.open('r') as f:
                error_flags['hedge'] = json.load(f)
                if error_flags['hedge'].get("HEDGING_FETCHING_BITGET_ERROR", False):
                    errors['has_error'] = True
                    errors['hedging_error'] = True
                    errors['messages'].append("Failed to fetch Bitget hedging data")
                if "last_updated_hedge" not in error_flags['hedge']:
                    logger.warning("last_updated_hedge missing in hedge_fetching_errors.json")
        else:
            logger.warning(f"Hedging error flags file not found: {HEDGE_ERROR_FLAGS_PATH}")
            errors['has_error'] = True
            errors['hedging_error'] = True
            errors['messages'].append("Hedging error flags file missing")
    except Exception as e:
        logger.error(f"Error reading hedging error flags: {str(e)}")
        errors['has_error'] = True
        errors['hedging_error'] = True
        errors['messages'].append("Error reading hedging error flags")

    try:
        if LP_ERROR_FLAGS_PATH.exists():
            with LP_ERROR_FLAGS_PATH.open('r') as f:
                error_flags['lp'] = json.load(f)
                if error_flags['lp'].get("LP_FETCHING_KRYSTAL_ERROR", False):
                    errors['has_error'] = True
                    errors['krystal_error'] = True
                    errors['messages'].append("Failed to fetch Krystal LP data")
                if error_flags['lp'].get("LP_FETCHING_METEORA_ERROR", False):
                    errors['has_error'] = True
                    errors['meteora_error'] = True
                    errors['messages'].append("Failed to fetch Meteora LP data")
                if "last_meteora_lp_update" not in error_flags['lp']:
                    logger.warning("last_meteora_lp_update missing in lp_fetching_errors.json")
                if "last_krystal_lp_update" not in error_flags['lp']:
                    logger.warning("last_krystal_lp_update missing in lp_fetching_errors.json")
        else:
            logger.warning(f"LP error flags file not found: {LP_ERROR_FLAGS_PATH}")
            errors['has_error'] = True
            errors['krystal_error'] = True
            errors['meteora_error'] = True
            errors['messages'].append("LP error flags file missing")
    except Exception as e:
        logger.error(f"Error reading LP error flags: {str(e)}")
        errors['has_error'] = True
        errors['krystal_error'] = True
        errors['meteora_error'] = True
        errors['messages'].append("Error reading LP error flags")

    # Load CSVs
    csv_files = {
        "Rebalancing": REBALANCING_LATEST_CSV,
        "Krystal": KRYSTAL_LATEST_CSV,
        "Meteora": METEORA_LATEST_CSV,
        "Hedging": HEDGING_LATEST_CSV,
        "Meteora PnL": METEORA_PNL_CSV,
        "Krystal PnL": KRYSTAL_POOL_PNL_CSV
    }

    for name, path in csv_files.items():
        if not os.path.exists(path):
            logger.warning(f"CSV file not found: {path}")
            errors['messages'].append(f"Error: {path} not found")
            continue
        try:
            dataframes[name] = pd.read_csv(path)
            logger.info(f"Loaded CSV: {path}")
            if name == "Krystal" and errors['krystal_error']:
                logger.warning(f"Krystal CSV {path} may be stale due to LP fetching error")
            if name == "Meteora" and errors['meteora_error']:
                logger.warning(f"Meteora CSV {path} may be stale due to LP fetching error")
            if name == "Hedging" and errors['hedging_error']:
                logger.warning(f"Hedging CSV {path} may be stale due to hedging fetching error")
        except Exception as e:
            logger.error(f"Error reading CSV {path}: {str(e)}")
            errors['messages'].append(f"Error reading {name} CSV: {str(e)}")

    return {
        'dataframes': dataframes,
        'error_flags': error_flags,
        'errors': errors
    }

def load_hedgeable_tokens() -> dict:
    """Load hedgeable tokens from JSON."""
    try:
        if HEDGEABLE_TOKENS_JSON.exists():
            with HEDGEABLE_TOKENS_JSON.open('r') as f:
                return json.load(f)
        else:
            logger.info(f"{HEDGEABLE_TOKENS_JSON} not found, initializing empty dictionary")
            return {}
    except Exception as e:
        logger.error(f"Error loading hedgeable tokens: {str(e)}")
        return {}

def load_encountered_tokens() -> dict:
    """Load encountered tokens from JSON."""
    try:
        if ENCOUNTERED_TOKENS_JSON.exists():
            with ENCOUNTERED_TOKENS_JSON.open('r') as f:
                return json.load(f)
        else:
            logger.info(f"{ENCOUNTERED_TOKENS_JSON} not found, initializing empty dictionary")
            return {}
    except Exception as e:
        logger.error(f"Error loading encountered tokens: {str(e)}")
        return {}

def load_json(file_path) -> dict:
    """Load JSON file."""
    try:
        if file_path.exists():
            with file_path.open('r') as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    logger.error(f"Invalid format in {file_path}: Expected dictionary")
                    return {}
                logger.debug(f"Loaded: {data}")
                return data
        else:
            logger.info(f"{file_path} not found, returning empty dictionary")
            return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON in {file_path}: {str(e)}")
        return {}
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        return {}