import yaml
from common.path_config import PYTHON_YAML_CONFIG_PATH
import logging

logger = logging.getLogger(__name__)

# Global config object
CONFIG = None

def load_config():
    """Load configuration from config.yaml, falling back to defaults."""
    global CONFIG
    if CONFIG is not None:
        return CONFIG
    
    try:
        if PYTHON_YAML_CONFIG_PATH.exists():
            with PYTHON_YAML_CONFIG_PATH.open('r') as f:
                CONFIG = yaml.safe_load(f) or {}
            logger.info(f"Loaded configuration from {PYTHON_YAML_CONFIG_PATH}")
        else:
            logger.warning(f"Config file not found: {PYTHON_YAML_CONFIG_PATH}.")

    except Exception as e:
        logger.error(f"Error loading config file {PYTHON_YAML_CONFIG_PATH}: {str(e)}.")


def get_config():
    """Get the global configuration, loading it if necessary."""
    if CONFIG is None:
        load_config()
    return CONFIG

# Load config at module import
load_config()