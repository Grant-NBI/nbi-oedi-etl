"""
logger: A logger initializer to configure the logging settings for the ETL job. A logging function sharing the same configuration is shared across all ETL processes and tasks. This ensures that all logs are written to the same file and have the same format.

Note that you cannot instantiate a logger instance here and share it across processes because the logger is not picklable. Instead, a logger instance is created in each process using the shared configuration.
"""

import json
import logging
import os
from datetime import datetime

import structlog

# settings
PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../../"))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config.json")

# Default values for logging
DEFAULT_LOGGING_LEVEL = "DEBUG"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILENAME = "etl.log"


# Load configuration from config.json
def load_config():
    """
    Loads the configuration from a JSON file specified by CONFIG_PATH.

    Tries to open and read the configuration file. If the file is not found,
    it prints an error message and returns an empty dictionary.

    Returns:
        dict: The configuration settings loaded from the file, or an empty
        dictionary if the file is not found.
    """
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        print(f"Configuration file '{CONFIG_PATH}' not found. Using default settings.")
        return {}


# Load app configuration or use defaults
app_config = load_config()
settings = app_config.get("settings", {})
logging_level = app_config.get("logging_level", DEFAULT_LOGGING_LEVEL)
log_filename = app_config.get("log_filename", DEFAULT_LOG_FILENAME)
log_dir = app_config.get("log_dir", DEFAULT_LOG_DIR)
print(f"Logging level: {logging_level}")

# prep
log_level = getattr(logging, logging_level.upper(), logging.INFO)
log_file_path = os.path.normpath(
    os.path.join(
        PROJECT_ROOT,
        "scripts/etl",
        log_dir,
        # once per day log file
        f"{datetime.now().strftime('%Y-%m-%d')}-{log_filename}",
    )
)
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# File handler for logging to file
file_handler = logging.FileHandler(log_file_path, mode="a")
file_handler.setLevel(log_level)

# Console handler for critical messages only
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)

# logging config
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=log_level,
    handlers=[file_handler, console_handler],
)


# Structlog configuration
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(log_level),
    cache_logger_on_first_use=True,
)


# Shared logger function
def get_logger():
    """
    Get a configured logger instance.

    Returns:
        structlog.BoundLogger: A configured logger instance from structlog.
    """
    return structlog.get_logger()
