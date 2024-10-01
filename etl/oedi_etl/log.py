"""
logger: A logger initializer to configure the logging settings for the ETL job. A logging function sharing the same configuration is shared across all ETL processes and tasks. This ensures that all logs are written to the same file and have the same format.

Note that you cannot instantiate a logger instance here and share it across processes because the logger is not picklable. Instead, a logger instance is created in each process using the shared configuration.
"""

import logging
import os
from datetime import datetime

import structlog
from structlog.stdlib import ProcessorFormatter


# Default values for logging
DEFAULT_LOGGING_LEVEL = "DEBUG"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILENAME = "etl.log"

# Load app configuration or use defaults
#! TODO all jobs share the same logs (only partitioned by date). You can make this at the second level but anyone launched at the same time share these files. This is a quick way to get logs from multiple jobs in one place but can be ugly if u run multiple jobs -> need better solution
logging_level = os.getenv("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL)
log_filename = os.getenv("LOG_FILENAME", DEFAULT_LOG_FILENAME)
log_dir = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)
print(f"Logging level: {logging_level}")

# ETL DIR diff for local and Glue. On Glue,  you won't have access to this level
if os.getenv("ETL_EXECUTION_ENV") == "AWS_GLUE":
    ETL_DIR = "/tmp/"
else:
    ETL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../etl"))



# prep
log_level = getattr(logging, logging_level.upper(), logging.INFO)
log_file_path = os.path.normpath(
    os.path.join(
        ETL_DIR,
        log_dir,
        # once per day log file
        f"{datetime.now().strftime('%Y-%m-%d')}-{log_filename}",
    )
)
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# Define the formatter
formatter = ProcessorFormatter(
    processor=structlog.dev.ConsoleRenderer(colors=False),
    foreign_pre_chain=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ],
)

# File handler for logging to file
file_handler = logging.FileHandler(log_file_path, mode="a")
file_handler.setLevel(log_level)
file_handler.setFormatter(formatter)

# Console handler for critical messages only
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)
console_handler.setFormatter(formatter)


# logging config
logging.basicConfig(
    level=log_level,
    handlers=[file_handler, console_handler],
)


# Structlog configuration
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%S.%f", utc=True),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        ProcessorFormatter.wrap_for_formatter,
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
