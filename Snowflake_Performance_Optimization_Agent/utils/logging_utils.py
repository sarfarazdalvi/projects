"""
Logging utilities for the Snowflake Performance Agent.
"""

import logging
import logging.handlers


def setup_logging(log_level=logging.INFO):
    """
    Set up console-only logging (Docker-friendly).
    
    Args:
        log_level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    # Create formatter for console
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    # === CONSOLE HANDLER ONLY ===
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Get module-specific logger
    logger = logging.getLogger(__name__)
    
    # Log the logging setup completion
    logger.info("=" * 80)
    logger.info("🚀 SNOWFLAKE PERFORMANCE AGENT - LANGGRAPH IMPLEMENTATION")
    logger.info("=" * 80)
    logger.info(f"📝 Logging configured:")
    logger.info(f"   • Console: {log_level} level (Docker-friendly)")
    logger.info("=" * 80)
    
    return logger