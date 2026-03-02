"""
Utility functions and constants for Snowflake Performance Agent.
Contains shared utilities, logging setup, and constants.
"""

from .logging_utils import setup_logging
from .constants import OPTIMIZATION_RULES_TEXT

__all__ = [
    'setup_logging',
    'OPTIMIZATION_RULES_TEXT'
]