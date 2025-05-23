"""
Logging utilities for the project.
"""
import logging
from typing import Optional
from config import LOG_CONFIG

def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Set up a logger with the specified name and level.
    
    Args:
        name: The name of the logger
        level: Optional logging level (defaults to LOG_CONFIG['level'])
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Set level
    log_level = level or LOG_CONFIG['level']
    logger.setLevel(getattr(logging, log_level))
    
    # Create console handler if no handlers exist
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt=LOG_CONFIG['format'],
            datefmt=LOG_CONFIG['datefmt']
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger 