import logging
import pathlib
import sys
from typing import Optional, Union, TextIO


def setup_logger(
    name: Optional[str] = None,
    log_path: Optional[Union[str, pathlib.Path]] = None,
    level: int = logging.INFO,
    stream: Optional[TextIO] = sys.stdout,
    log_format: str = "%(asctime)s | %(levelname)s | %(message)s",
    date_format: str = "%Y-%m-%dT%H:%M:%SZ",
    file_mode: str = "w",
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Optional stream handler
    if stream is not None:
        stream_handler = logging.StreamHandler(stream=stream)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # Optional file handler
    if log_path:
        file_handler = logging.FileHandler(log_path, mode=file_mode)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
