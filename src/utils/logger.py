import logging
import os

def get_logger(name: str = "retail_pipeline") -> logging.Logger:
    """
    Creates and returns a configured logger.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # prevents duplicate handlers

    logger.setLevel(logging.INFO)

    # formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # optional file handler
    os.makedirs("../logs", exist_ok=True)
    file_handler = logging.FileHandler("../logs/pipeline.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
