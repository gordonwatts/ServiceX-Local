import logging
from functools import wraps
from pathlib import Path


def log_to_file(log_file):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            handler = logging.FileHandler(log_file, mode='a')  # Append mode
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            try:
                return func(*args, **kwargs)
            finally:
                logger.removeHandler(handler)
                handler.close()

        return wrapper

    return decorator
