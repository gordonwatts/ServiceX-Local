import logging
from functools import wraps


def log_to_file(log_file):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger()
            original_logging_level = logger.level
            logger.setLevel(logging.DEBUG)

            handler = logging.FileHandler(log_file, mode="a")  # Append mode
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
                logger.setLevel(original_logging_level)

        return wrapper

    return decorator
