import logging
from functools import wraps


def log_to_file(log_file):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger()
            original_logging_level = logger.level
            # Lower the root level to INFO so the file handler can capture
            # transform output even when the user's default is WARNING, but
            # never raise above the user's level — otherwise callers who
            # explicitly set DEBUG (e.g. caplog.at_level(DEBUG) in tests)
            # would lose their messages.
            effective = logger.getEffectiveLevel()
            logger.setLevel(min(effective, logging.INFO))

            handler = logging.FileHandler(log_file, mode="a")  # Append mode
            handler.setLevel(logging.INFO)
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
