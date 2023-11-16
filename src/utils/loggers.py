import logging


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"


def set_console_logger(level=logging.INFO, format=LOG_FORMAT):  # pragma: no cover
    logging.basicConfig(level=level, format=format, handlers=[logging.StreamHandler()])
