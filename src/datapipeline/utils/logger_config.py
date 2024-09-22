from dataclasses import dataclass
import logging


@dataclass
class Logger:
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.INFO)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
