import logging
import logging.config

from .config import config

logger = logging.getLogger("dbmaster")
logging.config.dictConfig(config.logging)

__all__ = ["config"]
