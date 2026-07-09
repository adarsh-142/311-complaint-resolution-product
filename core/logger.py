import logging
from core.config import settings

def setup_logger():
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )