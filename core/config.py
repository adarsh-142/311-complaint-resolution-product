import os
from datetime import datetime, timedelta

class Settings:
    APP_NAME = "Agentic Analytics System"
    DATA_PATH = os.getenv("DATA_PATH", "data/raw/")
    PROCESSED_PATH = os.getenv("PROCESSED_PATH", "data/processed/")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    API_URL = os.getenv("API_URL", "https://data.cityofnewyork.us/resource/erm2-nwe9.json")
    API_LIMIT = int(os.getenv("API_LIMIT", 25000))
    API_BATCH_SIZE = int(os.getenv("API_BATCH_SIZE", 1000))
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", 240))
    
    # Date filtering (default: last 30 days)
    _end_date = datetime.now()
    _start_date = _end_date - timedelta(days=30)
    API_START_DATE = os.getenv("API_START_DATE", _start_date.strftime("%Y-%m-%d"))
    API_END_DATE = os.getenv("API_END_DATE", _end_date.strftime("%Y-%m-%d"))

settings = Settings()