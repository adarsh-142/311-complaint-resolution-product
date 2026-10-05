import json
import os
from datetime import datetime

from core.config import settings


def save_raw_data(data):
    base_path = settings.DATA_PATH
    os.makedirs(base_path, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{base_path}/311_data_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    return file_path
