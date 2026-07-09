from core.config import settings
import os
from datetime import datetime
import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


def save_processed_data(df, batch_size=10000, timeout_seconds=300):
    """Save processed data as a single JSON file.

    Spark's JSON writer emits a directory containing part files, so this helper
    materializes the result and writes a normal JSON file at the requested path.
    """
    base_path = settings.PROCESSED_PATH
    os.makedirs(base_path, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(base_path, f"processed_{timestamp}.json")
    output_file = Path(output_path)

    result = [None]
    exception = [None]

    def save_worker():
        try:
            row_count = df.count()
            logger.info(f"Saving {row_count} records to {output_path}")

            if row_count == 0:
                output_file.write_text("[]", encoding="utf-8")
                result[0] = str(output_file)
                return

            rows = df.collect()
            records = [row.asDict(recursive=True) for row in rows]
            output_file.write_text(json.dumps(records, default=str, indent=2), encoding="utf-8")
            result[0] = str(output_file)
            logger.info(f"Saved {len(records)} records to {output_path}")
        except Exception as e:
            exception[0] = e

    thread = threading.Thread(target=save_worker, daemon=False)
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        logger.error(f"Timeout saving to {output_path}")
        raise TimeoutError(f"Timed out after {timeout_seconds}s while saving processed data")

    if exception[0]:
        logger.error(f"Error saving processed data: {exception[0]}")
        raise exception[0]

    return result[0]