import logging
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path

from core.config import settings

logger = logging.getLogger(__name__)


def save_processed_data(df, batch_size=10000, timeout_seconds=300):
    """Save processed data as a single JSON file without full DataFrame collect()."""
    base_path = settings.PROCESSED_PATH
    os.makedirs(base_path, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(base_path, f"processed_{timestamp}.json")
    output_file = Path(output_path)
    tmp_dir = Path(base_path) / f"processed_{timestamp}_parts"
    write_partitions = max(int(getattr(settings, "PROCESSED_WRITE_PARTITIONS", 1)), 1)

    result = [None]
    exception = [None]

    def save_worker():
        try:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)

            try:
                writer_df = (
                    df.coalesce(1) if write_partitions == 1 else df.repartition(write_partitions)
                )
                writer_df.write.mode("overwrite").json(str(tmp_dir))

                part_files = sorted(tmp_dir.glob("part-*.json"))
                if not part_files:
                    output_file.write_text("[]", encoding="utf-8")
                    result[0] = str(output_file)
                    return

                with output_file.open("w", encoding="utf-8") as out_f:
                    out_f.write("[")
                    wrote_any = False
                    for part_file in part_files:
                        with part_file.open("r", encoding="utf-8") as in_f:
                            for line in in_f:
                                row_text = line.strip()
                                if not row_text:
                                    continue
                                if wrote_any:
                                    out_f.write(",")
                                out_f.write("\n")
                                out_f.write(row_text)
                                wrote_any = True

                    if wrote_any:
                        out_f.write("\n")
                    out_f.write("]")

                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception as spark_write_error:
                logger.warning(
                    "Spark JSON writer failed; falling back to iterator-based JSON persistence: %s",
                    spark_write_error,
                )
                with output_file.open("w", encoding="utf-8") as out_f:
                    out_f.write("[")
                    wrote_any = False
                    for row_json in df.toJSON().toLocalIterator():
                        if wrote_any:
                            out_f.write(",")
                        out_f.write("\n")
                        out_f.write(row_json)
                        wrote_any = True

                    if wrote_any:
                        out_f.write("\n")
                    out_f.write("]")

            result[0] = str(output_file)
            logger.info(
                f"Saved processed snapshot to {output_path} using {write_partitions} partition(s)"
            )
        except Exception as e:
            shutil.rmtree(tmp_dir, ignore_errors=True)
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
