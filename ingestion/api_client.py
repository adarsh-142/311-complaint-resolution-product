import time
from typing import Any
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def fetch_311_data(
    api_url: str,
    total_limit: int = 5000,
    batch_size: int = 1000,
    timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 1.5,
    start_date: str = None,
    end_date: str = None,
) -> list[dict[str, Any]]:

    from core.config import settings
    
    # Use provided dates or fall back to config settings
    if start_date is None:
        start_date = settings.API_START_DATE
    if end_date is None:
        end_date = settings.API_END_DATE
    
    all_data = []
    offset = 0
    offset_retry_count = {}  # Track retries per offset
    MAX_RETRIES_PER_OFFSET = 3  # Circuit breaker per offset
    unlimited = total_limit is None or total_limit <= 0

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=list(RETRYABLE_STATUS_CODES),
        allowed_methods=["GET"],
        backoff_factor=backoff_factor,
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Format dates for SOQL query
    print(f"\n[FETCH] Fetching data from {start_date} to {end_date}")
    soql_date_filter = f"created_date BETWEEN '{start_date}T00:00:00' AND '{end_date}T23:59:59'"

    while unlimited or len(all_data) < total_limit:

        # Calculate remaining records to fetch
        if unlimited:
            current_batch_size = batch_size
        else:
            remaining = total_limit - len(all_data)
            current_batch_size = min(batch_size, remaining)
        
        finished_message = f"{len(all_data)}/{total_limit}" if not unlimited else f"{len(all_data)}/unlimited"
        print(
            f"\n[BATCH] Offset={offset} | Records fetched={finished_message} | Batch size={current_batch_size}"
        )

        params = {
            "$limit": current_batch_size,
            "$offset": offset,
            "$where": soql_date_filter,
            "$order": "created_date ASC",
        }

        try:
            # Reset retry count for this offset if request succeeds
            if offset not in offset_retry_count:
                offset_retry_count[offset] = 0

            request_start = time.time()

            response = session.get(
                api_url,
                params=params,
                timeout=(5, timeout),
            )

            response.raise_for_status()

            batch = response.json()

            request_time = time.time() - request_start

            print(f"[OK] Request completed in {request_time:.2f}s | Batch: {len(batch)} records")

            if not batch:
                done_message = f"{len(all_data)}/{total_limit}" if not unlimited else f"{len(all_data)}/unlimited"
                print(f"\n[DONE] NO MORE RECORDS - Total fetched: {done_message}")
                return all_data

            all_data.extend(batch)
            
            # Reset retry counter on success
            offset_retry_count[offset] = 0

            progress_message = f"{len(all_data)}/{total_limit}" if not unlimited else f"{len(all_data)}/unlimited"
            print(f"[PROGRESS] Total progress: {progress_message} records")

            # Only increment by actual batch received for safe pagination
            offset += len(batch)

            # Stop if we've reached the limit
            if not unlimited and len(all_data) >= total_limit:
                print(f"\n[SUCCESS] LIMIT REACHED: {len(all_data)}/{total_limit} records fetched")
                return all_data[:total_limit]  # Trim to exact limit

            # avoid hammering API
            time.sleep(0.2)


        except requests.exceptions.RequestException as e:

            status_code = getattr(
                e.response,
                "status_code",
                None
            )

            is_retryable = isinstance(
                e,
                (
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ProxyError,
                    requests.exceptions.SSLError,
                ),
            )

            if status_code in RETRYABLE_STATUS_CODES:
                is_retryable = True

            if not is_retryable:
                raise Exception(
                    f"[ERROR] Non-retryable API Error: {e}"
                ) from e

            # Increment retry counter for this offset
            offset_retry_count[offset] = offset_retry_count.get(offset, 0) + 1
            retry_count = offset_retry_count[offset]

            # Circuit breaker: stop retrying if too many failures on same offset
            if retry_count > MAX_RETRIES_PER_OFFSET:
                print(
                    f"[ERROR] Max retries ({MAX_RETRIES_PER_OFFSET}) exceeded for offset={offset}. "
                    f"Fetched {len(all_data)} records so far."
                )
                return all_data

            backoff_wait = backoff_factor ** retry_count
            print(
                f"[RETRY] Retriable error (status={status_code}, retry {retry_count}/{MAX_RETRIES_PER_OFFSET}) - "
                f"waiting {backoff_wait:.1f}s before retry..."
            )

            time.sleep(backoff_wait)
            # Continue loop without incrementing offset - retry same batch

    final_message = f"{len(all_data)}/{total_limit}" if not unlimited else f"{len(all_data)}/unlimited"
    print(
        f"\n[FINAL] FINAL INGESTED RECORDS: {final_message}"
    )

    return all_data if unlimited else all_data[:total_limit]
