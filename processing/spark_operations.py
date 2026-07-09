"""Timeout-protected Spark operations to prevent pipeline hangs."""

import logging
from functools import wraps
import threading

logger = logging.getLogger(__name__)

class TimeoutException(Exception):
    """Raised when a Spark operation exceeds timeout."""
    pass

def spark_operation_with_timeout(timeout_seconds=300):
    """Decorator to wrap Spark operations with timeout protection.
    
    Args:
        timeout_seconds: Maximum seconds to allow operation (default 300s = 5min)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = [None]
            exception = [None]
            
            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e
            
            thread = threading.Thread(target=target, daemon=False)
            thread.start()
            thread.join(timeout=timeout_seconds)
            
            if thread.is_alive():
                logger.error(f"Operation {func.__name__} timed out after {timeout_seconds}s")
                raise TimeoutException(
                    f"Spark operation '{func.__name__}' exceeded {timeout_seconds}s timeout"
                )
            
            if exception[0]:
                raise exception[0]
            
            return result[0]
        
        return wrapper
    return decorator


def safe_collect(df, timeout_seconds=300, description="collect"):
    """Safe DataFrame.collect() with timeout protection.
    
    Args:
        df: Spark DataFrame
        timeout_seconds: Max time to wait (default 300s)
        description: Operation description for logging
    
    Returns:
        List of Row objects
    """
    logger.info(f"Starting Spark {description} operation (timeout={timeout_seconds}s)...")
    
    result = [None]
    exception = [None]
    
    def target():
        try:
            result[0] = df.collect()
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        error_msg = f"DataFrame {description} operation timed out after {timeout_seconds}s"
        logger.error(error_msg)
        raise TimeoutException(error_msg)
    
    if exception[0]:
        logger.error(f"{description} failed: {exception[0]}")
        raise exception[0]
    
    logger.info(f"{description} completed - retrieved {len(result[0])} rows")
    return result[0]


def safe_count(df, timeout_seconds=60, description="count"):
    """Safe DataFrame.count() with timeout protection.
    
    Args:
        df: Spark DataFrame
        timeout_seconds: Max time to wait (default 60s)
        description: Operation description for logging
    
    Returns:
        Number of rows
    """
    logger.info(f"Starting Spark {description} operation (timeout={timeout_seconds}s)...")
    
    result = [None]
    exception = [None]
    
    def target():
        try:
            result[0] = df.count()
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        error_msg = f"DataFrame {description} operation timed out after {timeout_seconds}s"
        logger.error(error_msg)
        raise TimeoutException(error_msg)
    
    if exception[0]:
        logger.error(f"{description} failed: {exception[0]}")
        raise exception[0]
    
    logger.info(f"{description} completed - result: {result[0]}")
    return result[0]


def safe_distinct_collect(df, column, timeout_seconds=300):
    """Safe distinct().collect() with timeout protection.
    
    Args:
        df: Spark DataFrame
        column: Column to get distinct values from
        timeout_seconds: Max time to wait (default 300s)
    
    Returns:
        List of distinct values
    """
    logger.info(f"Getting distinct values for '{column}' (timeout={timeout_seconds}s)...")
    
    result = [None]
    exception = [None]
    
    def target():
        try:
            rows = df.select(column).distinct().collect()
            result[0] = [row[column] for row in rows]
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        error_msg = f"Distinct collect operation timed out after {timeout_seconds}s"
        logger.error(error_msg)
        raise TimeoutException(error_msg)
    
    if exception[0]:
        logger.error(f"Distinct collect failed: {exception[0]}")
        raise exception[0]
    
    logger.info(f"Retrieved {len(result[0])} distinct values")
    return result[0]


def safe_groupby_collect(df, groupby_cols, agg_dict=None, timeout_seconds=300):
    """Safe groupBy().agg().collect() with timeout protection.
    
    Args:
        df: Spark DataFrame
        groupby_cols: Column(s) to group by
        agg_dict: Dict of aggregations (if None, just group)
        timeout_seconds: Max time to wait (default 300s)
    
    Returns:
        List of Row objects from grouped data
    """
    logger.info(f"Starting groupBy operation (timeout={timeout_seconds}s)...")
    
    result = [None]
    exception = [None]
    
    def target():
        try:
            grouped_df = df.groupBy(groupby_cols)
            if agg_dict:
                grouped_df = grouped_df.agg(agg_dict)
            result[0] = grouped_df.collect()
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=target, daemon=False)
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        error_msg = f"GroupBy operation timed out after {timeout_seconds}s"
        logger.error(error_msg)
        raise TimeoutException(error_msg)
    
    if exception[0]:
        logger.error(f"GroupBy failed: {exception[0]}")
        raise exception[0]
    
    logger.info(f"GroupBy completed - {len(result[0])} groups")
    return result[0]
