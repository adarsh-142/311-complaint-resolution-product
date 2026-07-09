import logging
import time
from core.config import settings
from ingestion.api_client import fetch_311_data
from ingestion.data_saver import save_raw_data
from processing.processed_data_saver import save_processed_data
from ingestion.data_loader import load_data
from processing.spark_session import get_spark_session
from ingestion.schema_validation import validate_schema
from processing.transformations import clean_data
from processing.feature_engineering import add_time_features, add_resolution_metrics
from orchestrator.workflow import WorkflowOrchestrator

logger = logging.getLogger(__name__)


def run_pipeline(start_date: str = None, end_date: str = None):

    timings = {}

    # Step 1: Fetch data
    logger.info("[START] STARTING PIPELINE EXECUTION...")
    logger.info(f"[FETCH] Fetching 311 data from API ({start_date or settings.API_START_DATE} to {end_date or settings.API_END_DATE})...")
    fetch_start = time.time()
    
    try:
        raw_data = fetch_311_data(
            api_url=settings.API_URL,
            total_limit=settings.API_LIMIT or None,
            batch_size=settings.API_BATCH_SIZE,
            timeout=settings.API_TIMEOUT,
            start_date=start_date,
            end_date=end_date
        )
        
        if not raw_data:
            logger.error("[ERROR] No data fetched from API")
            timings["fetch"] = time.time() - fetch_start
            return {"status": "error", "message": "No data fetched from API", "timings": timings}
        
        timings["fetch"] = time.time() - fetch_start
        logger.info(f"[OK] Successfully fetched {len(raw_data)} records from API in {timings['fetch']:.2f}s")
    except Exception as e:
        timings["fetch"] = time.time() - fetch_start
        logger.error(f"[ERROR] API fetch failed: {str(e)}")
        return {"status": "error", "message": f"API fetch failed: {str(e)}", "timings": timings}

    # Step 2: Save raw snapshot
    logger.info("[SAVE] Saving raw data snapshot...")
    save_raw_start = time.time()
    try:
        file_path = save_raw_data(raw_data)
        timings["save_raw"] = time.time() - save_raw_start
        logger.info(f"[OK] Raw data saved to {file_path} in {timings['save_raw']:.2f}s")
    except Exception as e:
        timings["save_raw"] = time.time() - save_raw_start
        logger.error(f"[ERROR] Failed to save raw data: {str(e)}")
        return {"status": "error", "message": f"Failed to save raw data: {str(e)}", "timings": timings}

    # Step 3: Spark session
    logger.info("[INIT] Initializing Spark session...")
    spark_start = time.time()
    try:
        spark = get_spark_session()
        timings["spark_startup"] = time.time() - spark_start
        logger.info(f"[OK] Spark session initialized in {timings['spark_startup']:.2f}s")
    except Exception as e:
        timings["spark_startup"] = time.time() - spark_start
        logger.error(f"[ERROR] Spark initialization failed: {str(e)}")
        return {"status": "error", "message": f"Spark initialization failed: {str(e)}", "timings": timings}

    # Step 4: Load into Spark
    logger.info("[LOAD] Loading data into Spark DataFrame...")
    load_start = time.time()
    try:
        df = load_data(spark, file_path)
        timings["load"] = time.time() - load_start
        logger.info(f"[OK] Data loaded into Spark in {timings['load']:.2f}s")
    except Exception as e:
        timings["load"] = time.time() - load_start
        logger.error(f"[ERROR] Failed to load data into Spark: {str(e)}")
        return {"status": "error", "message": f"Failed to load data: {str(e)}", "timings": timings}

    # Step 5: Validate
    logger.info("[VALIDATE] Validating data schema...")
    validate_start = time.time()
    try:
        validation = validate_schema(df)
        timings["validate"] = time.time() - validate_start
        if not validation["valid"]:
            logger.error(f"[ERROR] Schema validation failed: {validation}")
            return {"status": "error", "message": "Schema validation failed", "details": validation, "timings": timings}
        logger.info(f"[OK] Schema validation passed in {timings['validate']:.2f}s")
    except Exception as e:
        timings["validate"] = time.time() - validate_start
        logger.error(f"[ERROR] Validation failed: {str(e)}")
        return {"status": "error", "message": f"Validation failed: {str(e)}", "timings": timings}

    # Step 6: Transform
    logger.info("[CLEAN] Cleaning data...")
    clean_start = time.time()
    try:
        df = clean_data(df)
        timings["cleaning"] = time.time() - clean_start
        logger.info(f"[OK] Data cleaning completed in {timings['cleaning']:.2f}s")
    except Exception as e:
        timings["cleaning"] = time.time() - clean_start
        logger.error(f"[ERROR] Data cleaning failed: {str(e)}")
        return {"status": "error", "message": f"Data cleaning failed: {str(e)}", "timings": timings}
    
    logger.info("[FEATURES] Applying feature engineering...")
    features_start = time.time()
    try:
        df = add_time_features(df)
        df = add_resolution_metrics(df)
        timings["feature_engineering"] = time.time() - features_start
        logger.info(f"[OK] Feature engineering completed in {timings['feature_engineering']:.2f}s")
    except Exception as e:
        timings["feature_engineering"] = time.time() - features_start
        logger.error(f"[ERROR] Feature engineering failed: {str(e)}")
        return {"status": "error", "message": f"Feature engineering failed: {str(e)}", "timings": timings}

    # Persist and materialize the cleaned DataFrame to prevent repeated Spark recomputation
    cache_start = time.time()
    try:
        df = df.cache()
        cached_rows = df.count()
        timings["cache_materialization"] = time.time() - cache_start
        logger.info(f"[OK] Data cached with {cached_rows} rows in {timings['cache_materialization']:.2f}s")
    except Exception as e:
        timings["cache_materialization"] = time.time() - cache_start
        logger.warning(f"[WARNING] Data caching failed: {str(e)}")

    # Preview data
    logger.info("[PREVIEW] Data preview:")
    try:
        df.show(5)
    except Exception as e:
        logger.warning(f"Could not show data preview: {str(e)}")
    
    # Step 7: Save processed data
    logger.info("[SAVE] Saving processed data snapshot...")
    save_processed_start = time.time()
    try:
        save_processed_data(df)
        timings["save_processed"] = time.time() - save_processed_start
        logger.info(f"[OK] Processed data saved successfully in {timings['save_processed']:.2f}s")
    except Exception as e:
        timings["save_processed"] = time.time() - save_processed_start
        logger.warning(f"[WARNING] Processed data save encountered issue: {str(e)}. Continuing pipeline... ({timings['save_processed']:.2f}s)")

    # Step 8: Orchestrator
    logger.info("[ORCHESTRATE] Running workflow orchestrator (analytics, insights, recommendations)...")
    orchestrator_start = time.time()
    try:
        orchestrator = WorkflowOrchestrator()
        results = orchestrator.run(df)
        timings["workflow"] = time.time() - orchestrator_start
        logger.info(f"[OK] Orchestrator pipeline completed in {timings['workflow']:.2f}s")
    except Exception as e:
        timings["workflow"] = time.time() - orchestrator_start
        logger.error(f"[ERROR] Orchestrator failed: {str(e)}")
        return {"status": "error", "message": f"Orchestrator failed: {str(e)}", "timings": timings}

    logger.info("[COMPLETE] PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    logger.info(f"[TIMINGS] Pipeline timings: {timings}")
    if "timings" in results and isinstance(results["timings"], dict):
        logger.info(f"[TIMINGS] Orchestrator breakdown: {results['timings']}")
    return {"status": "success", "message": "Pipeline executed successfully", "results": results, "timings": timings}