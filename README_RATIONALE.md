# README Rationale and Decision Traceability

This document explains why each statement in README.md exists, what decision it reflects, and where it is implemented in the codebase.

## How to Read This Document

- Statement: Exact or near-exact statement from README.md
- Why this statement exists: The design intent, communication intent, or operational reason
- Implementation evidence: Code location that supports the statement

## Title and Opening Description

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| 311 Complaint Resolution Product | Names the domain and outcome focus clearly for technical and non-technical readers. | main.py, services/pipeline_services.py |
| An agent-driven analytics platform for NYC 311 service requests, built with FastAPI and PySpark. | Identifies architecture style and anchor technologies to set expectations on runtime model and scaling behavior. | api/routes.py, api/controllers.py, processing/spark_session.py |
| The system ingests complaint data, applies transformation and feature engineering, computes operational performance metrics, and produces insights plus action-oriented recommendations. | Summarizes the full value chain in one sentence so readers immediately understand inputs, processing, outputs, and decision support role. | ingestion/api_client.py, processing/transformations.py, processing/feature_engineering.py, agents/analytics_agent.py, agents/insights_agent.py, agents/recommendation_agent.py |

## Executive Summary

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| This project provides an end-to-end operational analytics pipeline that helps teams answer questions such as: | Signals that this is not just data collection, but decision support from ingestion to recommendation. | services/pipeline_services.py, orchestrator/workflow.py |
| Are service requests being resolved within expected timelines? | Frames SLA and resolution-time analytics as a core business question. | agents/analytics_agent.py |
| Which complaint categories and boroughs have the highest operational impact? | Highlights segmentation by complaint_type and borough as prioritization mechanism. | agents/analytics_agent.py, agents/insights_agent.py |
| Is backlog growth threatening service quality? | Establishes backlog as service risk indicator. | agents/analytics_agent.py |
| What concrete actions should operations teams take next? | Confirms output is prescriptive, not only descriptive. | agents/recommendation_agent.py |
| The platform exposes a lightweight API endpoint that triggers the full pipeline and returns structured outputs for validation, analytics, insights, recommendations, and timing telemetry. | Communicates API-first integration pattern and response contract shape for clients. | api/routes.py, api/controllers.py, services/pipeline_services.py |

## Core Capabilities

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| API-triggered analytics workflow using FastAPI | Indicates minimal operational friction for triggering runs from apps, scripts, or dashboards. | main.py, api/routes.py |
| Resilient ingestion from NYC Open Data (Socrata) with retry and pagination controls | Communicates reliability and correctness protections against network/API volatility. | ingestion/api_client.py |
| Spark-based transformation and feature engineering pipeline | Clarifies distributed dataframe processing model used for cleansing and derived metrics. | processing/spark_session.py, processing/transformations.py, processing/feature_engineering.py |
| Agentic analysis chain | Explains modular decision pipeline design with role-separated components. | orchestrator/workflow.py |
| Validation agent | Ensures data quality and null coverage before interpretation layers. | agents/validation_agents.py |
| Analytics agent | Computes KPI and diagnostic aggregates needed for downstream reasoning. | agents/analytics_agent.py |
| Insight agent | Converts numeric KPI signals to concise narratives and priorities. | agents/insights_agent.py |
| Recommendation agent | Converts insights into operational actions. | agents/recommendation_agent.py |
| Snapshot persistence for both raw and processed datasets | Supports auditability, reproducibility, and rollback/debug workflows. | ingestion/data_saver.py, processing/processed_data_saver.py |
| Timing and stage-level observability for pipeline diagnostics | Enables bottleneck detection and runtime health tracking. | services/pipeline_services.py, orchestrator/workflow.py |
| Timeout-protected Spark actions for improved runtime stability | Reduces risk of long hangs on expensive Spark actions. | processing/spark_operations.py |

## Architecture Diagram

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Client Request -> FastAPI Route /run-analysis | Entry boundary and trigger mechanism. | api/routes.py |
| FastAPI Route -> Pipeline Service | Route delegates business logic to service layer for clean separation. | api/controllers.py, services/pipeline_services.py |
| Pipeline Service -> Fetch 311 Data | First stage obtains source records. | ingestion/api_client.py |
| Fetch 311 Data -> Save Raw Snapshot | Captures immutable raw input for tracing. | ingestion/data_saver.py |
| Save Raw Snapshot -> Create Spark Session | Spark starts only after data is available to process. | services/pipeline_services.py, processing/spark_session.py |
| Create Spark Session -> Load DataFrame | Converts stored JSON to Spark DataFrame. | ingestion/data_loader.py |
| Load DataFrame -> Schema Validation | Early guardrail before expensive analytics. | ingestion/schema_validation.py |
| Schema Validation -> Cleaning + Feature Engineering | Data prep follows validity confirmation. | processing/transformations.py, processing/feature_engineering.py |
| Cleaning + Feature Engineering -> Save Processed Snapshot | Persists transformed state for reuse and audits. | processing/processed_data_saver.py |
| Save Processed Snapshot -> Workflow Orchestrator | Agent chain runs on engineered dataset. | orchestrator/workflow.py |
| Workflow Orchestrator -> Validation/Analytics/Insight/Recommendation Agents | Encodes staged analytical reasoning and action generation. | orchestrator/workflow.py, agents/*.py |
| Recommendation Agent -> Structured API Response | Final output exposed to caller in machine-consumable form. | api/controllers.py, services/pipeline_services.py |

## Project Structure Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| main.py | Shows canonical executable API entrypoint. | main.py |
| requirements.txt | Documents dependency lock point for reproducible setup. | requirements.txt |
| api/routes.py | Shows HTTP endpoint definitions. | api/routes.py |
| api/controllers.py | Shows request-to-service adaptation layer. | api/controllers.py |
| core/config.py | Centralized runtime configuration surface. | core/config.py |
| core/logger.py | Global logging setup for observability. | core/logger.py |
| ingestion/api_client.py | Source-system acquisition logic. | ingestion/api_client.py |
| ingestion/data_loader.py | JSON to Spark DataFrame ingestion. | ingestion/data_loader.py |
| ingestion/data_saver.py | Raw snapshot persistence. | ingestion/data_saver.py |
| ingestion/schema_validation.py | Required-schema and critical-null validation. | ingestion/schema_validation.py |
| processing/spark_session.py | Spark runtime creation and platform tuning. | processing/spark_session.py |
| processing/spark_operations.py | Timeout safety wrappers for Spark actions. | processing/spark_operations.py |
| processing/transformations.py | Base cleaning operations. | processing/transformations.py |
| processing/feature_engineering.py | Derived time and resolution features. | processing/feature_engineering.py |
| processing/processed_data_saver.py | Processed snapshot persistence. | processing/processed_data_saver.py |
| agents/base_agent.py | Shared interface/contract for agents. | agents/base_agent.py |
| agents/validation_agents.py | Data quality profile agent. | agents/validation_agents.py |
| agents/analytics_agent.py | KPI and segmentation computation agent. | agents/analytics_agent.py |
| agents/insights_agent.py | Rule-based interpretation agent. | agents/insights_agent.py |
| agents/recommendation_agent.py | Action recommendation agent. | agents/recommendation_agent.py |
| orchestrator/workflow.py | Sequencing and timing of agent chain. | orchestrator/workflow.py |
| services/pipeline_services.py | Full end-to-end pipeline coordinator. | services/pipeline_services.py |
| tests/test_processed_data_saver.py | Example unit test around output artifact correctness. | tests/test_processed_data_saver.py |
| data/raw and data/processed | Explicit storage boundaries for lineage. | ingestion/data_saver.py, processing/processed_data_saver.py |

## Technology Stack Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Python | Language baseline for API, orchestration, and Spark integration. | All source files |
| FastAPI | High-productivity typed API framework for endpoint exposure. | main.py, api/routes.py |
| Uvicorn | ASGI server needed to run FastAPI app. | main.py, requirements.txt |
| PySpark | DataFrame engine for scalable transforms and aggregations. | processing/spark_session.py, agents/analytics_agent.py |
| Requests | Robust HTTP client for Socrata API pulls. | ingestion/api_client.py |
| Pydantic | Included for model/validation support in API ecosystem and future expansion. | requirements.txt |
| python-dotenv | Environment variable loading support for configuration workflows. | requirements.txt |

## Data and Analytics Workflow Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Ingestion | Separates acquisition concerns from transformations and analytics. | ingestion/api_client.py |
| Pulls records from NYC 311 API in controlled batches. | Prevents oversized payloads and supports predictable throughput. | ingestion/api_client.py |
| Applies date-window logic and robust retry behavior for transient errors. | Ensures temporal targeting and network resilience. | ingestion/api_client.py |
| Persistence (raw) | Explicitly marks immutable input checkpoint for lineage. | ingestion/data_saver.py |
| Saves raw API payload to timestamped JSON snapshots in data/raw. | Allows reruns and forensic comparisons. | ingestion/data_saver.py |
| Processing | Distinguishes compute phase from acquisition and reporting. | processing/*.py |
| Creates a local Spark session tuned for stability. | Platform-specific Spark reliability hardening. | processing/spark_session.py |
| Loads JSON into a Spark DataFrame. | Standardized tabular abstraction for downstream analytics. | ingestion/data_loader.py |
| Validates required schema and critical data quality constraints. | Fails fast on malformed/low-signal data. | ingestion/schema_validation.py |
| Cleans data (null filtering and deduplication). | Removes invalid duplicates and essential-null rows to improve metric quality. | processing/transformations.py |
| Adds timestamp and resolution metrics. | Creates core analytical features for SLA and delay evaluation. | processing/feature_engineering.py |
| Persistence (processed) | Captures transformed state for downstream consumption. | processing/processed_data_saver.py |
| Saves transformed records to a timestamped JSON file in data/processed. | Supports reproducibility and offline analysis. | processing/processed_data_saver.py |
| Agentic Orchestration | Signals multi-stage analytical interpretation architecture. | orchestrator/workflow.py |
| Validation Agent: row volume and null coverage checks. | Provides dataset health before interpretation. | agents/validation_agents.py |
| Analytics Agent: SLA, backlog, resolution distributions, SES, and segmented breakdowns. | Computes operational KPIs and priority segments. | agents/analytics_agent.py |
| Insight Agent: threshold-based narrative interpretation. | Converts KPI values into concise human-readable risk statements. | agents/insights_agent.py |
| Recommendation Agent: operational actions derived from insights. | Produces actionable outcomes for operators and managers. | agents/recommendation_agent.py |
| API Response | Clarifies interface output boundary. | api/controllers.py |
| Returns structured outputs and timing breakdowns for observability. | Makes results machine-consumable and traceable by stage duration. | services/pipeline_services.py, orchestrator/workflow.py |

## API Reference Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Health Check | Defines minimal readiness probe for tooling and monitoring. | api/routes.py |
| Method: GET | Read-only status query should be idempotent and cache-friendly. | api/routes.py |
| Path: / | Common root probe endpoint convention. | api/routes.py |
| Purpose: confirms the API is running | Supports quick operational verification. | api/routes.py |
| Run Analysis Pipeline | Names the main operational action endpoint. | api/routes.py |
| Method: POST | Triggering a processing workflow is an action with side effects. | api/routes.py |
| Path: /run-analysis | Semantic endpoint naming for clarity. | api/routes.py |
| Query params optional start_date and end_date | Allows bounded analysis windows without breaking defaults. | api/routes.py, services/pipeline_services.py, ingestion/api_client.py |
| PowerShell example request | Gives Windows users immediate runnable invocation. | README usability statement |
| Example response shape | Documents payload contract for client integrators. | api/controllers.py, services/pipeline_services.py, orchestrator/workflow.py |

## Setup and Installation Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Prerequisites | Prevents setup failures by making platform assumptions explicit. | Environment requirements implied by Spark + Python runtime |
| Windows environment tuning target | Spark and path handling include Windows-specific safeguards. | processing/spark_session.py, main.py |
| Python installed | Required runtime for application execution. | All source files |
| Java runtime available for Spark | Spark JVM dependency requirement. | processing/spark_session.py usage context |
| Optional local Hadoop binaries at C:\Hadoop | Improves Windows Spark compatibility based on startup environment settings. | main.py |
| pip install -r requirements.txt | Single-step reproducible dependency installation. | requirements.txt |

## Configuration Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Runtime configuration is defined in core/config.py via environment variables. | Centralized config reduces hardcoding and eases deployment portability. | core/config.py |
| DATA_PATH default data/raw | Ensures raw artifact destination exists by convention. | core/config.py, ingestion/data_saver.py |
| PROCESSED_PATH default data/processed | Separates transformed artifacts from raw lineage. | core/config.py, processing/processed_data_saver.py |
| LOG_LEVEL default INFO | Balances observability with signal-to-noise. | core/config.py, core/logger.py |
| API_URL default NYC 311 endpoint | Provides sensible default data source. | core/config.py |
| API_LIMIT default 25000 | Bounds API volume for predictable runtime and resource usage. | core/config.py, ingestion/api_client.py |
| API_BATCH_SIZE default 1000 | Reasonable pagination unit for throughput and stability. | core/config.py, ingestion/api_client.py |
| API_TIMEOUT default 240 | Protects against long stalls while tolerating large responses. | core/config.py, ingestion/api_client.py |
| API_START_DATE default last 30 days | Keeps default analysis timely and bounded. | core/config.py |
| API_END_DATE default today | Aligns window end with current state. | core/config.py |

## Running the Application Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Start the API server with python main.py | Provides canonical local startup command. | main.py |
| Service binds to localhost and auto-selects available port, preferring 8000 | Avoids startup failure when port 8000 is occupied. | main.py |

## Testing Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Run test suite with pytest | Standard test runner invocation for Python projects. | tests/test_processed_data_saver.py |
| test_env.py validates interpreter and subprocess behavior | Documents environment sanity check script purpose. | test_env.py |
| test_pipeline.py executes full pipeline run with diagnostics | Documents end-to-end smoke test utility. | test_pipeline.py |

## Observability and Runtime Diagnostics Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Stage-level timing telemetry is captured in pipeline execution output. | Enables performance benchmarking and stage bottleneck identification. | services/pipeline_services.py |
| Logs include step markers for ingestion, Spark startup, validation, transformation, save steps, and orchestration. | Improves operator readability and troubleshooting speed. | services/pipeline_services.py |
| Timeout-aware Spark wrappers reduce risk of indefinite blocking during collect and count operations. | Hardens long-running operations against hangs. | processing/spark_operations.py |

## Data Artifacts Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Raw snapshots: data/raw | States where source snapshots are stored. | ingestion/data_saver.py |
| Processed snapshots: data/processed | States where transformed outputs are stored. | processing/processed_data_saver.py |
| Each run creates timestamped JSON files to support reproducibility, backtracking, and auditability. | Explains operational rationale for timestamped artifact strategy. | ingestion/data_saver.py, processing/processed_data_saver.py |

## Reliability Features Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Retries and backoff for transient API/network failures | Reduces failure rate under temporary transport/server errors. | ingestion/api_client.py |
| Per-offset retry circuit breaker to avoid endless pagination loops | Prevents infinite retry loops on problematic offsets. | ingestion/api_client.py |
| Date-window normalization and fallback behavior | Guards against invalid windows and sparse/no-result periods. | ingestion/api_client.py |
| Defensive error handling at each pipeline stage | Returns actionable failure context and prevents silent collapse. | services/pipeline_services.py |
| Graceful continuation if processed snapshot save encounters non-fatal issues | Preserves analytical output even when persistence step has issues. | services/pipeline_services.py |

## Roadmap Suggestions Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Expand unit and integration test coverage across ingestion and agent outputs | Current tests are limited; broader coverage improves confidence. | tests/test_processed_data_saver.py scope |
| Add CI workflow definitions under workflows/ | Automates quality gates and regression checks. | workflows/ currently placeholder |
| Add API auth and request throttling for production deployment | Improves security and abuse protection for public/internal exposure. | Not yet implemented |
| Add persistent metric sink for long-term trend analysis | Supports historical observability and KPI trending. | Not yet implemented |

## Contributing Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| Create a feature branch. | Encourages isolated development and clean review flows. | Team workflow convention |
| Implement and test your changes. | Protects code quality and runtime stability. | pytest usage, test utilities |
| Keep code style and logging conventions consistent. | Improves maintainability and troubleshooting consistency. | core/logger.py pattern, existing module conventions |
| Submit a pull request with clear rationale and validation notes. | Supports effective review and change traceability. | Engineering process best practice |

## License Section

| Statement | Why this statement exists | Implementation evidence |
|---|---|---|
| No explicit license file is currently present. | Avoids legal ambiguity by stating current status. | Repository root inspection |
| Add a LICENSE file before external distribution. | Needed for clear usage rights and compliance posture. | Open-source/commercial distribution best practice |

## Additional Clarifications About Technology Choices

| Technology | Why it is used here | Tradeoff acknowledged |
|---|---|---|
| FastAPI | Quick API construction, good typing ergonomics, straightforward integration with Python services. | Async and pydantic model depth can add complexity as API surface grows. |
| Uvicorn | Lightweight ASGI runtime with simple local startup path. | Requires process manager strategy for production resilience. |
| PySpark | Handles larger-than-memory style tabular operations and rich aggregation semantics. | Operational overhead and JVM dependency are higher than pandas-only setups. |
| Requests | Mature, reliable HTTP client for API ingestion and retry adapter compatibility. | Not async by default; throughput is bounded by sync request cycle. |
| Pydantic | Future-facing schema validation and typed API models support. | Adds model maintenance overhead if heavily adopted. |
| python-dotenv | Simplifies local environment variable management. | Must be paired with secrets-management strategy in production. |

## Notes on Statement Precision

Some README statements are intentionally concise and directional rather than exhaustive. This rationale file expands those concise statements into implementation-level justifications without changing their original intent.