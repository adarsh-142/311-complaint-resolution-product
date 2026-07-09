# Code Coherence Review - 311 Complaint Resolution Product

## ✅ ISSUES FOUND - 5 Critical/High Priority Problems

---

### **ISSUE #1: MISSING `sla_breach` COLUMN (CRITICAL)**

**File**: `agents/analytics_agent.py` (Line 14)  
**Problem**: Code tries to filter on `sla_breach` column that doesn't exist:
```python
sla_compliant = df.filter(col("sla_breach") == 0).count()
```

**Root Cause**: 
- `add_resolution_metrics()` in `processing/feature_engineering.py` does NOT create the `sla_breach` column
- Only creates: `resolution_time_days` and `resolution_completed`

**Impact**: 
- **RUNTIME ERROR** - Will crash with: `AnalysisException: Column 'sla_breach' does not exist`
- Pipeline fails at Step 8 (Orchestrator)

**Fix Required**: Add this to `processing/feature_engineering.py`:
```python
df = df.withColumn(
    "sla_breach",
    when(
        (col("closed_ts").isNotNull()) & (col("closed_ts") > col("due_ts")),
        1
    ).otherwise(0)
)
```

---

### **ISSUE #2: JSON SERIALIZATION FAILURE (HIGH)**

**Files**: 
- `agents/analytics_agent.py` (Line 44-49)
- `api/controllers.py`

**Problem**: Spark Row objects cannot be JSON serialized:
```python
"borough_stats": [
    {
        "borough": row["borough"],                    # Spark type
        "avg_resolution_time": row["avg_resolution_time"],  # decimal.Decimal
        "total_requests": row["total_requests"]      # long64
    }
    for row in by_borough.collect()
]
```

**Impact**:
- FastAPI will fail when returning response
- Error: `TypeError: Object of type Decimal is not JSON serializable`

**Fix Required**: Convert Spark types to Python natives:
```python
"borough_stats": [
    {
        "borough": str(row["borough"]),
        "avg_resolution_time": float(row["avg_resolution_time"]) if row["avg_resolution_time"] else 0.0,
        "total_requests": int(row["total_requests"])
    }
    for row in by_borough.collect()
]
```

---

### **ISSUE #3: INCONSISTENT DATA TYPES IN VALIDATION AGENT (MEDIUM)**

**File**: `agents/validation_agents.py` (Line 16)  
**Problem**: Returns Spark `long64` type instead of Python `int`:
```python
"null_counts": {
    col: df.filter(df[col].isNull()).count()  # Returns long64
    for col in ["agency", "complaint_type", "borough"]
}
```

**Impact**:
- JSON serialization may fail
- Inconsistent with other agents

**Fix Required**:
```python
"null_counts": {
    col: int(df.filter(df[col].isNull()).count())
    for col in ["agency", "complaint_type", "borough"]
}
```

---

### **ISSUE #4: MISSING ERROR HANDLING IN SCHEMA VALIDATION (MEDIUM)**

**File**: `ingestion/schema_validation.py`  
**Problem**: Validates column existence but NOT data integrity:
```python
def validate_schema(df):
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    
    if missing_cols:
        return {"valid": False, "missing_columns": missing_cols}
    
    return {"valid": True}
    # ❌ Doesn't check if columns have all NULL values!
```

**Issue**: 
- If `created_date` or `closed_date` are ALL NULL, validation passes but pipeline fails later
- `clean_data()` filters out all rows if `created_date` is null everywhere

**Fix Required**: Add data quality checks:
```python
def validate_schema(df):
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    
    if missing_cols:
        return {"valid": False, "missing_columns": missing_cols}
    
    # Check if critical columns are all null
    total_rows = df.count()
    null_created = df.filter(df["created_date"].isNull()).count()
    
    if null_created == total_rows:
        return {"valid": False, "message": "All created_date values are NULL"}
    
    return {"valid": True}
```

---

### **ISSUE #5: INACTIVE/SKIPPED DATA SAVING STEP (MEDIUM)**

**File**: `services/pipeline_services.py` (Line 62-63)  
**Problem**: Step 7 is skipped without executing save:
```python
# Step 7: Save processed data
logger.info("Skipping processed data save on Windows environment...")
# ❌ Never calls save_processed_data(df)
```

**Impact**:
- Processed data is never saved
- Data history is lost after pipeline completes
- Takes up `processed_data_saver.py` module that's never used

**Fix Options**:
1. Either remove the skip and actually save data
2. Or remove the function and skip import

**Current Code** (if you want to save):
```python
logger.info("Saving processed data...")
try:
    save_processed_data(df)
except Exception as e:
    logger.warning(f"Data save failed: {str(e)}")
```

---

## ⚠️ MEDIUM PRIORITY ISSUES

### **ISSUE #6: DUPLICATE RECOMMENDATIONS (LOGIC ERROR)**

**File**: `agents/recommendation_agent.py`  
**Problem**: Multiple `if` statements allow duplicate recommendations:
```python
for insight in insights_output["insights"]:
    if "Low SLA" in insight:
        recommendations.append("Increase workforce...")
    if "High backlog" in insight:  # Should be elif
        recommendations.append("Redistribute workload...")
    if "Resolution times" in insight:  # Should be elif
        recommendations.append("Investigate bottlenecks...")
```

**Impact**: Not critical, but same insight could produce multiple identical recommendations

**Fix**: Use `elif` instead of multiple `if`:
```python
for insight in insights_output["insights"]:
    if "Low SLA" in insight:
        recommendations.append("Increase workforce...")
    elif "High backlog" in insight:
        recommendations.append("Redistribute workload...")
    elif "Resolution times" in insight:
        recommendations.append("Investigate bottlenecks...")
```

---

## ✅ WHAT WORKS CORRECTLY

| Component | Status | Notes |
|-----------|--------|-------|
| FastAPI Routes | ✅ | Properly structured |
| API Client | ✅ | Fetches data with error handling |
| Data Loader | ✅ | Uses Spark correctly |
| Schema Validation | ⚠️ | Missing data quality checks |
| Transformations | ✅ | Spark operations correct |
| Feature Engineering | ❌ | Missing `sla_breach` column |
| Analytics Agent | ❌ | JSON serialization + missing column |
| Validation Agent | ⚠️ | Type conversion needed |
| Insight Agent | ✅ | Logic is correct |
| Recommendation Agent | ⚠️ | Could have duplicates |
| Orchestrator | ✅ | Chains agents correctly |

---

## SUMMARY TABLE

| Issue # | Severity | File | Problem | Impact |
|---------|----------|------|---------|--------|
| 1 | 🔴 CRITICAL | feature_engineering.py | Missing `sla_breach` column | Pipeline crashes |
| 2 | 🔴 HIGH | analytics_agent.py | Spark types not JSON serializable | API response fails |
| 3 | 🟠 MEDIUM | validation_agents.py | Type mismatch (long64 vs int) | Serialization error |
| 4 | 🟠 MEDIUM | schema_validation.py | No NULL data checks | Silent failures |
| 5 | 🟠 MEDIUM | pipeline_services.py | Data saving skipped | No data persistence |
| 6 | 🟡 LOW | recommendation_agent.py | Duplicate recommendations possible | Minor UX issue |

---

## EXECUTION FLOW WITH ISSUES

```
Request → API Routes → Controllers → Pipeline Services
    ↓
Fetch Data ✅ → Save Raw ✅ → Spark Session ✅ → Load ✅
    ↓
Validate Schema ⚠️ → Clean ✅ → Time Features ✅ 
    ↓
Resolution Metrics ❌ (missing sla_breach)
    ↓
Orchestrator runs:
  ├─ Validation Agent: ⚠️ (type issue)
  ├─ Analytics Agent: 🔴 (sla_breach error + JSON serialization)
  ├─ Insight Agent: ✅ 
  └─ Recommendation Agent: ⚠️ (duplicate issue)
    ↓
Response 🔴 FAILS at JSON serialization or analytics agent error
```

---

## RECOMMENDED FIX ORDER

1. **First**: Fix Issue #1 (add `sla_breach` column) - BLOCKING
2. **Second**: Fix Issue #2 (JSON serialization) - BLOCKING  
3. **Third**: Fix Issue #3 (type conversion)
4. **Fourth**: Fix Issue #4 (NULL checks)
5. **Fifth**: Fix Issue #6 (use elif)
6. **Optional**: Fix Issue #5 (enable data saving)
