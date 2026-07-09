REQUIRED_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "complaint_type",
    "borough"
]

def validate_schema(df):
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    
    if missing_cols:
        return {
            "valid": False,
            "missing_columns": missing_cols,
            "existing_columns": df.columns
        }
    
    # Validate that critical date columns are not all null
    try:
        total_rows = df.count()
        null_created = df.filter(df["created_date"].isNull()).count()
        null_closed = df.filter(df["closed_date"].isNull()).count()
        
        if null_created == total_rows:
            return {
                "valid": False,
                "message": "All records have null created_date"
            }
        
        if null_closed == total_rows:
            return {
                "valid": False,
                "message": "All records have null closed_date"
            }
    except Exception as e:
        return {
            "valid": False,
            "message": f"Schema validation error: {str(e)}"
        }
    
    return {"valid": True}