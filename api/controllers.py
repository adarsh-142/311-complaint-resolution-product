from services.pipeline_services import run_pipeline

def run_analysis(start_date: str = None, end_date: str = None):
    
    try:
        result = run_pipeline(start_date=start_date, end_date=end_date)
        return {
            "status": "success",
            "data": result
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }