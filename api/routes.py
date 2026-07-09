from fastapi import APIRouter
from api.controllers import run_analysis

router = APIRouter()

@router.get("/")
def health_check():
    return {"status": "API is running"}

@router.post("/run-analysis")
def trigger_pipeline(start_date: str = None, end_date: str = None):
    return run_analysis(start_date=start_date, end_date=end_date)