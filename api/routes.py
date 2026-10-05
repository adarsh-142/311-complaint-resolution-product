from fastapi import APIRouter, File, Request, Response, UploadFile, status

from api.controllers import health_status, run_analysis, run_analysis_file
from api.schemas import (
    ErrorResponse,
    HealthResponse,
    RunAnalysisRequest,
    RunAnalysisSuccessResponse,
)

router = APIRouter()


@router.get("/", response_model=HealthResponse, tags=["health"])
@router.get("/health", response_model=HealthResponse, tags=["health"])
def health_check():
    return health_status()


@router.post(
    "/run-analysis",
    response_model=RunAnalysisSuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        422: {"model": ErrorResponse, "description": "Request or schema validation error"},
        500: {"model": ErrorResponse, "description": "Pipeline failure"},
    },
    tags=["analysis"],
)
def trigger_pipeline(request: Request, response: Response, payload: RunAnalysisRequest):
    return run_analysis(request=request, response=response, payload=payload)


@router.post(
    "/run-analysis/file",
    response_model=RunAnalysisSuccessResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse, "description": "Missing filename"},
        413: {"model": ErrorResponse, "description": "File too large"},
        415: {"model": ErrorResponse, "description": "Unsupported file type"},
        422: {"model": ErrorResponse, "description": "Input schema validation error"},
        500: {"model": ErrorResponse, "description": "Pipeline failure"},
    },
    tags=["analysis"],
)
async def trigger_pipeline_file(request: Request, response: Response, file: UploadFile = File(...)):
    return await run_analysis_file(request=request, response=response, file=file)
