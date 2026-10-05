import os
import socket

from fastapi import FastAPI

from api.exception_handlers import register_exception_handlers
from api.middleware import register_request_middleware
from api.routes import router
from core.config import settings
from core.logger import setup_logger

setup_logger()

app = FastAPI(
    title=settings.APP_NAME,
    description="AI-powered 311 Service Optimization System",
    version=settings.APP_VERSION,
    docs_url=settings.API_DOCS_URL,
    openapi_url=settings.API_OPENAPI_URL,
)

register_request_middleware(app)
app.include_router(router)
register_exception_handlers(app)


def get_available_port(default_port: int = 8000) -> int:
    requested_port = int(os.getenv("PORT", str(default_port)))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", requested_port))
            return requested_port
        except OSError:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as fallback:
                fallback.bind(("127.0.0.1", 0))
                return fallback.getsockname()[1]


if __name__ == "__main__":
    import uvicorn

    port = get_available_port()
    print(f"Starting API on http://127.0.0.1:{port}")
    uvicorn.run("main:app", host="127.0.0.1", port=port, reload=False)
