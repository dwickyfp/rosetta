"""
Rosetta ETL Platform - FastAPI Application.

A production-ready FastAPI application for managing ETL pipeline configurations
with PostgreSQL WAL monitoring capabilities.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app import __version__
from app.api.v1 import api_router
from app.core.config import get_settings
from app.core.database import check_database_health, db_manager
from app.core.exceptions import RosettaException
from app.core.logging import get_logger, setup_logging
from app.infrastructure.tasks.scheduler import BackgroundScheduler

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Get settings
settings = get_settings()

# Initialize background scheduler
background_scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown events for the application.
    """
    # Startup
    logger.info(
        "Starting Rosetta ETL Platform",
        extra={"version": __version__, "environment": settings.app_env},
    )

    try:
        # Initialize database (sync operation)
        db_manager.initialize()
        logger.info("Database initialized successfully")

        # Start background scheduler
        background_scheduler.start()
        logger.info("Background scheduler started successfully")

        # Check database health
        db_healthy = check_database_health()
        if not db_healthy:
            logger.warning("Database health check failed during startup")

        logger.info("Application startup completed successfully")

    except Exception as e:
        logger.error("Failed to start application", extra={"error": str(e)})
        raise

    yield

    # Shutdown
    logger.info("Shutting down Rosetta ETL Platform")

    try:
        # Stop background scheduler
        background_scheduler.stop()
        logger.info("Background scheduler stopped")

        # Close database connections
        db_manager.close()
        logger.info("Database connections closed")

        logger.info("Application shutdown completed successfully")

    except Exception as e:
        logger.error("Error during application shutdown", extra={"error": str(e)})


# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    version=__version__,
    description=(
        "A production-ready FastAPI application for managing ETL pipeline "
        "configurations with PostgreSQL WAL monitoring capabilities."
    ),
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    openapi_url="/openapi.json" if settings.debug else None,
    lifespan=lifespan,
)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handlers
@app.exception_handler(RosettaException)
async def rosetta_exception_handler(
    request: Request, exc: RosettaException
) -> JSONResponse:
    """
    Handle custom Rosetta exceptions.

    Returns standardized error response with appropriate HTTP status code.
    """
    logger.warning(
        f"Application exception: {exc.__class__.__name__}",
        extra={
            "error": exc.message,
            "path": request.url.path,
            "status_code": exc.status_code,
        },
    )

    return JSONResponse(status_code=exc.status_code, content=exc.to_dict())


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """
    Handle Pydantic validation errors.

    Returns detailed validation error information.
    """
    logger.warning(
        "Validation error", extra={"errors": exc.errors(), "path": request.url.path}
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "Request validation failed",
            "details": exc.errors(),
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle unexpected exceptions.

    Returns generic error response and logs exception details.
    """
    logger.error(
        "Unexpected exception",
        extra={"error": str(exc), "type": type(exc).__name__, "path": request.url.path},
        exc_info=True,
    )

    # Don't expose internal errors in production
    error_message = str(exc) if settings.debug else "Internal server error"

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": error_message,
            "details": {},
        },
    )


# Include API routers
app.include_router(api_router, prefix=settings.api_v1_prefix)


# Root endpoint
@app.get("/", tags=["root"], summary="Root endpoint", description="Get API information")
async def root():
    """
    Root endpoint.

    Returns basic API information.
    """
    return {
        "name": settings.app_name,
        "version": __version__,
        "environment": settings.app_env,
        "docs_url": f"{settings.api_v1_prefix}/docs" if settings.debug else None,
    }


# Health check endpoint (outside versioned API for monitoring)
@app.get(
    "/health",
    tags=["health"],
    summary="Health check",
    description="Check application health",
)
async def health_check():
    """
    Health check endpoint.

    Used by load balancers and monitoring systems.
    """
    db_healthy = await check_database_health()
    overall_status = "healthy" if db_healthy else "unhealthy"

    return {
        "status": overall_status,
        "version": __version__,
        "checks": {"database": db_healthy},
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
