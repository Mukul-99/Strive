"""
FastAPI application for PNS-Based Specification Analysis
Main entry point for the API server
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.core.config import settings
from app.core.redis_client import redis_client
from app.api.v1 import analyze, health

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    # Startup
    logger.info("Starting PNS FastAPI application...")
    
    # Test Redis connection
    try:
        await redis_client.ping()
        logger.info("✅ Redis connection successful")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down PNS FastAPI application...")
    await redis_client.close()

# Create FastAPI app
app = FastAPI(
    title="PNS Specification Analysis API",
    description="Background job-based API for analyzing buyer specifications using PNS data and CSV sources",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware - configured for development, update for production
allowed_origins = [
    "http://localhost:3000",  # React dev server
    "http://localhost:8080",  # Alternative frontend
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8080",
]

# Add production origins from environment if available
import os
if production_origins := os.getenv("ALLOWED_ORIGINS"):
    allowed_origins.extend(production_origins.split(","))

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],  # Only allow necessary methods
    allow_headers=["Content-Type", "Authorization"],  # Only allow necessary headers
)

# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(analyze.router, prefix="/api/v1", tags=["analysis"])

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "PNS Specification Analysis API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
