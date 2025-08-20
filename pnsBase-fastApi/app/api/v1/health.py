"""
Health check endpoints
"""

from fastapi import APIRouter, HTTPException
from app.core.redis_client import redis_client
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test Redis connection
        await redis_client.ping()
        
        return {
            "status": "healthy",
            "service": "PNS Specification Analysis API",
            "version": "1.0.0",
            "redis": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "service": "PNS Specification Analysis API",
                "version": "1.0.0",
                "redis": "disconnected",
                "error": str(e)
            }
        )
