"""
Analysis endpoints for PNS specification processing
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from typing import Dict, Any
import uuid
import logging

from app.models.job import (
    JobRequest, 
    JobResponse, 
    JobStatusResponse, 
    JobResultsResponse, 
    ErrorResponse,
    JobStatus
)
from app.core.redis_client import job_manager
from app.services.job_processor import JobProcessor

router = APIRouter()
logger = logging.getLogger(__name__)

# Dependency to get job processor
def get_job_processor() -> JobProcessor:
    return JobProcessor()

@router.post("/analyze", response_model=JobResponse)
async def create_analysis_job(
    request: JobRequest,
    background_tasks: BackgroundTasks,
    job_processor: JobProcessor = Depends(get_job_processor)
):
    """
    Create a new analysis job for the given MCAT ID
    
    This endpoint:
    1. Creates a unique job ID
    2. Stores job in Redis
    3. Starts background processing
    4. Returns job ID immediately
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Create job in Redis
        success = await job_manager.create_job(job_id, request.mcat_id)
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to create job in Redis"
            )
        
        # Start background processing
        background_tasks.add_task(
            job_processor.process_analysis_job,
            job_id,
            request.mcat_id
        )
        
        logger.info(f"Created analysis job {job_id} for MCAT ID {request.mcat_id}")
        
        return JobResponse(
            job_id=job_id,
            status=JobStatus.PROCESSING,
            mcat_id=request.mcat_id,
            estimated_time_minutes=3
        )
        
    except Exception as e:
        logger.error(f"Failed to create analysis job: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create analysis job: {str(e)}"
        )

@router.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the current status of an analysis job
    """
    try:
        job_data = await job_manager.get_job_status(job_id)
        
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found"
            )
        
        return JobStatusResponse(
            job_id=job_id,
            status=JobStatus(job_data["status"]),
            progress=int(job_data.get("progress", 0)),
            current_step=job_data.get("current_step"),
            created_at=job_data["created_at"],
            updated_at=job_data["updated_at"],
            error=job_data.get("error")
        )
        
    except ValueError as e:
        # Handle invalid job status enum
        logger.error(f"Invalid job status for {job_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Invalid job status: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status for {job_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get job status: {str(e)}"
        )

@router.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
async def get_job_results(job_id: str):
    """
    Get the results of a completed analysis job
    """
    try:
        # First check job status
        job_data = await job_manager.get_job_status(job_id)
        
        if not job_data:
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found"
            )
        
        job_status = job_data["status"]
        
        if job_status == "failed":
            raise HTTPException(
                status_code=400,
                detail=ErrorResponse(
                    error=job_data.get("error", "Job failed"),
                    job_id=job_id,
                    details={"status": job_status}
                ).dict()
            )
        
        if job_status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Job {job_id} is not completed yet. Current status: {job_status}"
            )
        
        # Get results from Redis
        results = await job_manager.get_job_results(job_id)
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"Results not found for job {job_id}"
            )
        
        return JobResultsResponse(**results)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job results for {job_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get job results: {str(e)}"
        )

@router.delete("/jobs/{job_id}")
async def cleanup_job(job_id: str):
    """
    Manually cleanup a job (for testing/admin purposes)
    """
    try:
        success = await job_manager.cleanup_job(job_id)
        
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found or already cleaned up"
            )
        
        return {"message": f"Job {job_id} cleaned up successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cleanup job {job_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cleanup job: {str(e)}"
        )
