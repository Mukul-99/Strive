"""
Redis client setup and utilities for job management
"""

import redis.asyncio as redis
import json
import asyncio
from typing import Optional, Any, Dict
from datetime import datetime, timedelta
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

class RedisJobManager:
    """Redis-based job management system"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    # Job Status Management
    async def create_job(self, job_id: str, mcat_id: str) -> bool:
        """Create a new job in Redis"""
        try:
            logger.info(f"Starting job creation for {job_id} with MCAT {mcat_id}")
            
            # Test Redis connection first
            try:
                await self.redis.ping()
                logger.info("✅ Redis connection test successful")
            except Exception as ping_error:
                logger.error(f"❌ Redis connection test failed: {ping_error}")
                return False
            
            job_data = {
                "job_id": job_id,
                "mcat_id": mcat_id,
                "status": "processing",
                "progress": 0,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Job data prepared: {job_data}")
            logger.info(f"Attempting to create Redis key: job:{job_id}")
            
            # Set job data - use individual hset calls for each field
            for field, value in job_data.items():
                await self.redis.hset(f"job:{job_id}", field, value)
            logger.info(f"Redis hset completed for all fields")
            
            # Set expiration (cleanup after processing + buffer time)
            expire_time = timedelta(minutes=settings.job_cleanup_delay_minutes + 10)
            expire_result = await self.redis.expire(f"job:{job_id}", expire_time)
            logger.info(f"Redis expire result: {expire_result}")
            
            # Verify job was created
            created_job = await self.redis.hgetall(f"job:{job_id}")
            logger.info(f"Verification - created job data: {created_job}")
            
            logger.info(f"✅ Successfully created job {job_id} for MCAT ID {mcat_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create job {job_id}: {e}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    async def update_job_status(self, job_id: str, status: str, progress: int = None, 
                              current_step: str = None, error: str = None) -> bool:
        """Update job status in Redis"""
        try:
            updates = {
                "status": status,
                "updated_at": datetime.utcnow().isoformat()
            }
            
            if progress is not None:
                updates["progress"] = progress
            if current_step:
                updates["current_step"] = current_step
            if error:
                updates["error"] = error
            
            # Use individual hset calls for compatibility
            for field, value in updates.items():
                await self.redis.hset(f"job:{job_id}", field, value)
            logger.info(f"Updated job {job_id}: status={status}, progress={progress}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")
            return False
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status from Redis"""
        try:
            job_data = await self.redis.hgetall(f"job:{job_id}")
            if not job_data:
                return None
            
            # Convert bytes to strings
            return {k.decode(): v.decode() for k, v in job_data.items()}
            
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None
    
    async def store_job_results(self, job_id: str, results: Dict[str, Any]) -> bool:
        """Store job results in Redis"""
        try:
            results_json = json.dumps(results)
            await self.redis.set(f"job:{job_id}:results", results_json)
            
            # Update job status to completed
            await self.update_job_status(job_id, "completed", progress=100)
            
            # Schedule cleanup after delay
            cleanup_delay = timedelta(minutes=settings.job_cleanup_delay_minutes)
            await self.redis.expire(f"job:{job_id}:results", cleanup_delay)
            
            logger.info(f"Stored results for job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store results for job {job_id}: {e}")
            return False
    
    async def get_job_results(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job results from Redis"""
        try:
            results_json = await self.redis.get(f"job:{job_id}:results")
            if not results_json:
                return None
            
            return json.loads(results_json)
            
        except Exception as e:
            logger.error(f"Failed to get results for job {job_id}: {e}")
            return None
    
    async def cleanup_job(self, job_id: str) -> bool:
        """Manually cleanup job data"""
        try:
            keys_to_delete = [
                f"job:{job_id}",
                f"job:{job_id}:results"
            ]
            
            deleted = await self.redis.delete(*keys_to_delete)
            logger.info(f"Cleaned up job {job_id}, deleted {deleted} keys")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup job {job_id}: {e}")
            return False

# Create Redis connection
redis_client = redis.from_url(
    settings.redis_connection_url,
    encoding="utf-8",
    decode_responses=False,  # We'll handle decoding manually
    max_connections=20
)

# Create job manager instance
job_manager = RedisJobManager(redis_client)
