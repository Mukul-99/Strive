"""
Configuration settings for the FastAPI application
"""

from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    """Application settings"""
    
    # API Settings
    api_title: str = "PNS Specification Analysis API"
    api_version: str = "1.0.0"
    debug: bool = False
    
    # Redis Settings - REPLACE WITH ACTUAL CREDENTIALS
    redis_host: str = "localhost"  # TODO: Replace with actual Redis host
    redis_port: int = 6379
    redis_password: Optional[str] = None  # TODO: Replace with actual Redis password
    redis_db: int = 0
    redis_url: Optional[str] = None
    
    # Job Settings
    job_cleanup_delay_minutes: int = 5  # Delete Redis keys after 5 minutes
    max_concurrent_jobs: int = 10  # Max background jobs running simultaneously
    
    # External API Settings
    pns_api_base_url: str = "https://extract-product-936671953004.asia-south1.run.app"
    pns_api_endpoint: str = "/process-mcat-from-gcs"
    pns_api_timeout: int = 60  # seconds
    
    # BigQuery Settings - REPLACE WITH ACTUAL CREDENTIALS
    bigquery_project_id: str = "your-project-id"  # TODO: Replace with actual project ID
    bigquery_dataset: str = "your_dataset"  # TODO: Replace with actual dataset
    bigquery_table: str = "your_table"  # TODO: Replace with actual table name
    bigquery_credentials_path: Optional[str] = None  # TODO: Add path to service account JSON
    
    # OpenAI Settings (from existing project)
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4.1-mini"
    openai_base_url: str = "https://imllm.intermesh.net/v1"
    
    # File Settings
    max_file_size_mb: int = 50
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra environment variables

    @property
    def redis_connection_url(self) -> str:
        """Get Redis connection URL"""
        if self.redis_url:
            return self.redis_url
        
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        else:
            return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

# Global settings instance
settings = Settings()
