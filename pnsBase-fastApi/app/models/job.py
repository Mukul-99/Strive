"""
Pydantic models for job management
"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

class JobStatus(str, Enum):
    """Job status enumeration"""
    PROCESSING = "processing"
    PNS_FETCHING = "pns_fetching"
    CSV_FETCHING = "csv_fetching"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    FAILED = "failed"

class JobRequest(BaseModel):
    """Request model for creating a new analysis job"""
    mcat_id: str = Field(..., description="MCAT ID for analysis", example="6472")
    
    class Config:
        json_schema_extra = {
            "example": {
                "mcat_id": "6472"
            }
        }

class JobResponse(BaseModel):
    """Response model for job creation"""
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Current job status")
    mcat_id: str = Field(..., description="MCAT ID being processed")
    estimated_time_minutes: int = Field(default=3, description="Estimated processing time in minutes")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "processing",
                "mcat_id": "6472",
                "estimated_time_minutes": 3
            }
        }

class JobStatusResponse(BaseModel):
    """Response model for job status check"""
    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="Current job status")
    progress: int = Field(default=0, description="Progress percentage (0-100)")
    current_step: Optional[str] = Field(None, description="Current processing step")
    created_at: datetime = Field(..., description="Job creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    error: Optional[str] = Field(None, description="Error message if job failed")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "analyzing",
                "progress": 65,
                "current_step": "Processing triangulation",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:32:30Z",
                "error": None
            }
        }

class SpecificationResult(BaseModel):
    """Individual specification result"""
    rank: int = Field(..., description="Specification rank")
    specification: str = Field(..., description="Specification name")
    options: str = Field(..., description="Available options")
    frequency: str = Field(..., description="Frequency information")
    status: str = Field(..., description="Status information")
    priority: str = Field(..., description="Priority level")

class IndividualResults(BaseModel):
    """Individual dataset analysis results"""
    search_keywords: List[SpecificationResult] = Field(default=[], description="Search keywords results")
    lms_chats: List[SpecificationResult] = Field(default=[], description="LMS chat results")
    rejection_comments: List[SpecificationResult] = Field(default=[], description="BLNI rejection comments results")
    custom_spec: List[SpecificationResult] = Field(default=[], description="Custom spec (WhatsApp) results")
    pns_individual: List[SpecificationResult] = Field(default=[], description="PNS individual results")

class TriangulationResult(BaseModel):
    """Final triangulation result"""
    rank: int = Field(..., description="Specification rank")
    score: int = Field(..., description="Cross-source validation score")
    pns: str = Field(..., description="PNS specification")
    options: str = Field(..., description="Combined options from all sources")
    search_keywords: str = Field(..., description="Present in search keywords")
    whatsapp_specs: str = Field(..., description="Present in WhatsApp data")
    rejection_comments: str = Field(..., description="Present in rejection comments")
    lms_chats: str = Field(..., description="Present in LMS chats")

class JobResultsResponse(BaseModel):
    """Response model for completed job results"""
    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="Job status (should be completed)")
    mcat_id: str = Field(..., description="MCAT ID that was processed")
    individual_results: IndividualResults = Field(..., description="Individual dataset results")
    final_validation: List[TriangulationResult] = Field(..., description="Final PNS validation results")
    processing_summary: Dict[str, Any] = Field(default={}, description="Processing summary and metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "completed",
                "mcat_id": "6472",
                "individual_results": {
                    "pns_individual": [
                        {
                            "rank": 1,
                            "specification": "Motor Power",
                            "options": "100 kg/hr / 200 kg/hr",
                            "frequency": "40 / 37 (Total: 77)",
                            "status": "✅ Dominant / 🔶 Emerging",
                            "priority": "Primary"
                        }
                    ]
                },
                "final_validation": [
                    {
                        "rank": 1,
                        "score": 4,
                        "pns": "Motor Power",
                        "options": "100 kg/hr, 200 kg/hr, 50 kg/hr",
                        "search_keywords": "Yes",
                        "whatsapp_specs": "Yes",
                        "rejection_comments": "Yes",
                        "lms_chats": "Yes"
                    }
                ]
            }
        }

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str = Field(..., description="Error message")
    job_id: Optional[str] = Field(None, description="Job ID if applicable")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "No PNS data available for MCAT ID: 6472",
                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                "details": {
                    "mcat_id": "6472",
                    "step": "pns_fetching"
                }
            }
        }
