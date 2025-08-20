"""
Background job processor for analysis workflow
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime

from app.core.redis_client import job_manager
from app.services.data_fetcher import DataFetcher
from app.services.workflow import run_spec_extraction_workflow
from app.models.job import JobStatus, SpecificationResult, IndividualResults, TriangulationResult

logger = logging.getLogger(__name__)

class JobProcessor:
    """Main job processor for background analysis tasks"""
    
    def __init__(self):
        self.data_fetcher = DataFetcher()
    
    async def process_analysis_job(self, job_id: str, mcat_id: str):
        """
        Main background job processing function
        
        This function orchestrates the entire analysis workflow:
        1. Fetch PNS data (fail fast if not available)
        2. Fetch CSV data from BigQuery
        3. Process data using existing workflow
        4. Store results in Redis
        """
        try:
            logger.info(f"Starting analysis job {job_id} for MCAT ID {mcat_id}")
            
            # Step 1: Update status to PNS fetching
            await job_manager.update_job_status(
                job_id, JobStatus.PNS_FETCHING, progress=10, current_step="Fetching PNS data"
            )
            
            # Step 2: Fetch all data (PNS first, then CSV)
            data_success, combined_data, data_error = await self.data_fetcher.fetch_all_data(mcat_id)
            
            if not data_success:
                await job_manager.update_job_status(
                    job_id, JobStatus.FAILED, progress=0, error=data_error
                )
                logger.error(f"Job {job_id} failed during data fetching: {data_error}")
                return
            
            # Step 3: Update status to analyzing
            await job_manager.update_job_status(
                job_id, JobStatus.ANALYZING, progress=50, current_step="Processing specifications"
            )
            
            # Step 4: Process the data
            results = await self._process_analysis_data(combined_data)
            
            # Step 5: Store results and mark as completed
            final_results = {
                "job_id": job_id,
                "status": JobStatus.COMPLETED,
                "mcat_id": mcat_id,
                "individual_results": results["individual_results"],
                "final_validation": results["final_validation"],
                "processing_summary": results["processing_summary"]
            }
            
            await job_manager.store_job_results(job_id, final_results)
            
            logger.info(f"Successfully completed analysis job {job_id} for MCAT ID {mcat_id}")
            
        except Exception as e:
            error_msg = f"Unexpected error in job {job_id}: {str(e)}"
            logger.error(error_msg)
            
            # Try to update job status, but don't fail if Redis is down
            try:
                await job_manager.update_job_status(
                    job_id, JobStatus.FAILED, progress=0, error=error_msg
                )
            except Exception as redis_error:
                logger.error(f"Failed to update job status in Redis for {job_id}: {redis_error}")
                # Job will remain in previous state, but at least we logged the original error
    
    async def _process_analysis_data(self, combined_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process the fetched data using complete workflow orchestration
        """
        try:
            mcat_id = combined_data["mcat_id"]
            pns_json_content = combined_data["pns_json_content"]
            csv_sources = combined_data["csv_sources"]
            
            # Run the complete workflow (PNS → CSV Extraction → Triangulation)
            logger.info(f"Running complete workflow for MCAT {mcat_id}")
            workflow_results = await run_spec_extraction_workflow(
                mcat_id=mcat_id,
                pns_json_content=pns_json_content,
                csv_sources=csv_sources
            )
            
            # Check if workflow completed successfully
            if "error" in workflow_results:
                raise Exception(f"Workflow failed: {workflow_results['error']}")
            
            # Extract results from workflow
            pns_specs = workflow_results["pns_specs"]
            csv_agent_results = workflow_results["csv_agent_results"]
            triangulation_result = workflow_results["triangulation_result"]
            
            # Convert to API format
            
            # Convert PNS specs to API format
            pns_individual_results = []
            for i, spec in enumerate(pns_specs[:5], 1):
                pns_individual_results.append(SpecificationResult(
                    rank=i,
                    specification=spec.get("spec_name", "N/A"),
                    options=spec.get("option", "N/A"),
                    frequency=spec.get("frequency", "N/A"),
                    status=spec.get("spec_status", "N/A"),
                    priority=spec.get("importance_level", "N/A")
                ))
            
            # Convert CSV agent results to API format
            individual_csv_results = self._convert_csv_results_to_api_format(csv_agent_results)
            
            # Create individual results
            individual_results = IndividualResults(
                search_keywords=individual_csv_results.get("search_keywords", []),
                lms_chats=individual_csv_results.get("lms_chats", []),
                rejection_comments=individual_csv_results.get("rejection_comments", []),
                custom_spec=individual_csv_results.get("whatsapp_specs", []),
                pns_individual=pns_individual_results
            )
            
            # Convert triangulation results to API format
            final_validation = self._convert_triangulation_results_to_api_format(
                triangulation_result["triangulated_table"]
            )
            
            # Use processing summary from workflow
            processing_summary = workflow_results["processing_summary"]
            
            return {
                "individual_results": individual_results.dict(),
                "final_validation": final_validation,
                "processing_summary": processing_summary
            }
            
        except Exception as e:
            logger.error(f"Error processing analysis data: {e}")
            raise
    
    def _convert_csv_results_to_api_format(self, csv_agent_results: Dict[str, Any]) -> Dict[str, List[SpecificationResult]]:
        """
        Convert CSV agent results to API specification result format
        """
        processed_sources = {}
        
        for source_key, result in csv_agent_results.items():
            source_results = []
            
            if result.get("status") == "completed" and result.get("extracted_specs"):
                # Parse the extracted specs text into structured data
                specs_text = result["extracted_specs"]
                parsed_specs = self._parse_specs_text(specs_text)
                
                for i, spec in enumerate(parsed_specs[:5], 1):  # Top 5 per source
                    source_results.append(SpecificationResult(
                        rank=i,
                        specification=spec.get("specification", "N/A"),
                        options=spec.get("option", "N/A"),
                        frequency=str(spec.get("frequency", 0)),
                        status="Available",
                        priority="Standard"
                    ))
            
            processed_sources[source_key] = source_results
        
        return processed_sources
    
    def _convert_triangulation_results_to_api_format(self, triangulated_table: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert triangulation results to API format
        """
        api_results = []
        
        for item in triangulated_table:
            api_results.append({
                "rank": item.get("Rank", 0),
                "score": item.get("Score", 0),
                "pns": item.get("PNS", "N/A"),
                "options": item.get("Options", "N/A"),
                "search_keywords": item.get("search_keywords", "N/A"),
                "whatsapp_specs": item.get("whatsapp_specs", "N/A"),
                "rejection_comments": item.get("rejection_comments", "N/A"),
                "lms_chats": item.get("lms_chats", "N/A")
            })
        
        return api_results
    
    def _parse_specs_text(self, specs_text: str) -> List[Dict[str, Any]]:
        """
        Parse extracted specs text into structured data with robust error handling
        """
        specs = []
        if not specs_text or not isinstance(specs_text, str):
            logger.warning("Empty or invalid specs text provided")
            return specs
            
        lines = specs_text.strip().split('\n')
        
        # Skip header lines and find data rows
        data_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('|---') and '|' in line:
                # Skip table headers like "| Rank | Specification | ..."
                if not any(header in line.lower() for header in ['rank', 'specification', 'option', 'frequency']):
                    data_lines.append(line)
        
        for line_num, line in enumerate(data_lines, 1):
            try:
                # Remove leading/trailing | characters
                line = line.strip('|').strip()
                parts = [part.strip() for part in line.split('|')]
                
                if len(parts) >= 2:  # At least specification and option
                    # Handle frequency parsing more robustly
                    frequency = 0
                    if len(parts) > 2:
                        freq_str = parts[2].strip()
                        # Extract numbers from frequency string (e.g., "40 / 37 (Total: 77)" -> 77)
                        import re
                        numbers = re.findall(r'\d+', freq_str)
                        if numbers:
                            frequency = int(numbers[-1])  # Use the last number (usually total)
                    
                    specs.append({
                        "specification": parts[0] if parts[0] else "N/A",
                        "option": parts[1] if len(parts) > 1 and parts[1] else "N/A", 
                        "frequency": frequency
                    })
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse line {line_num}: '{line}' - {e}")
                continue
        
        logger.info(f"Parsed {len(specs)} specifications from text")
        return specs
