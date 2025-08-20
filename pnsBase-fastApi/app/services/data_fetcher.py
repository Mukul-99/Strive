"""
Data fetching services for PNS API and BigQuery
"""

import httpx
import json
import pandas as pd
from google.cloud import bigquery
from typing import Dict, List, Any, Optional, Tuple
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

class PNSAPIClient:
    """Client for PNS API integration"""
    
    def __init__(self):
        self.base_url = settings.pns_api_base_url
        self.endpoint = settings.pns_api_endpoint
        self.timeout = settings.pns_api_timeout
    
    async def fetch_pns_data(self, mcat_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Fetch PNS JSON data for given MCAT ID
        
        Returns:
            Tuple[bool, Optional[str], Optional[str]]: (success, pns_json_content, error_message)
        """
        try:
            url = f"{self.base_url}{self.endpoint}"
            params = {"mcat_id": mcat_id}
            headers = {"Content-Type": "application/json"}
            
            logger.info(f"Fetching PNS data for MCAT ID: {mcat_id}")
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, params=params, headers=headers, json={})
                
                if response.status_code != 200:
                    error_msg = f"PNS API returned status {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    return False, None, error_msg
                
                api_response = response.json()
                
                # Check if response contains signed URL
                signed_url = self._extract_signed_url(api_response)
                if not signed_url or signed_url == "NA":
                    error_msg = f"No PNS data available for MCAT ID: {mcat_id}"
                    logger.warning(error_msg)
                    return False, None, error_msg
                
                # Fetch JSON from signed URL
                pns_json_content = await self._fetch_json_from_signed_url(signed_url)
                if not pns_json_content:
                    error_msg = f"Failed to fetch PNS JSON from signed URL for MCAT ID: {mcat_id}"
                    logger.error(error_msg)
                    return False, None, error_msg
                
                logger.info(f"Successfully fetched PNS data for MCAT ID: {mcat_id}")
                return True, pns_json_content, None
                
        except httpx.TimeoutException:
            error_msg = f"PNS API timeout for MCAT ID: {mcat_id}"
            logger.error(error_msg)
            return False, None, error_msg
        except Exception as e:
            error_msg = f"PNS API error for MCAT ID {mcat_id}: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg
    
    def _extract_signed_url(self, api_response: Dict[str, Any]) -> Optional[str]:
        """Extract signed_url from API response"""
        try:
            if "gcs_urls" in api_response and "signed_url" in api_response["gcs_urls"]:
                signed_url = api_response["gcs_urls"]["signed_url"]
                if signed_url and signed_url.strip():
                    return signed_url
            return None
        except Exception as e:
            logger.error(f"Error extracting signed_url: {e}")
            return None
    
    async def _fetch_json_from_signed_url(self, signed_url: str) -> Optional[str]:
        """Fetch JSON content from GCS signed URL"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(signed_url)
                
                if response.status_code == 200:
                    return response.text
                else:
                    logger.error(f"Failed to fetch from signed URL: {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching from signed URL: {e}")
            return None

class BigQueryClient:
    """Client for BigQuery data fetching"""
    
    def __init__(self):
        # TODO: Replace with actual BigQuery credentials
        self.project_id = settings.bigquery_project_id
        self.dataset = settings.bigquery_dataset
        self.table = settings.bigquery_table
        self.credentials_path = settings.bigquery_credentials_path
        
        # Initialize BigQuery client (dummy for now)
        # TODO: Replace with actual client initialization
        # self.client = bigquery.Client.from_service_account_json(self.credentials_path)
        self.client = None  # Placeholder
    
    async def fetch_csv_data(self, mcat_id: str) -> Tuple[bool, Optional[Dict[str, List[Dict[str, Any]]]], Optional[str]]:
        """
        Fetch CSV data for all 4 sources from BigQuery
        
        Returns:
            Tuple[bool, Optional[Dict], Optional[str]]: (success, grouped_data, error_message)
        """
        try:
            logger.info(f"Fetching CSV data for MCAT ID: {mcat_id}")
            
            # TODO: Replace with actual BigQuery query
            # For now, return dummy data structure
            dummy_data = self._generate_dummy_csv_data(mcat_id)
            
            logger.info(f"Successfully fetched CSV data for MCAT ID: {mcat_id}")
            return True, dummy_data, None
            
        except Exception as e:
            error_msg = f"BigQuery error for MCAT ID {mcat_id}: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg
    
    def _generate_dummy_csv_data(self, mcat_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate dummy CSV data structure for testing
        TODO: Replace with actual BigQuery data fetching
        """
        return {
            "search_keywords": [
                {"spec_kw": "100 kg/hr", "frequency": 40, "source": "Search Keywords"},
                {"spec_kw": "200 kg/hr", "frequency": 37, "source": "Search Keywords"},
                {"spec_kw": "50 kg/hr", "frequency": 35, "source": "Search Keywords"}
            ],
            "lms_chats": [
                {"spec_kw": "Motor Power", "frequency": 25, "source": "LMS"},
                {"spec_kw": "Cooling Capacity", "frequency": 20, "source": "LMS"}
            ],
            "rejection_comments": [
                {"spec_kw": "Phase", "frequency": 15, "source": "BLNI"},
                {"spec_kw": "Grinding Mechanism", "frequency": 12, "source": "BLNI"}
            ],
            "custom_spec": [
                {"spec_kw": "100kg", "frequency": 5, "source": "Custom Spec"},
                {"spec_kw": "100 kg/hr, 80 kg/hr", "frequency": 4, "source": "Custom Spec"}
            ]
        }
    
    async def _execute_bigquery_query(self, mcat_id: str) -> Optional[pd.DataFrame]:
        """
        Execute BigQuery query to fetch data
        TODO: Implement actual BigQuery query execution
        """
        # Placeholder query - replace with actual implementation
        query = f"""
        SELECT spec_kw, frequency, source 
        FROM `{self.project_id}.{self.dataset}.{self.table}`
        WHERE mcat_id = '{mcat_id}'
        ORDER BY frequency DESC
        """
        
        # TODO: Execute query and return DataFrame
        # job = self.client.query(query)
        # return job.to_dataframe()
        
        return None  # Placeholder

class DataFetcher:
    """Main data fetching orchestrator"""
    
    def __init__(self):
        self.pns_client = PNSAPIClient()
        self.bq_client = BigQueryClient()
    
    async def fetch_all_data(self, mcat_id: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Fetch both PNS and CSV data for given MCAT ID
        
        Returns:
            Tuple[bool, Optional[Dict], Optional[str]]: (success, combined_data, error_message)
        """
        try:
            # Step 1: Fetch PNS data first (fail fast if not available)
            pns_success, pns_json, pns_error = await self.pns_client.fetch_pns_data(mcat_id)
            
            if not pns_success:
                return False, None, pns_error
            
            # Step 2: Fetch CSV data from BigQuery
            csv_success, csv_data, csv_error = await self.bq_client.fetch_csv_data(mcat_id)
            
            if not csv_success:
                return False, None, csv_error
            
            # Combine all data
            combined_data = {
                "mcat_id": mcat_id,
                "pns_json_content": pns_json,
                "csv_sources": csv_data
            }
            
            logger.info(f"Successfully fetched all data for MCAT ID: {mcat_id}")
            return True, combined_data, None
            
        except Exception as e:
            error_msg = f"Data fetching error for MCAT ID {mcat_id}: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg
