"""
Data processing utilities for CSV sources
Simplified version adapted from existing Streamlit project
"""

import pandas as pd
import json
import re
from typing import Dict, List, Any, Optional
import logging
from io import StringIO

logger = logging.getLogger(__name__)

# Column mappings for different sources (must match original exactly)
COLUMN_MAPPINGS = {
    "search_keywords": {
        "data_column": "decoded_keyword",
        "frequency_column": "pageviews"
    },
    "whatsapp_specs": {
        "data_column": "fk_im_spec_options_desc",
        "frequency_column": "Frequency"
    },
    "rejection_comments": {
        "data_column": "eto_ofr_reject_comment",
        "frequency_column": "Frequency"
    },
    "lms_chats": {
        "data_column": "message_text_json",
        "frequency_column": "Frequency"
    }
}

class DataProcessor:
    """Handles data processing for different CSV sources"""
    
    @staticmethod
    def process_csv_data(file_content: str, source_name: str, max_rows: int = 8500) -> list:
        """Process CSV data with preprocessing pipeline and return list of chunks for batching"""
        try:
            # Read CSV from string content
            from io import StringIO
            df = pd.read_csv(StringIO(file_content))
            
            logger.info(f"Processing {source_name}: {len(df)} rows loaded")
            
            # Check minimum row requirement (10 rows minimum for processing)
            if len(df) < 10:
                logger.warning(f"Dataset {source_name} excluded: Only {len(df)} rows available, minimum 10 rows required for processing")
                return []  # Return empty list to skip processing
            
            # Get column mapping for this source
            column_config = COLUMN_MAPPINGS.get(source_name)
            if not column_config:
                raise ValueError(f"No column mapping found for source: {source_name}")
            
            logger.info(f"Skipping preprocessing - using clean data as-is: {len(df)} rows")
            
            # For now, return single chunk (can be enhanced for large datasets)
            # Convert back to CSV string format
            processed_content = df.to_csv(index=False)
            return [processed_content]
            
        except Exception as e:
            logger.error(f"Error processing CSV data for {source_name}: {e}")
            return []
    
    @staticmethod
    def process_csv_data_from_bigquery(data: List[Dict[str, Any]], source_name: str) -> str:
        """Process CSV data from BigQuery results into formatted text"""
        try:
            logger.info(f"Processing {source_name}: {len(data)} rows from BigQuery")
            
            if len(data) < 10:
                logger.warning(f"Dataset {source_name} excluded: Only {len(data)} rows available, minimum 10 rows required")
                return ""
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(data)
            
            # Get column configuration
            column_config = COLUMN_MAPPINGS.get(source_name)
            if not column_config:
                raise ValueError(f"No column mapping found for source: {source_name}")
            
            # Process based on source type
            if source_name == "search_keywords":
                return DataProcessor._process_search_keywords(df, column_config)
            elif source_name == "whatsapp_specs":
                return DataProcessor._process_whatsapp_specs(df, column_config)
            elif source_name == "rejection_comments":
                return DataProcessor._process_rejection_comments(df, column_config)
            elif source_name == "lms_chats":
                return DataProcessor._process_lms_chats(df, column_config)
            else:
                raise ValueError(f"Unknown source type: {source_name}")
                
        except Exception as e:
            logger.error(f"Error processing {source_name}: {str(e)}")
            raise
    
    @staticmethod
    def _process_search_keywords(df: pd.DataFrame, config: Dict) -> str:
        """Process search keywords data"""
        data_col = config["data_column"]
        freq_col = config["frequency_column"]
        
        # Ensure required columns exist
        if data_col not in df.columns or freq_col not in df.columns:
            # Map from BigQuery result columns
            if "spec_kw" in df.columns:
                data_col = "spec_kw"
            if "frequency" in df.columns:
                freq_col = "frequency"
        
        # Clean and sort data
        df_clean = df[[data_col, freq_col]].dropna()
        df_clean = df_clean[df_clean[data_col].astype(str).str.strip() != ""]
        df_clean = df_clean.sort_values(freq_col, ascending=False)
        
        # Format as CSV text
        formatted_text = f"# SEARCH KEYWORDS DATA\n"
        formatted_text += f"{data_col},{freq_col}\n"
        
        for _, row in df_clean.iterrows():
            formatted_text += f"{row[data_col]},{row[freq_col]}\n"
        
        logger.info(f"Processed search keywords: {len(df_clean)} entries")
        return formatted_text
    
    @staticmethod
    def _process_whatsapp_specs(df: pd.DataFrame, config: Dict) -> str:
        """Process WhatsApp specs data"""
        data_col = config["data_column"]
        
        # Map from BigQuery result columns
        if "spec_kw" in df.columns:
            data_col = "spec_kw"
        
        df_clean = df[data_col].dropna()
        df_clean = df_clean[df_clean.astype(str).str.strip() != ""]
        
        # Format as CSV text
        formatted_text = f"# WHATSAPP SPECIFICATIONS\n"
        formatted_text += f"{data_col}\n"
        for spec in df_clean.unique():
            formatted_text += f"{spec}\n"
        
        logger.info(f"Processed WhatsApp specs: {len(df_clean)} entries")
        return formatted_text
    
    @staticmethod
    def _process_rejection_comments(df: pd.DataFrame, config: Dict) -> str:
        """Process rejection comments data"""
        data_col = config["data_column"]
        
        # Map from BigQuery result columns
        if "spec_kw" in df.columns:
            data_col = "spec_kw"
        
        df_clean = df[data_col].dropna()
        df_clean = df_clean[df_clean.astype(str).str.strip() != ""]
        
        # Format as CSV text
        formatted_text = f"# REJECTION COMMENTS\n"
        formatted_text += f"{data_col}\n"
        for comment in df_clean.unique():
            formatted_text += f"{comment}\n"
        
        logger.info(f"Processed rejection comments: {len(df_clean)} entries")
        return formatted_text
    
    @staticmethod
    def _process_lms_chats(df: pd.DataFrame, config: Dict) -> str:
        """Process LMS chats data"""
        data_col = config["data_column"]
        
        # Map from BigQuery result columns
        if "spec_kw" in df.columns:
            data_col = "spec_kw"
        
        df_clean = df[data_col].dropna()
        
        formatted_text = f"# LMS CHAT DATA\n"
        formatted_text += f"extracted_chat_data\n"
        processed_count = 0
        
        for entry in df_clean:
            try:
                # For now, treat as simple text (can be enhanced for JSON parsing)
                if isinstance(entry, str) and entry.strip():
                    formatted_text += f"{entry}\n"
                    processed_count += 1
                    
            except Exception as e:
                logger.warning(f"Error processing LMS chat entry: {str(e)}")
                continue
        
        logger.info(f"Processed LMS chats: {processed_count} entries")
        return formatted_text
