"""
Complete state management for LangGraph workflow
Copied from original Streamlit project with FastAPI adaptations
"""

from typing import Dict, List, Any, Optional
from typing_extensions import TypedDict, Annotated
import json
import operator

class SpecExtractionState(TypedDict):
    """State for the Spec Extraction LangGraph workflow"""
    
    # User inputs - these should not be updated after initialization
    product_name: str
    uploaded_files: Dict[str, str]  # {source_name: file_content}
    
    # PNS JSON content (now processed as regular agent)
    pns_json_content: str  # Raw PNS JSON content
    
    # Processing state - each agent updates its own unique key
    search_keywords_status: str
    whatsapp_specs_status: str
    rejection_comments_status: str
    lms_chats_status: str
    
    current_step: str
    
    # Agent outputs - each agent has its own unique key
    search_keywords_result: Dict[str, Any]
    whatsapp_specs_result: Dict[str, Any]
    rejection_comments_result: Dict[str, Any]
    lms_chats_result: Dict[str, Any]
    
    # PNS processed data (direct from JSON, not agent result)
    pns_processed_specs: List[Dict[str, Any]]  # Direct PNS specifications
    
    # Triangulation results (single stage only)
    triangulated_result: str
    triangulated_table: List[Dict[str, Any]]  # For CSV download
    
    # Progress & logging - using annotations for concurrent updates
    progress_percentage: int
    logs: Annotated[List[str], operator.add]  # Allow concurrent log additions
    
    # Errors - each agent has its own unique error key
    search_keywords_error: str
    whatsapp_specs_error: str
    rejection_comments_error: str
    lms_chats_error: str
    pns_processing_error: str  # PNS processing error (direct processing)

# Dataset type mapping
DATASET_TYPE_MAPPING = {
    "search_keywords": "internal-search",      # Uses pageviews
    "whatsapp_specs": "buyer-specs",          # Uses Frequency  
    "rejection_comments": "rejection-reasons", # Uses Frequency
    "lms_chats": "chat-data",                 # Uses Frequency
}

# Column mappings for CSV files
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

# Source names mapping
SOURCE_NAMES = {
    "search_keywords": "Internal Search Keywords",
    "whatsapp_specs": "WhatsApp Conversations", 
    "rejection_comments": "BLNI Comments/QRF Data",
    "lms_chats": "LMS Chat Logs",
}

def create_initial_state(product_name: str, files: Dict[str, str], pns_json: str = "") -> SpecExtractionState:
    """Create initial state for the workflow"""
    
    # Extract product name from PNS JSON if not provided or is default
    final_product_name = product_name
    if pns_json and product_name in ["Unknown Product", "", None]:
        try:
            pns_data = json.loads(pns_json)
            category_name = pns_data.get("category_name", "")
            if category_name:
                final_product_name = category_name
                print(f"✅ Extracted product name from PNS JSON: {final_product_name}")
        except Exception as e:
            print(f"⚠️ Could not extract category_name from PNS JSON: {e}")
    
    return SpecExtractionState(
        product_name=final_product_name,
        uploaded_files=files,
        
        # PNS JSON content (processed directly, not as agent)
        pns_json_content=pns_json,
        
        search_keywords_status="idle" if "search_keywords" in files else "not_uploaded",
        whatsapp_specs_status="idle" if "whatsapp_specs" in files else "not_uploaded",
        rejection_comments_status="idle" if "rejection_comments" in files else "not_uploaded",
        lms_chats_status="idle" if "lms_chats" in files else "not_uploaded",
        current_step="initialization",
        search_keywords_result={},
        whatsapp_specs_result={},
        rejection_comments_result={},
        lms_chats_result={},
        
        # PNS processed directly from JSON
        pns_processed_specs=[],
        
        triangulated_result="",
        triangulated_table=[],
        
        progress_percentage=0,
        logs=[f"Initialized single-stage workflow for product: {final_product_name}"],
        search_keywords_error="",
        whatsapp_specs_error="",
        rejection_comments_error="",
        lms_chats_error="",
        pns_processing_error=""  # PNS direct processing error
    )

def get_agents_status(state: SpecExtractionState) -> Dict[str, str]:
    """Get agents status from individual status fields (CSV agents only)"""
    return {
        "search_keywords": state["search_keywords_status"],
        "whatsapp_specs": state["whatsapp_specs_status"],
        "rejection_comments": state["rejection_comments_status"],
        "lms_chats": state["lms_chats_status"],
    }

def get_agent_results(state: SpecExtractionState) -> Dict[str, Dict[str, Any]]:
    """Get agent results from individual result fields (CSV agents only)"""
    return {
        "search_keywords": state["search_keywords_result"],
        "whatsapp_specs": state["whatsapp_specs_result"],
        "rejection_comments": state["rejection_comments_result"],
        "lms_chats": state["lms_chats_result"],
    }

def get_errors(state: SpecExtractionState) -> Dict[str, str]:
    """Get errors from individual error fields (CSV agents only)"""
    return {
        "search_keywords": state["search_keywords_error"],
        "whatsapp_specs": state["whatsapp_specs_error"],
        "rejection_comments": state["rejection_comments_error"],
        "lms_chats": state["lms_chats_error"],
    }

def process_pns_data_directly(state: SpecExtractionState) -> SpecExtractionState:
    """Process PNS JSON data directly (not as an agent)"""
    if not state.get("pns_json_content"):
        return {
            "pns_processed_specs": [],
            "pns_processing_error": "No PNS JSON content provided"
        }
    
    try:
        from ..services.pns_processor import process_pns_json
        
        # Process PNS JSON directly
        pns_result = process_pns_json(state["pns_json_content"])
        
        if pns_result["status"] == "completed":
            return {
                "pns_processed_specs": pns_result["extracted_specs"],
                "pns_processing_error": ""
            }
        else:
            return {
                "pns_processed_specs": [],
                "pns_processing_error": pns_result.get("error", "PNS processing failed")
            }
            
    except Exception as e:
        return {
            "pns_processed_specs": [],
            "pns_processing_error": f"PNS processing error: {str(e)}"
        }
