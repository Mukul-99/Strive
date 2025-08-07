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
    # pns_calls_status: str  # Commented out - now handled as JSON
    rejection_comments_status: str
    lms_chats_status: str
    # pns_data_status: str  # REMOVED: PNS no longer an agent
    
    current_step: str
    
    # Agent outputs - each agent has its own unique key
    search_keywords_result: Dict[str, Any]
    whatsapp_specs_result: Dict[str, Any]
    # pns_calls_result: Dict[str, Any]  # Commented out - now handled as JSON
    rejection_comments_result: Dict[str, Any]
    lms_chats_result: Dict[str, Any]
    # pns_data_result: Dict[str, Any]  # REMOVED: PNS no longer an agent
    
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
    # pns_calls_error: str  # Commented out - now handled as JSON
    rejection_comments_error: str
    lms_chats_error: str
    # pns_data_error: str  # REMOVED: PNS no longer an agent
    pns_processing_error: str  # PNS processing error (direct processing)

# Dataset type mapping
DATASET_TYPE_MAPPING = {
    "search_keywords": "internal-search",      # Uses pageviews
    "whatsapp_specs": "buyer-specs",          # Uses Frequency  
    # "pns_calls": "call-transcripts",        # Commented out - now JSON processing
    "rejection_comments": "rejection-reasons", # Uses Frequency
    "lms_chats": "chat-data",                 # Uses Frequency
    # "pns_data": "pns-json"                  # REMOVED: PNS no longer treated as agent
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
    # "pns_calls": {
    #     "data_column": "transcribed_text"
    # },  # Commented out - now handled as JSON upload
    "rejection_comments": {
        "data_column": "eto_ofr_reject_comment",
        "frequency_column": "Frequency"
    },
    "lms_chats": {
        "data_column": "message_text_json",
        "frequency_column": "Frequency"
    }
    # PNS data doesn't use column mappings - processed directly from JSON
}

# Source names mapping
SOURCE_NAMES = {
    "search_keywords": "Internal Search Keywords",
    "whatsapp_specs": "WhatsApp Conversations", 
    # "pns_calls": "PNS Call Transcript",  # Commented out - now JSON processing
    "rejection_comments": "BLNI Comments/QRF Data",
    "lms_chats": "LMS Chat Logs",
    # "pns_data": "PNS JSON Specifications"  # REMOVED: PNS no longer treated as agent source
}

def create_initial_state(product_name: str, files: Dict[str, str], pns_json: str = "") -> SpecExtractionState:
    """Create initial state for the workflow"""
    return SpecExtractionState(
        product_name=product_name,
        uploaded_files=files,
        
        # PNS JSON content (processed directly, not as agent)
        pns_json_content=pns_json,
        
        search_keywords_status="idle" if "search_keywords" in files else "not_uploaded",
        whatsapp_specs_status="idle" if "whatsapp_specs" in files else "not_uploaded",
        # pns_calls_status="idle" if "pns_calls" in files else "not_uploaded",  # Commented out
        rejection_comments_status="idle" if "rejection_comments" in files else "not_uploaded",
        lms_chats_status="idle" if "lms_chats" in files else "not_uploaded",
        # pns_data_status="idle" if pns_json else "not_uploaded",  # REMOVED: PNS no longer an agent
        current_step="initialization",
        search_keywords_result={},
        whatsapp_specs_result={},
        # pns_calls_result={},  # Commented out
        rejection_comments_result={},
        lms_chats_result={},
        # pns_data_result={},  # REMOVED: PNS no longer an agent
        
        # PNS processed directly from JSON
        pns_processed_specs=[],
        
        triangulated_result="",
        triangulated_table=[],
        
        progress_percentage=0,
        logs=[f"Initialized single-stage workflow for product: {product_name}"],
        search_keywords_error="",
        whatsapp_specs_error="",
        # pns_calls_error="",  # Commented out
        rejection_comments_error="",
        lms_chats_error="",
        # pns_data_error=""  # REMOVED: PNS no longer an agent
        pns_processing_error=""  # PNS direct processing error
    )

def get_agents_status(state: SpecExtractionState) -> Dict[str, str]:
    """Get agents status from individual status fields (CSV agents only)"""
    return {
        "search_keywords": state["search_keywords_status"],
        "whatsapp_specs": state["whatsapp_specs_status"],
        # "pns_calls": state["pns_calls_status"],  # Commented out
        "rejection_comments": state["rejection_comments_status"],
        "lms_chats": state["lms_chats_status"],
        # "pns_data": state["pns_data_status"]  # REMOVED: PNS no longer an agent
    }

def get_agent_results(state: SpecExtractionState) -> Dict[str, Dict[str, Any]]:
    """Get agent results from individual result fields (CSV agents only)"""
    return {
        "search_keywords": state["search_keywords_result"],
        "whatsapp_specs": state["whatsapp_specs_result"],
        # "pns_calls": state["pns_calls_result"],  # Commented out
        "rejection_comments": state["rejection_comments_result"],
        "lms_chats": state["lms_chats_result"],
        # "pns_data": state["pns_data_result"]  # REMOVED: PNS no longer an agent
    }

def get_errors(state: SpecExtractionState) -> Dict[str, str]:
    """Get errors from individual error fields (CSV agents only)"""
    return {
        "search_keywords": state["search_keywords_error"],
        "whatsapp_specs": state["whatsapp_specs_error"],
        # "pns_calls": state["pns_calls_error"],  # Commented out
        "rejection_comments": state["rejection_comments_error"],
        "lms_chats": state["lms_chats_error"],
        # "pns_data": state["pns_data_error"]  # REMOVED: PNS no longer an agent
    }

def process_pns_data_directly(state: SpecExtractionState) -> SpecExtractionState:
    """Process PNS JSON data directly (not as an agent)"""
    if not state.get("pns_json_content"):
        return {
            "pns_processed_specs": [],
            "pns_processing_error": "No PNS JSON content provided"
        }
    
    try:
        from ..agents.pns_processor import process_pns_json
        
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