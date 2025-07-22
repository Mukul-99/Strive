from typing import Dict, List, Any, Optional
from typing_extensions import TypedDict, Annotated
import json
import operator

class SpecExtractionState(TypedDict):
    """State for the Spec Extraction LangGraph workflow"""
    
    # User inputs - these should not be updated after initialization
    product_name: str
    uploaded_files: Dict[str, str]  # {source_name: file_content}
    
    # PNS JSON processing (separate from CSV workflow)
    pns_json_content: str  # Raw PNS JSON content
    pns_extracted_specs: List[Dict[str, Any]]  # Extracted PNS specs (max 5)
    pns_processing_status: str  # idle, processing, completed, failed
    pns_error: str
    
    # Meta-ensemble tracking - COMMENTED OUT
    # meta_ensemble_enabled: bool
    # current_run: int  # 1, 2, or 3
    # total_runs: int   # Always 3 for meta-ensemble
    # run_results: List[Dict[str, Any]]  # Store results from each run
    
    # Processing state - each agent updates its own unique key (PNS removed)
    search_keywords_status: str
    whatsapp_specs_status: str
    # pns_calls_status: str  # Commented out - now handled as JSON
    rejection_comments_status: str
    lms_chats_status: str
    
    current_step: str
    
    # Agent outputs - each agent has its own unique key (PNS removed)
    search_keywords_result: Dict[str, Any]
    whatsapp_specs_result: Dict[str, Any]
    # pns_calls_result: Dict[str, Any]  # Commented out - now handled as JSON
    rejection_comments_result: Dict[str, Any]
    lms_chats_result: Dict[str, Any]
    
    # Triangulation results (single run only)
    triangulated_result: str
    triangulated_table: List[Dict[str, Any]]  # For CSV download
    
    # Final triangulation (CSV triangulated + PNS specs)
    final_triangulated_result: str
    final_triangulated_table: List[Dict[str, Any]]
    
    # Meta-ensemble final result - COMMENTED OUT
    # final_ensemble_result: str
    # final_ensemble_table: List[Dict[str, Any]]
    
    # Progress & logging - using annotations for concurrent updates
    progress_percentage: int
    logs: Annotated[List[str], operator.add]  # Allow concurrent log additions
    
    # Errors - each agent has its own unique error key (PNS removed)
    search_keywords_error: str
    whatsapp_specs_error: str
    # pns_calls_error: str  # Commented out - now handled as JSON
    rejection_comments_error: str
    lms_chats_error: str

# Dataset type mapping
DATASET_TYPE_MAPPING = {
    "search_keywords": "internal-search",      # Uses pageviews
    "whatsapp_specs": "buyer-specs",          # Uses Frequency  
    # "pns_calls": "call-transcripts",        # Commented out - now JSON processing
    "rejection_comments": "rejection-reasons", # Uses Frequency
    "lms_chats": "chat-data"                  # Uses Frequency
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
}

# Source names mapping
SOURCE_NAMES = {
    "search_keywords": "Internal Search Keywords",
    "whatsapp_specs": "WhatsApp Conversations", 
    # "pns_calls": "PNS Call Transcript",  # Commented out - now JSON processing
    "rejection_comments": "BLNI Comments/QRF Data",
    "lms_chats": "LMS Chat Logs"
}

def create_initial_state(product_name: str, files: Dict[str, str], pns_json: str = "") -> SpecExtractionState:
    """Create initial state for the workflow"""
    return SpecExtractionState(
        product_name=product_name,
        uploaded_files=files,
        
        # PNS JSON processing
        pns_json_content=pns_json,
        pns_extracted_specs=[],
        pns_processing_status="idle" if pns_json else "not_uploaded",
        pns_error="",
        
        # Meta-ensemble tracking - COMMENTED OUT
        # meta_ensemble_enabled=True,  # Always enable meta-ensemble
        # current_run=0,
        # total_runs=3,
        # run_results=[],
        
        search_keywords_status="idle" if "search_keywords" in files else "not_uploaded",
        whatsapp_specs_status="idle" if "whatsapp_specs" in files else "not_uploaded",
        # pns_calls_status="idle" if "pns_calls" in files else "not_uploaded",  # Commented out
        rejection_comments_status="idle" if "rejection_comments" in files else "not_uploaded",
        lms_chats_status="idle" if "lms_chats" in files else "not_uploaded",
        current_step="initialization",
        search_keywords_result={},
        whatsapp_specs_result={},
        # pns_calls_result={},  # Commented out
        rejection_comments_result={},
        lms_chats_result={},
        triangulated_result="",
        triangulated_table=[],
        
        # Final triangulation (CSV triangulated + PNS specs)
        final_triangulated_result="",
        final_triangulated_table=[],
        
        # Meta-ensemble final result - COMMENTED OUT
        # final_ensemble_result="",
        # final_ensemble_table=[],
        
        progress_percentage=0,
        logs=[f"Initialized single-run workflow for product: {product_name}"],
        search_keywords_error="",
        whatsapp_specs_error="",
        # pns_calls_error="",  # Commented out
        rejection_comments_error="",
        lms_chats_error=""
    )

def get_agents_status(state: SpecExtractionState) -> Dict[str, str]:
    """Get agents status from individual status fields"""
    return {
        "search_keywords": state["search_keywords_status"],
        "whatsapp_specs": state["whatsapp_specs_status"],
        # "pns_calls": state["pns_calls_status"],  # Commented out
        "rejection_comments": state["rejection_comments_status"],
        "lms_chats": state["lms_chats_status"]
    }

def get_agent_results(state: SpecExtractionState) -> Dict[str, Dict[str, Any]]:
    """Get agent results from individual result fields"""
    return {
        "search_keywords": state["search_keywords_result"],
        "whatsapp_specs": state["whatsapp_specs_result"],
        # "pns_calls": state["pns_calls_result"],  # Commented out
        "rejection_comments": state["rejection_comments_result"],
        "lms_chats": state["lms_chats_result"]
    }

def get_errors(state: SpecExtractionState) -> Dict[str, str]:
    """Get errors from individual error fields"""
    return {
        "search_keywords": state["search_keywords_error"],
        "whatsapp_specs": state["whatsapp_specs_error"],
        # "pns_calls": state["pns_calls_error"],  # Commented out
        "rejection_comments": state["rejection_comments_error"],
        "lms_chats": state["lms_chats_error"]
    } 