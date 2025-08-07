import os
import logging
import time
from typing import Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from ..utils.state import SpecExtractionState, DATASET_TYPE_MAPPING
from ..utils.data_processor import DataProcessor

logger = logging.getLogger(__name__)

class ExtractionAgent:
    """Individual agent for processing each data source"""
    
    def __init__(self):
        self.llm = ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            temperature=0.1,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
    
    def process_source(self, source_name: str, product_name: str, file_content: str) -> Dict[str, Any]:
        """Process a single data source with multiple chunks and batching"""
        start_time = time.time()
        
        try:
            logger.info(f"Starting processing for {source_name}")
            
            # Process the data into chunks
            data_chunks = DataProcessor.process_csv_data(file_content, source_name)
            
            # Check if dataset was excluded due to insufficient rows
            if not data_chunks:  # Empty list means dataset was excluded
                logger.info(f"Dataset {source_name} was excluded due to insufficient rows (<10), skipping processing")
                return {
                    "source_type": DATASET_TYPE_MAPPING.get(source_name, "occurrences"),
                    "raw_data_count": 0,
                    "extracted_specs": "",
                    "processing_time": round(time.time() - start_time, 2),
                    "status": "excluded",
                    "exclusion_reason": "Insufficient rows: Dataset contains less than 10 rows required for processing",
                    "chunks_processed": 0
                }
            
            # Get dataset type for this source
            dataset_type = DATASET_TYPE_MAPPING.get(source_name, "occurrences")
            
            all_chunk_results = []
            total_row_count = 0
            
            # Process each chunk separately
            for chunk_idx, chunk_data in enumerate(data_chunks, 1):
                logger.info(f"Processing chunk {chunk_idx}/{len(data_chunks)} for {source_name}")
                
                # Build the extraction prompt for this chunk
                prompt = self._build_extraction_prompt(
                    product_name=product_name,
                    dataset_type=dataset_type,
                    text=chunk_data,
                    chunk_info=f"(Chunk {chunk_idx}/{len(data_chunks)})"
                )
                
                logger.info(f"Sending extraction request for {source_name} chunk {chunk_idx}")
                
                # Call LLM for this chunk
                response = self.llm.invoke([HumanMessage(content=prompt)])
                chunk_result = response.content
                all_chunk_results.append(chunk_result)
                
                # Count rows in this chunk
                chunk_row_count = len([line for line in chunk_data.split('\n') if line.strip()]) - 1  # -1 for header
                total_row_count += chunk_row_count
                
                logger.info(f"Completed chunk {chunk_idx} for {source_name} ({chunk_row_count} rows)")
            
            # Merge all chunk results if multiple chunks
            if len(data_chunks) > 1:
                logger.info(f"Merging {len(data_chunks)} chunk results for {source_name}")
                extracted_specs = self._merge_chunk_results(all_chunk_results, product_name, dataset_type)
            else:
                extracted_specs = all_chunk_results[0]
            
            # Debug: Log the final extracted specs
            logger.info(f"Agent {source_name} final extracted specs: {extracted_specs}")
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Create result object
            result = {
                "source_type": dataset_type,
                "raw_data_count": total_row_count,
                "extracted_specs": extracted_specs,
                "processing_time": round(processing_time, 2),
                "status": "completed",
                "chunks_processed": len(data_chunks)
            }
            
            logger.info(f"Completed processing for {source_name} in {processing_time:.2f}s ({len(data_chunks)} chunks)")
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing {source_name}: {error_msg}")
            
            return {
                "source_type": DATASET_TYPE_MAPPING.get(source_name, "occurrences"),
                "raw_data_count": 0,
                "extracted_specs": "",
                "processing_time": time.time() - start_time,
                "status": "failed",
                "error": error_msg,
                "chunks_processed": 0
            }
    
    def _merge_chunk_results(self, chunk_results: list, product_name: str, dataset_type: str) -> str:
        """Merge results from multiple chunks"""
        logger.info(f"Merging {len(chunk_results)} chunk results")
        
        # Create a consolidation prompt
        merged_content = "\n\n--- CHUNK RESULTS TO MERGE ---\n\n"
        for i, result in enumerate(chunk_results, 1):
            merged_content += f"=== CHUNK {i} RESULTS ===\n{result}\n\n"
        
        consolidation_prompt = f"""<role>
You are a data consolidation expert specializing in merging multi-chunk extraction results with high precision.
</role>

<task>
Consolidate {len(chunk_results)} chunk results for {product_name} specifications into a single, deduplicated table.
</task>

<consolidation_process>
1. EXACT MATCHING: Identify identical specifications and options across chunks
2. SEMANTIC MATCHING: Merge similar specifications (e.g., "Power" = "Capacity" = "KVA Rating")
3. FREQUENCY SUMMATION: Add up occurrences/pageviews for merged items
4. RANKING: Re-rank by total frequency in descending order after consolidation
5. FILTERING: Keep only top 10-15 specifications with highest frequency
</consolidation_process>

<merge_rules>
• Preserve exact option names when merging
• Sum ALL occurrences for duplicate entries
• Maintain data integrity - no approximations
• Standardize format but preserve meaning
</merge_rules>

<output_format>
Rank | Specification | Option | {'Pageviews' if dataset_type == 'internal-search' else 'Occurrences'}

Sort by total frequency in descending order {'pageviews' if dataset_type == 'internal-search' else 'occurrences'} (descending).
</output_format>

{merged_content}"""
        
        # Call LLM to merge chunks
        response = self.llm.invoke([HumanMessage(content=consolidation_prompt)])
        return response.content
    
    def _build_extraction_prompt(self, product_name: str, dataset_type: str, text: str, chunk_info: str = "") -> str:
        """Build the extraction prompt using research-backed best practices for 90-95% accuracy"""
        
        # Enhanced prompt following CARE model and research findings
        prompt = f"""<role>
You are a world-class B2B specification extraction specialist with deep expertise in parsing {product_name} specifications from unstructured data. Your extractions consistently achieve 95%+ accuracy through systematic analysis.
</role>

<task>
Extract and categorize ALL product specifications (ISQs) for {product_name} from the CSV dataset {chunk_info}.

Your primary objective: Identify every meaningful specification category and its options with the exact frequency counts from the data.
</task>

<methodology>
Apply this proven 5-step extraction process:

STEP 1 - SEMANTIC DECOMPOSITION:
Break down each entry into atomic specification components. For example:
- "heavy duty metal scraps 22mm" → {{"Type": "Heavy Duty", "Material": "Metal", "Form": "Scraps", "Size": "22mm"}}
- "kirloskar 15 kva three phase generator" → {{"Brand": "Kirloskar", "Power": "15 KVA", "Phase": "Three Phase", "Product": "Generator"}}

STEP 2 - INTELLIGENT CATEGORIZATION:
Group specifications into standard categories:
• Power/Capacity: KVA ratings, wattage, horsepower
• Phase Configuration: Single Phase, Three Phase, DC
• Physical Attributes: Size, dimensions, weight
• Material/Build: Metal type, construction material
• Type/Form: Product form factor, design type
• Grade/Quality: Heavy duty, industrial, commercial
• Compliance: Standards, certifications
• Technical Specs: Voltage, frequency, RPM

STEP 3 - OPTION CONSOLIDATION:
Merge semantically identical options:
- "5 KVA", "5KVA", "5 kva" → "5 KVA" (standardized)
- "three phase", "3 phase", "3-phase" → "Three Phase"
- Sum frequencies for merged items

STEP 4 - RELEVANCE FILTERING:
Include only specifications that:
✓ Directly relate to {product_name} attributes
✓ Appear in multiple entries (frequency > 1)
✓ Represent purchasable options (not descriptions)
✗ Exclude: Generic terms, location names, company names

STEP 5 - FREQUENCY VALIDATION:
Count exact occurrences/pageviews for each option after consolidation.
</methodology>

<parsing_rules>
CRITICAL PARSING INSTRUCTIONS:
1. NEVER treat entire search phrases as single options
2. ALWAYS break compound terms into constituent specifications
3. Standardize similar values (e.g., "5KVA" → "5 KVA")
4. Preserve original frequency data - sum when merging duplicates
5. Focus on specifications buyers use to filter/select products
</parsing_rules>

<examples>
GOOD EXTRACTION:
Input: "silent diesel generator 15 kva", pageviews: 156
Output: Fuel: Diesel (156), Type: Silent (156), Power: 15 KVA (156)

BAD EXTRACTION:
Input: "silent diesel generator 15 kva", pageviews: 156  
Output: Description: "silent diesel generator 15 kva" (156) ❌

Input: "heavy metal scraps", pageviews: 89
GOOD: Grade: Heavy (89), Material: Metal (89), Form: Scraps (89)
BAD: Type: "Heavy Metal Scraps" (89) ❌
</examples>

<output_requirements>
Generate a ranked table with EXACTLY these columns:
Rank | Specification | Option | {'Pageviews' if dataset_type == 'internal-search' else 'Occurrences'}

Requirements:
- Rank by Frequency descending {'pageviews' if dataset_type == 'internal-search' else 'occurrences'} (descending)
- Show consolidated counts after merging duplicates
- Include only the top 10-15 with the highest frequency
- Each specification must have multiple distinct options
- Ensure specification names don't duplicate {product_name}
</output_requirements>

<validation_checklist>
Before finalizing, verify:
□ All compound phrases are properly decomposed
□ Semantically similar options are merged with summed counts
□ Rankings reflect true frequency after consolidation
□ No generic/irrelevant specifications included
□ Output follows exact table format
</validation_checklist>

<dataset>
{text}
</dataset>"""
        
        return prompt


# Helper function for status messages
def _get_status_message(result: Dict[str, Any]) -> str:
    """Get appropriate status message based on result status"""
    status = result.get("status", "unknown")
    if status == "completed":
        return "Completed successfully"
    elif status == "excluded":
        exclusion_reason = result.get("exclusion_reason", "Unknown reason")
        return f"Excluded - {exclusion_reason}"
    elif status == "failed":
        return "Failed"
    else:
        return f"Unknown status: {status}"

# Node functions for LangGraph
def process_search_keywords(state: SpecExtractionState) -> SpecExtractionState:
    """Process search keywords source"""
    if "search_keywords" not in state["uploaded_files"]:
        return {}
    
    agent = ExtractionAgent()
    result = agent.process_source(
        "search_keywords", 
        state["product_name"], 
        state["uploaded_files"]["search_keywords"]
    )
    
    # Return updates to unique keys only
    return {
        "search_keywords_status": result["status"],
        "search_keywords_result": result,
        "search_keywords_error": result.get("error", ""),
        "logs": [f"Agent search_keywords: {_get_status_message(result)} in {result['processing_time']:.2f}s"]
    }

def process_whatsapp_specs(state: SpecExtractionState) -> SpecExtractionState:
    """Process WhatsApp specs source"""
    if "whatsapp_specs" not in state["uploaded_files"]:
        return {}
    
    agent = ExtractionAgent()
    result = agent.process_source(
        "whatsapp_specs", 
        state["product_name"], 
        state["uploaded_files"]["whatsapp_specs"]
    )
    
    # Return updates to unique keys only
    return {
        "whatsapp_specs_status": result["status"],
        "whatsapp_specs_result": result,
        "whatsapp_specs_error": result.get("error", ""),
        "logs": [f"Agent whatsapp_specs: {_get_status_message(result)} in {result['processing_time']:.2f}s"]
    }

# COMMENTED OUT - PNS now handled as JSON processing
# def process_pns_calls(state: SpecExtractionState) -> SpecExtractionState:
#     """Process PNS calls source"""
#     if "pns_calls" not in state["uploaded_files"]:
#         return {}
#     
#     agent = ExtractionAgent()
#     result = agent.process_source(
#         "pns_calls", 
#         state["product_name"], 
#         state["uploaded_files"]["pns_calls"]
#     )
#     
#     # Return updates to unique keys only
#     return {
#         "pns_calls_status": result["status"],
#         "pns_calls_result": result,
#         "pns_calls_error": result.get("error", ""),
#         "logs": [f"Agent pns_calls: {'Completed successfully' if result['status'] == 'completed' else 'Failed'} in {result['processing_time']:.2f}s"]
#     }

def process_rejection_comments(state: SpecExtractionState) -> SpecExtractionState:
    """Process rejection comments source"""
    if "rejection_comments" not in state["uploaded_files"]:
        return {}
    
    agent = ExtractionAgent()
    result = agent.process_source(
        "rejection_comments", 
        state["product_name"], 
        state["uploaded_files"]["rejection_comments"]
    )
    
    # Return updates to unique keys only
    return {
        "rejection_comments_status": result["status"],
        "rejection_comments_result": result,
        "rejection_comments_error": result.get("error", ""),
        "logs": [f"Agent rejection_comments: {_get_status_message(result)} in {result['processing_time']:.2f}s"]
    }

def process_lms_chats(state: SpecExtractionState) -> SpecExtractionState:
    """Process LMS chats source"""
    if "lms_chats" not in state["uploaded_files"]:
        return {}
    
    agent = ExtractionAgent()
    result = agent.process_source(
        "lms_chats", 
        state["product_name"], 
        state["uploaded_files"]["lms_chats"]
    )
    
    # Return updates to unique keys only
    return {
        "lms_chats_status": result["status"],
        "lms_chats_result": result,
        "lms_chats_error": result.get("error", ""),
        "logs": [f"Agent lms_chats: {_get_status_message(result)} in {result['processing_time']:.2f}s"]
    }

# REMOVED: process_pns_data function - PNS is now processed directly, not as an agent 