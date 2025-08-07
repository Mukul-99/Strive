import os
import logging
import time
import json
from typing import Dict, Any, List
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from ..utils.state import SpecExtractionState, get_agents_status, get_agent_results

logger = logging.getLogger(__name__)

# COMMENTED OUT - MetaEnsembleAgent no longer used
# class MetaEnsembleAgent:
#     """Agent for performing final ensemble triangulation of multiple runs"""
#     
#     def __init__(self):
#         self.llm = ChatOpenAI(
#             model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
#             temperature=0.1
#         )
#     
#     def ensemble_triangulate(self, state: SpecExtractionState) -> SpecExtractionState:
#         """Perform final ensemble triangulation of 3 run results"""
#         start_time = time.time()
#         
#         try:
#             logger.info("Starting meta-ensemble triangulation")
#             
#             run_results = state["run_results"]
#             if len(run_results) != 3:
#                 raise ValueError(f"Expected 3 run results, got {len(run_results)}")
#             
#             # Build ensemble prompt
#             prompt = self._build_ensemble_prompt(
#                 product_name=state["product_name"],
#                 run_results=run_results
#             )
#             
#             logger.info("Sending meta-ensemble triangulation request")
#             
#             # Call LLM for final ensemble
#             response = self.llm.invoke([HumanMessage(content=prompt)])
#             final_result = response.content
#             
#             # Parse the final result into table format
#             final_table = self._parse_ensemble_result(final_result)
#             
#             # Calculate processing time
#             processing_time = time.time() - start_time
#             
#             logger.info(f"Meta-ensemble triangulation completed in {processing_time:.2f}s")
#             
#             return {
#                 "final_ensemble_result": final_result,
#                 "final_ensemble_table": final_table,
#                 "current_step": "meta_ensemble_completed",
#                 "progress_percentage": 100,
#                 "logs": [f"Meta-ensemble triangulation completed successfully in {processing_time:.2f}s"]
#             }
#             
#         except Exception as e:
#             error_msg = str(e)
#             logger.error(f"Error during meta-ensemble triangulation: {error_msg}")
#             
#             return {
#                 "current_step": "meta_ensemble_failed",
#                 "logs": [f"Meta-ensemble triangulation failed: {error_msg}"]
#             }

class TriangulationAgent:
    """Agent for PNS-centric validation against CSV sources"""
    
    def __init__(self):
        self.llm = ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            temperature=0.1,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
    
    def triangulate_results(self, state: SpecExtractionState) -> SpecExtractionState:
        """Triangulate CSV agent results with direct PNS specifications"""
        start_time = time.time()
        
        try:
            logger.info("Starting triangulation with CSV agents and direct PNS specs")
            
            # Get all completed CSV agent results
            agent_results = get_agent_results(state)
            completed_agents = {
                source: result for source, result in agent_results.items()
                if result.get("status") == "completed"
            }
            
            if not completed_agents:
                raise ValueError("No completed CSV agent results for triangulation")
            
            # Get direct PNS specifications
            pns_specs = state.get("pns_processed_specs", [])
            pns_processing_error = state.get("pns_processing_error", "")
            
            # Prepare datasets for triangulation prompt
            datasets = []
            all_dataset_outputs = {}
            
            # Add CSV agent results
            for source, result in completed_agents.items():
                dataset_info = {
                    "source": source,
                    "type": result["source_type"],
                    "rows_processed": result["raw_data_count"],
                    "extracted_specs": result["extracted_specs"]
                }
                datasets.append(dataset_info)
                all_dataset_outputs[source] = result["extracted_specs"]
            
            # Add PNS specifications directly (if available)
            if pns_specs and not pns_processing_error:
                # Format PNS specs for triangulation
                pns_formatted = "# PNS SPECIFICATIONS (Direct from JSON)\n"
                pns_formatted += "Rank,Specification,Options,Frequency,Status,Priority\n"
                
                for i, spec in enumerate(pns_specs, 1):
                    pns_formatted += f"{i},{spec.get('spec_name', 'N/A')},{spec.get('option', 'N/A')},{spec.get('frequency', 'N/A')},{spec.get('spec_status', 'N/A')},{spec.get('importance_level', 'N/A')}\n"
                
                datasets.append({
                    "source": "pns_direct",
                    "type": "pns-json-direct",
                    "rows_processed": len(pns_specs),
                    "extracted_specs": pns_formatted
                })
                all_dataset_outputs["pns_direct"] = pns_formatted
                logger.info(f"Added {len(pns_specs)} direct PNS specifications to triangulation")
            else:
                logger.warning(f"PNS data not available for triangulation: {pns_processing_error}")
            
            # Execute triangulation with all available data
            triangulated_result, triangulated_table, processing_logs = self._triangulate_with_validation(
                product_name=state["product_name"],
                datasets=datasets,
                all_dataset_outputs=all_dataset_outputs
            )
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            logger.info(f"Triangulation with direct PNS specs completed in {processing_time:.2f}s")
            
            # Return only the keys this function should update
            return {
                "triangulated_result": triangulated_result,
                "triangulated_table": triangulated_table,
                "current_step": "completed",
                "progress_percentage": 100,
                "logs": processing_logs + [f"Triangulation completed successfully in {processing_time:.2f}s"]
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error during PNS validation: {error_msg}")
            
            # Return error state updates
            return {
                "current_step": "triangulation_failed",
                "logs": [f"PNS validation failed: {error_msg}"]
            }
    
    def _build_triangulation_prompt(self, product_name: str, datasets: List[Dict], all_dataset_outputs: Dict) -> str:
        """Build PNS-centric validation prompt with strict 10-option limit"""
        
        # Extract PNS data and CSV sources
        pns_direct = all_dataset_outputs.get("pns_direct", "")
        csv_sources = {k: v for k, v in all_dataset_outputs.items() if k != "pns_direct"}
        csv_source_names = list(csv_sources.keys())
        
        # Build CSV sources information
        csv_data_section = "\n=== CSV SOURCES DATA ===\n"
        for source, data in csv_sources.items():
            csv_data_section += f"\n--- {source.upper()} ---\n{data}\n"
        
        # Enhanced PNS-centric validation prompt with strict controls
        prompt = f"""<role>
You are a PNS validation specialist. Your task is to take PNS specifications as the base and validate them against CSV data sources for {product_name}, while aggregating exactly 10 options per specification using a strict prioritization system.

⚠️ CRITICAL SCORING RULE - YOU MUST FOLLOW THIS EXACTLY:
Before writing ANY score, count the "Yes" entries in that row: search_keywords + whatsapp_specs + rejection_comments + lms_chats
The score MUST equal this count. NO EXCEPTIONS.
</role>

<task>
PRIMARY RESPONSIBILITY: Use your AI intelligence to analyze the full PNS context and deliver exactly 5 unique, high-quality specifications.

1. INTELLIGENT SPECIFICATION SELECTION: Extract EXACTLY 5 UNIQUE PNS specifications by frequency 
   • Use full context to make smart decisions about duplicates and priorities
   • Handle semantic deduplication intelligently ("Power" vs "Power Rating" vs "Motor Power")
   • Choose the most business-relevant specifications

2. CONSERVATIVE SEMANTIC MATCHING: Check if each spec appears in CSV sources
   • When uncertain about presence, mark as "No" (avoid false positives)
   • Use business context to make matching decisions

3. PRECISE OPTION AGGREGATION: Exactly 10 options per specification
   • Prioritize common options first, then PNS-only options
   • Use semantic intelligence to identify truly common options

4. MANDATORY SCORING PROCEDURE - FOLLOW THIS STEP-BY-STEP:
   • STEP 1: For each row, count "Yes" in: search_keywords + whatsapp_specs + rejection_comments + lms_chats
   • STEP 2: Write that exact number as the Score
   • STEP 3: Double-check - Score must equal "Yes" count
   • Examples: 2 "Yes" = Score 2, 1 "Yes" = Score 1, 0 "Yes" = Score 0
   • NO EXCEPTIONS: Score must match "Yes" count exactly
   • When uncertain about presence, ALWAYS mark "No" (conservative approach)

5. QUALITY ASSURANCE: Final verification before output
   • Ensure all 5 specifications have different names
   • Verify option counts and score accuracy
</task>

<intelligent_deduplication_approach>
TRUST YOUR AI CAPABILITIES - You have the full context to make smart decisions:
• Analyze complete PNS data to understand specification relationships
• Identify semantic duplicates that simple text matching would miss
• Prioritize specifications based on business importance and frequency
• Make nuanced decisions about what constitutes a "duplicate"
• Use context to choose the best representation when duplicates exist

NOTE: A manual safety net will catch any exact duplicates you miss, so focus on intelligent semantic analysis.
</intelligent_deduplication_approach>

<pns_base_data>
{pns_direct}
</pns_base_data>

{csv_data_section}

<strict_option_prioritization>
For each PNS specification, follow this EXACT prioritization process:

STEP 1 - IDENTIFY COMMON OPTIONS:
• Find options that appear in BOTH PNS data AND at least one CSV source (use semantic matching)
• Examples of semantic matching: "5KVA" = "5 KVA" = "5.0KVA", "Steel" = "steel", "Single Phase" = "1-phase"

STEP 2 - PRIORITIZE COMMON OPTIONS FIRST:
• If 10 or more common options exist → Take first 10 common options ONLY
• If fewer than 10 common options → Take ALL common options, then fill remaining slots with PNS-only options

STEP 3 - FILL WITH PNS-ONLY OPTIONS:
• Add PNS-only options (those that don't appear in any CSV source) to reach exactly 10 total options
• Select PNS-only options by frequency order (highest frequency first)

STEP 4 - ENFORCE 10-OPTION LIMIT:
• ALWAYS show exactly 10 options per specification
• If combined common + PNS-only options exceed 10 → Truncate to exactly 10
• If total available options < 10 → Show all available options, but note this should be rare
</strict_option_prioritization>

<validation_rules>
1. SELECT TOP 5 UNIQUE PNS SPECIFICATIONS:
   • Choose the 5 most frequent PNS specifications
   • ABSOLUTELY NO DUPLICATES - check each spec name before adding
   • Use their exact names from PNS data (first occurrence only for duplicates)

2. CONSERVATIVE SEMANTIC MATCHING WITH CSV SOURCES:
   • "Power" = "Motor Power" = "Power Rating" = "Power Output" = "KVA"
   • "Size" = "Grinding Size" = "Chamber Size" = "Dimensions"
   • "Material" = "Body Material" = "Construction Material"
   • "Capacity" = "Grinding Capacity" = "Output Capacity"
   • "Phase" = "Phase Configuration" = "Electrical Phase"
   • CONSERVATIVE RULE: When in doubt, mark as "No" to avoid false positives

3. ULTRA-STRICT SCORING SYSTEM:
   • Score = EXACT number of CSV sources where spec appears (0-4)
   • ONLY mark "Yes" if you can clearly find the specification in that CSV source
   • CONSERVATIVE APPROACH: If uncertain, mark as "No" 
   • Score must EXACTLY equal count of "Yes" entries in that row
   • Triple-check each source before marking "Yes"

4. RANKING:
   • Primary: Score (descending) - higher score = higher rank
   • Secondary: PNS frequency (descending)
</validation_rules>

<csv_sources_to_check>
The 4 CSV sources to validate against:
- search_keywords
- whatsapp_specs  
- rejection_comments
- lms_chats
</csv_sources_to_check>

<output_requirements>
Create a PNS validation table with EXACTLY this format:

| Score | PNS | Options | search_keywords | whatsapp_specs | rejection_comments | lms_chats |

CRITICAL REQUIREMENTS:
1. Score: Number (0-4) indicating how many CSV sources contain this PNS spec
2. PNS: PNS specification name only (no options or frequency data)
3. Options: EXACTLY 10 comma-separated options following strict prioritization (common first, then PNS-only)
4. CSV Columns: "Yes" if spec appears in that source, "No" if not
5. Order by Score (descending), then by PNS frequency (descending)
6. Show EXACTLY 5 rows (top 5 UNIQUE PNS specs) - NO MORE, NO LESS

DEDUPLICATION VALIDATION:
• ZERO TOLERANCE for duplicate specifications in final output
• Each of the 5 PNS specifications must have a DIFFERENT name
• Check: Brand ≠ Brand, Color ≠ Color, Weight ≠ Weight, etc.
• If you find duplicates, select only the first occurrence and find another unique spec

OPTION SELECTION VALIDATION:
• Count must be exactly 10 options per specification
• Common options (PNS + CSV) must appear first
• PNS-only options fill remaining slots
• Use semantic matching to identify common options
• No duplicates in final option list

ULTRA-CONSERVATIVE SCORING:
• ONLY mark "Yes" if you can clearly identify the PNS spec in that CSV source
• When uncertain, ALWAYS mark as "No" (better false negative than false positive)
• Score must EXACTLY equal the count of "Yes" entries in that row
• If BLNI/rejection_comments data is not provided, mark as "No"

FINAL VALIDATION CHECKLIST:
□ Exactly 5 rows in output table
□ All 5 PNS specification names are different (no duplicates)
□ Each row has exactly 10 options
□ MANDATORY SCORING: Count "Yes" entries and write that number as Score
□ STEP-BY-STEP: Count "Yes" in search_keywords + whatsapp_specs + rejection_comments + lms_chats = Score
□ Examples: 2 "Yes" = Score 2, 1 "Yes" = Score 1, 0 "Yes" = Score 0
□ Conservative "Yes/No" decisions (when in doubt, choose "No")

CRITICAL INSTRUCTIONS - HYBRID APPROACH:
• Use your AI intelligence for semantic analysis and smart decisions
• EXACTLY 5 unique PNS specs - leverage your contextual understanding
• EXACTLY 10 options per specification - no more, no less
• MANDATORY SCORING: For each row, count "Yes" entries and write that number as Score
• STEP-BY-STEP: Count "Yes" in search_keywords + whatsapp_specs + rejection_comments + lms_chats = Score
• Examples: 2 "Yes" entries = Score 2, 1 "Yes" entry = Score 1, 0 "Yes" entries = Score 0
• Conservative approach: When uncertain about presence, ALWAYS mark "No"
• Focus on quality over perfection - manual safety net will catch exact duplicates if needed
• If PNS has no specifications, respond with "PNS has no specifications"
</output_requirements>

<example_output>
| Score | PNS | Options | search_keywords | whatsapp_specs | rejection_comments | lms_chats |
| 4 | Power Rating | 5KVA, 10KVA, 15KVA, 20KVA, 25KVA, 30KVA, 7.5KVA, 12.5KVA, 1KVA, 2KVA | Yes | Yes | Yes | Yes |
| 2 | Brand | Chand Tara, Meenakshi, Cat, Shivani, Tiger, Jeevan, Cycle, Kangaroo, Chandni, Eagle | Yes | No | No | Yes |
| 1 | Weight | 2kg, 3kg, 5kg, 10kg, 15kg, 20kg, 25kg, 30kg, 35kg, 40kg | No | Yes | No | No |
| 0 | Color | White, Red, Black, Golden, Green, Brown, Yellow, Blue, Pink, Orange | No | No | No | No |
| 3 | Material | Steel, Aluminum, Cast Iron, Carbon Steel, Stainless Steel, Mild Steel, Iron, Copper, Brass, Bronze | Yes | Yes | No | Yes |
| 2 | Size | 10mm, 15mm, 20mm, 25mm, 30mm, 12mm, 18mm, 22mm, 8mm, 35mm | No | Yes | Yes | No |
| 1 | Phase | Single Phase, Three Phase, DC, AC, 1-Phase, 3-Phase, Mono Phase, Poly Phase, Two Phase, Multi Phase | Yes | No | No | No |
| 0 | Capacity | 100kg/hr, 200kg/hr, 300kg/hr, 150kg/hr, 250kg/hr, 50kg/hr, 400kg/hr, 500kg/hr, 75kg/hr, 125kg/hr | No | No | No | No |
</example_output>
"""
        
        return prompt
    

    
    def _triangulate_with_validation(self, product_name: str, datasets: List[Dict], all_dataset_outputs: Dict) -> tuple:
        """Perform triangulation with 2-layer validation system"""
        processing_logs = []
        
        # First attempt - main triangulation (LLM handles deduplication intelligently)
        logger.info("Layer 0: Starting main triangulation with full context")
        processing_logs.append("🔄 Starting main PNS triangulation (LLM intelligence first)")
        
        prompt = self._build_triangulation_prompt(product_name, datasets, all_dataset_outputs)
        response = self.llm.invoke([HumanMessage(content=prompt)])
        triangulated_result = response.content
        triangulated_table = self._parse_triangulation_result(triangulated_result)
        
        logger.info(f"Main triangulation completed. Parsed {len(triangulated_table)} specs")
        
        # Layer 1: Gap identification (always runs)
        logger.info("Layer 1: Starting gap identification validation")
        processing_logs.append("🔍 Layer 1: Validating triangulation results")
        
        validation_result = self._validate_triangulation_result(
            triangulated_result, triangulated_table, product_name, all_dataset_outputs
        )
        
        logger.info(f"Validation result: Valid={validation_result['is_valid']}, Issues={len(validation_result['issues'])}")
        
        if validation_result["is_valid"]:
            logger.info("Validation passed - using main triangulation result")
            processing_logs.append("✅ Layer 1: Validation passed - no issues found")
            return triangulated_result, triangulated_table, processing_logs
        
        # Layer 2: Correction with feedback (runs only if issues found)
        logger.info(f"Validation failed with {len(validation_result['issues'])} issues. Starting Layer 2 correction")
        processing_logs.append(f"⚠️  Layer 1: Found {len(validation_result['issues'])} issues. Starting Layer 2 correction")
        
        for issue in validation_result['issues']:
            logger.info(f"Validation issue: {issue}")
        
        retry_prompt = self._build_retry_prompt(
            product_name, datasets, all_dataset_outputs, 
            first_attempt=triangulated_result,
            validation_issues=validation_result['issues']
        )
        
        try:
            logger.info("Layer 2: Sending correction request to LLM")
            retry_response = self.llm.invoke([HumanMessage(content=retry_prompt)])
            retry_result = retry_response.content
            retry_table = self._parse_triangulation_result(retry_result)
            
            logger.info(f"Layer 2: Correction completed with {len(retry_table)} specs")
            processing_logs.append("🔧 Layer 2: Correction completed - using improved result")
            
            return retry_result, retry_table, processing_logs
            
        except Exception as e:
            logger.error(f"Layer 2 correction failed: {str(e)}")
            processing_logs.append(f"❌ Layer 2: Correction failed ({str(e)}) - using original result")
            
            # Fallback to original result
            return triangulated_result, triangulated_table, processing_logs
    
    def _validate_triangulation_result(self, result: str, table: List[Dict], product_name: str, all_dataset_outputs: Dict) -> Dict[str, Any]:
        """Layer 1: Gap identification - validate triangulation result for issues"""
        
        try:
            logger.info("Building validation prompt for gap identification")
            validation_prompt = self._build_validation_prompt(result, table, product_name, all_dataset_outputs)
            
            logger.info("Sending validation request to LLM")
            response = self.llm.invoke([HumanMessage(content=validation_prompt)])
            validation_response = response.content
            
            logger.info("Parsing validation response")
            return self._parse_validation_response(validation_response)
            
        except Exception as e:
            logger.error(f"Validation layer failed: {str(e)}")
            return {
                "is_valid": False,
                "issues": [f"Validation system error: {str(e)}"],
                "summary": f"Validation failed due to error: {str(e)}"
            }
    
    def _build_validation_prompt(self, result: str, table: List[Dict], product_name: str, all_dataset_outputs: Dict) -> str:
        """Build validation prompt for gap identification"""
        
        # Extract CSV sources data for validation reference
        csv_sources = {k: v for k, v in all_dataset_outputs.items() if k != "pns_direct"}
        pns_direct = all_dataset_outputs.get("pns_direct", "")
        
        # Build source data section for reference
        source_data_section = f"\n=== PNS DATA ===\n{pns_direct}\n"
        for source, data in csv_sources.items():
            source_data_section += f"\n=== {source.upper()} DATA ===\n{data}\n"
        
        # Build table summary for validation
        table_summary = "\n=== TRIANGULATION RESULT TO VALIDATE ===\n"
        table_summary += "Current table:\n"
        for row in table:
            options_count = len(row.get('Options', '').split(',')) if row.get('Options', '') else 0
            table_summary += f"Spec: {row.get('PNS', 'N/A')}, Score: {row.get('Score', 'N/A')}, Options: {options_count}, CSV Sources: {row.get('search_keywords', 'N/A')}/{row.get('whatsapp_specs', 'N/A')}/{row.get('rejection_comments', 'N/A')}/{row.get('lms_chats', 'N/A')}\n"
        
        prompt = f"""<role>
You are a triangulation validation expert. Your job is to identify gaps and issues in PNS triangulation results to ensure accuracy and compliance with requirements.
</role>

<task>
Validate this triangulation result for {product_name} and identify ALL issues that need correction.
</task>

        <validation_checklist>
Check for these CRITICAL issues:

1. SPECIFICATION COUNT VALIDATION:
   • EXACTLY 5 specifications must be present - no more, no less
   • Flag if count ≠ 5 specifications

2. DUPLICATE SPECIFICATIONS VALIDATION:
   • ZERO TOLERANCE for duplicate specification names
   • Check for exact duplicates: "Brand" & "Brand", "Color" & "Color"
   • Check for case variations: "brand" & "Brand" 
   • Flag ANY duplicate specification names found

3. OPTIONS COUNT VALIDATION:
   • Each specification must have EXACTLY 10 options
   • Count the comma-separated options in each row
   • Flag any row with ≠ 10 options

4. ULTRA-STRICT SCORING ACCURACY VALIDATION:
   • CRITICAL: Score MUST equal exact count of "Yes" entries in that row
   • Count "Yes" in: search_keywords + whatsapp_specs + rejection_comments + lms_chats
   • Examples: 2 "Yes" entries = Score 2, 1 "Yes" entry = Score 1, 0 "Yes" entries = Score 0
   • Flag ANY score mismatch: Score=3 but only 2 "Yes" entries = ERROR
   • Check each CSV source column against actual source data
   • Flag false positives: "Yes" marked but spec not found in source

5. CONSERVATIVE SEMANTIC MATCHING VALIDATION:
   • Only "Yes" if specification clearly appears in that CSV source
   • Flag suspicious "Yes" decisions where spec presence is unclear
   • Check for false positives in sources that weren't provided (e.g., BLNI data)

6. OPTIONS PRIORITIZATION VALIDATION:
   • Common options (appearing in both PNS and CSV) should be listed first
   • PNS-only options should fill remaining slots
   • Verify semantic matching was used correctly

7. DATA CONSISTENCY VALIDATION:
   • Proper ranking by score (descending) then frequency
   • No missing or corrupted data fields
</validation_checklist>

<source_data_for_reference>
{source_data_section}
</source_data_for_reference>

{table_summary}

        <validation_instructions>
Perform these MANDATORY checks in order:

STEP 1 - COUNT VALIDATION:
• Count total specifications (must be exactly 5)
• Count options in each row (must be exactly 10 per spec)

STEP 2 - DUPLICATE DETECTION:
• List all PNS specification names
• Check for exact matches and case variations
• Flag any duplicates found

STEP 3 - SCORING ACCURACY CHECK:
• For each row, count "Yes" entries across all CSV columns
• Verify count matches the Score value exactly
• Flag any mismatches

STEP 4 - FALSE POSITIVE DETECTION:
• For each "Yes" entry, verify the PNS spec actually appears in that specific CSV source
• Be extra careful with sources that may not have been provided
• Flag suspicious "Yes" decisions

STEP 5 - OPTIONS PRIORITIZATION:
• Check if common options appear first, then PNS-only options
• Verify semantic matching was used correctly

Provide specific, actionable feedback for each issue found.
</validation_instructions>

<output_format>
VALIDATION_RESULT: PASS/FAIL

ISSUES_FOUND:
[List each specific issue with spec name and problem description]

If no issues: "No issues found - validation passed"
If issues found: List each issue with specific details

Example issues:
- "CRITICAL: Found 7 specifications instead of exactly 5"
- "CRITICAL: Duplicate specifications found - 'Brand' appears in rows 1 and 2"
- "Color: Only 8 options provided, need exactly 10"
- "Brand: Score is 0 but shows Yes/Yes/Yes (3 sources) - major score mismatch"
- "Weight: Marked Yes for rejection_comments but no BLNI data was provided"
- "Size: Marked Yes for lms_chats but 'Size' not found in lms_chats data"
- "Phase: Options not prioritized correctly - PNS-only options appear before common options"
</output_format>

CRITICAL: Be thorough and specific. Identify ALL issues to ensure accurate correction in Layer 2."""
        
        return prompt
    
    def _parse_validation_response(self, validation_response: str) -> Dict[str, Any]:
        """Parse validation response to extract issues"""
        try:
            # Check if validation passed
            is_valid = "VALIDATION_RESULT: PASS" in validation_response
            
            # Extract issues
            issues = []
            lines = validation_response.split('\n')
            
            in_issues_section = False
            for line in lines:
                line = line.strip()
                
                if "ISSUES_FOUND:" in line:
                    in_issues_section = True
                    continue
                
                if in_issues_section and line:
                    # Skip empty lines and section headers
                    if not line.startswith("If no issues") and not line.startswith("If issues") and not line.startswith("Example"):
                        if line.startswith("- ") or line.startswith("•"):
                            issues.append(line[2:].strip())  # Remove bullet point
                        elif line and not line.startswith("VALIDATION_RESULT") and "No issues found" not in line:
                            issues.append(line)
            
            # If marked as failed but no specific issues found, add general issue
            if not is_valid and not issues:
                issues.append("Validation failed but no specific issues identified")
            
            summary = "No issues found" if is_valid else f"{len(issues)} issues identified"
            
            logger.info(f"Validation parsing result: Valid={is_valid}, Issues={len(issues)}")
            for issue in issues:
                logger.info(f"Issue: {issue}")
            
            return {
                "is_valid": is_valid,
                "issues": issues,
                "summary": summary,
                "raw_response": validation_response
            }
            
        except Exception as e:
            logger.error(f"Error parsing validation response: {e}")
            return {
                "is_valid": False,
                "issues": [f"Validation parsing error: {str(e)}"],
                "summary": f"Validation parsing failed: {str(e)}",
                "raw_response": validation_response
            }
    
    def _build_retry_prompt(self, product_name: str, datasets: List[Dict], all_dataset_outputs: Dict, first_attempt: str, validation_issues: List[str]) -> str:
        """Build retry prompt with validation feedback for Layer 2 correction"""
        
        # Extract PNS data and CSV sources
        pns_direct = all_dataset_outputs.get("pns_direct", "")
        csv_sources = {k: v for k, v in all_dataset_outputs.items() if k != "pns_direct"}
        
        # Build CSV sources information
        csv_data_section = "\n=== CSV SOURCES DATA FOR REFERENCE ===\n"
        for source, data in csv_sources.items():
            csv_data_section += f"\n--- {source.upper()} ---\n{data}\n"
        
        # Build validation feedback section
        validation_feedback = "\n=== VALIDATION ISSUES FROM LAYER 1 ===\n"
        for i, issue in enumerate(validation_issues, 1):
            validation_feedback += f"{i}. {issue}\n"
        
        prompt = f"""<role>
You are a PNS validation specialist performing Layer 2 correction. Your first attempt had validation issues that need to be fixed using specific feedback.
</role>

<task>
Create a CORRECTED PNS validation table for {product_name} that addresses ALL validation issues identified in Layer 1.
</task>

<critical_corrections_needed>
Your first attempt had these specific issues that MUST be fixed:
{validation_feedback}

You MUST address each issue above in your corrected response.
</critical_corrections_needed>

<pns_base_data>
{pns_direct}
</pns_base_data>

{csv_data_section}

<first_attempt_with_issues>
{first_attempt}
</first_attempt_with_issues>

<correction_guidelines>
Based on the validation feedback, apply these CRITICAL corrections:

1. SPECIFICATION COUNT CORRECTION:
   • Show EXACTLY 5 specifications - no more, no less
   • If you have more than 5, select top 5 by frequency
   • If you have fewer than 5, add more from PNS data

2. DUPLICATE ELIMINATION CORRECTION:
   • ABSOLUTELY NO duplicate specification names
   • Check for: "Brand" & "Brand", "Color" & "Color", etc.
   • If duplicates found, keep only the first occurrence and replace with different spec
   • Ensure all 5 specs have DIFFERENT names

3. OPTIONS COUNT CORRECTION:
   • Ensure EXACTLY 10 options per specification
   • Count carefully and adjust as needed

4. ULTRA-CONSERVATIVE SCORING CORRECTION:
   • CRITICAL: Score MUST equal exact count of "Yes" entries in that row
   • Count "Yes" in: search_keywords + whatsapp_specs + rejection_comments + lms_chats
   • Examples: 2 "Yes" entries = Score 2, 1 "Yes" entry = Score 1, 0 "Yes" entries = Score 0
   • ONLY mark "Yes" if you can clearly find the spec in that CSV source
   • When uncertain, ALWAYS mark "No" (avoid false positives)
   • If BLNI/rejection_comments data wasn't provided, mark as "No"

5. OPTIONS PRIORITIZATION CORRECTION:
   • First: Options that appear in BOTH PNS and at least one CSV source (common options)
   • Then: PNS-only options to fill remaining slots
   • Use semantic matching: "5KVA" = "5 KVA", "Steel" = "steel"

6. SEMANTIC MATCHING CORRECTION:
   • Only mark "Yes" if the PNS spec semantically appears in the CSV source
   • Examples: "Power" matches "KVA", "Motor Power", "Power Rating"
   • Be extra conservative - verify actual presence

7. DATA CONSISTENCY CORRECTION:
   • Maintain proper ranking by score (descending)
   • Ensure all data fields are complete
</correction_guidelines>

<output_requirements>
Create the CORRECTED PNS validation table with EXACTLY this format:

| Score | PNS | Options | search_keywords | whatsapp_specs | rejection_comments | lms_chats |

CORRECTION REQUIREMENTS:
• Address ALL validation issues from Layer 1
• EXACTLY 5 unique specifications (no more, no less)
• ZERO duplicate specification names
• EXACTLY 10 options per specification (count carefully)
• Score = exact count of "Yes" entries in that row
• Ultra-conservative "Yes/No" decisions (when unsure, choose "No")
• Prioritize common options first, then PNS-only options

MANDATORY VERIFICATION CHECKLIST:
□ Exactly 5 rows in the table
□ All 5 PNS specification names are DIFFERENT (no duplicates)
□ Each spec has exactly 10 comma-separated options
□ Score equals count of "Yes" entries in same row
□ Each "Yes" is justified by clear presence in CSV source
□ Conservative approach used (false negatives better than false positives)
□ Options are prioritized correctly (common first, then PNS-only)
□ All validation issues from Layer 1 are addressed
</output_requirements>

CRITICAL: This is your corrected attempt. Address every validation issue identified in Layer 1 to produce an accurate result."""
        
        return prompt
    
    def _parse_triangulation_result(self, result: str) -> List[Dict[str, Any]]:
        """Parse PNS validation result into structured table format for export"""
        try:
            lines = result.strip().split('\n')
            table_data = []
            rank = 1
            
            # Debug: log each line being processed
            logger.info(f"Processing {len(lines)} lines for PNS validation parsing")
            
            # Check for "PNS has no specifications" message
            if "PNS has no specifications" in result:
                logger.info("PNS has no specifications in the result")
                return [{
                    'Rank': 1,
                    'Score': 'N/A',
                    'PNS': 'No PNS Specifications',
                    'Options': 'N/A',
                    'search_keywords': 'N/A',
                    'whatsapp_specs': 'N/A',
                    'rejection_comments': 'N/A',
                    'lms_chats': 'N/A'
                }]
            
            # Look for table format in the result
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Debug: log the line being processed
                logger.info(f"Line {i}: '{line}' - Pipe count: {line.count('|')}")
                
                # Skip empty lines, headers, and separator lines
                if not line:
                    continue
                if 'Score' in line and 'PNS' in line and 'search_keywords' in line:
                    continue
                if line.startswith('|--') or line.startswith('|-'):
                    continue
                if line.count('|') < 6:  # Need at least 7 columns (score, pns, options, search_keywords, whatsapp_specs, rejection_comments, lms_chats)
                    continue
                
                # Look for table rows (containing | separator)
                if '|' in line:
                    # Clean up the line
                    cleaned_line = line
                    if cleaned_line.startswith('|'):
                        cleaned_line = cleaned_line[1:]
                    if cleaned_line.endswith('|'):
                        cleaned_line = cleaned_line[:-1]
                    
                    parts = [part.strip() for part in cleaned_line.split('|')]
                    
                    # Debug: log the parts
                    logger.info(f"Parsed parts: {parts} (count: {len(parts)})")
                    
                    # Ensure we have exactly 7 parts (score, pns, options, search_keywords, whatsapp_specs, rejection_comments, lms_chats)
                    if len(parts) >= 7:
                        # Convert score to integer for sorting, handle non-numeric scores
                        try:
                            score_value = int(parts[0])
                        except (ValueError, TypeError):
                            score_value = 0
                            
                        table_data.append({
                            'Rank': rank,
                            'Score': parts[0],
                            'PNS': parts[1],
                            'Options': parts[2],
                            'search_keywords': parts[3],
                            'whatsapp_specs': parts[4],
                            'rejection_comments': parts[5],
                            'lms_chats': parts[6],
                            '_score_value': score_value  # For sorting
                        })
                        rank += 1
                        logger.info(f"Successfully added PNS validation row {rank-1}: {parts[1]} with score: {parts[0]} and options: {parts[2]}")
            
            # Debug log
            logger.info(f"Successfully parsed {len(table_data)} PNS validation table rows from LLM")
            
            # SAFETY NET: Single comprehensive validation and correction loop
            if table_data:
                logger.info("🛡️  SAFETY NET: Applying comprehensive validation and correction")
                
                # Single loop for all safety net operations
                seen_specs = set()
                validated_data = []
                duplicates_removed = 0
                scoring_corrections = 0
                
                for item in table_data:
                    spec_name = item['PNS'].strip().lower()
                    
                    # Check for duplicates
                    if spec_name in seen_specs:
                        logger.warning(f"🚨 Safety net: Removing duplicate spec '{item['PNS']}'")
                        duplicates_removed += 1
                        continue
                    
                    seen_specs.add(spec_name)
                    
                    # Validate and correct scoring
                    yes_count = sum([
                        1 if item.get('search_keywords', '').strip().lower() == 'yes' else 0,
                        1 if item.get('whatsapp_specs', '').strip().lower() == 'yes' else 0,
                        1 if item.get('rejection_comments', '').strip().lower() == 'yes' else 0,
                        1 if item.get('lms_chats', '').strip().lower() == 'yes' else 0
                    ])
                    
                    current_score = item.get('Score', '0')
                    if str(yes_count) != current_score:
                        logger.warning(f"🔧 Safety net: Correcting '{item['PNS']}' score from {current_score} to {yes_count}")
                        item['Score'] = str(yes_count)
                        scoring_corrections += 1
                    else:
                        logger.info(f"✅ Safety net: Correct scoring for '{item['PNS']}' - Score={yes_count}")
                    
                    validated_data.append(item)
                
                table_data = validated_data
                
                # Log safety net results
                if duplicates_removed > 0:
                    logger.warning(f"🚨 Safety net: Removed {duplicates_removed} duplicate specs")
                else:
                    logger.info("✅ Safety net: No duplicates detected")
                
                if scoring_corrections > 0:
                    logger.warning(f"🔧 Safety net: Corrected {scoring_corrections} scoring errors")
                else:
                    logger.info("✅ Safety net: All scoring was accurate")
                
                # Sort by score value (descending)
                table_data.sort(key=lambda x: x.get('_score_value', 0), reverse=True)
                
                # Enforce exactly 5 specifications limit
                if len(table_data) > 5:
                    logger.warning(f"🚨 Safety net: Truncating from {len(table_data)} to 5 specifications")
                    table_data = table_data[:5]
                elif len(table_data) < 5:
                    logger.warning(f"⚠️  Safety net: Only {len(table_data)} unique specs available (less than target of 5)")
                else:
                    logger.info("✅ Safety net: Exactly 5 specifications")
                
                # Update ranks and remove the temporary sorting field
                for new_rank, item in enumerate(table_data, 1):
                    item['Rank'] = new_rank
                    if '_score_value' in item:
                        del item['_score_value']
                    logger.info(f"PNS Rank {new_rank}: '{item['PNS']}' (Score: {item['Score']})")
                
                logger.info(f"🎯 Hybrid approach completed: {len(table_data)} unique specs (LLM intelligence + efficient safety net)")
            
            return table_data
            
        except Exception as e:
            logger.error(f"Error parsing PNS validation result: {e}")
            # Return a fallback structure for the new 7-column format
            return [{
                'Rank': 1,
                'Score': 'Error',
                'PNS': 'Parse Error',
                'Options': 'N/A',
                'search_keywords': 'N/A',
                'whatsapp_specs': 'N/A',
                'rejection_comments': 'N/A',
                'lms_chats': 'N/A'
            }]
    





def triangulate_all_results(state: SpecExtractionState) -> SpecExtractionState:
    """LangGraph node function for PNS-centric validation"""
    agent = TriangulationAgent()
    return agent.triangulate_results(state)

# COMMENTED OUT - Meta-ensemble triangulation no longer used
# def meta_ensemble_triangulate(state: SpecExtractionState) -> SpecExtractionState:
#     """LangGraph node function for meta-ensemble triangulation"""
#     agent = MetaEnsembleAgent()
#     return agent.ensemble_triangulate(state)

class FinalTriangulationAgent:
    """Agent for performing final triangulation between CSV results and PNS specs"""
    
    def __init__(self):
        self.llm = ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            temperature=0.1,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
    
    def final_triangulate(self, state: SpecExtractionState) -> SpecExtractionState:
        """Perform final triangulation between CSV triangulated result and PNS specs with validation"""
        start_time = time.time()
        
        try:
            logger.info("Starting final triangulation between CSV results and PNS specs")
            
            csv_result = state.get("triangulated_result", "")
            pns_specs = state.get("pns_processed_specs", [])
            
            if not csv_result and not pns_specs:
                raise ValueError("No data available for final triangulation")
            
            # Attempt final triangulation with validation and single retry
            final_result, final_table, processing_logs = self._triangulate_with_validation(
                product_name=state["product_name"],
                csv_result=csv_result,
                pns_specs=pns_specs
            )
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            logger.info(f"Final triangulation with validation completed in {processing_time:.2f}s")
            
            return {
                "final_triangulated_result": final_result,
                "final_triangulated_table": final_table,
                "current_step": "final_triangulation_completed",
                "progress_percentage": 100,
                "logs": processing_logs + [f"Final triangulation completed successfully in {processing_time:.2f}s"]
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error during final triangulation: {error_msg}")
            
            return {
                "current_step": "final_triangulation_failed",
                "logs": [f"Final triangulation failed: {error_msg}"]
            }
    
    def _triangulate_with_validation(self, product_name: str, csv_result: str, pns_specs: List[Dict[str, Any]]) -> tuple:
        """Perform final triangulation with validation and single retry"""
        processing_logs = []
        
        # First attempt
        logger.info("First triangulation attempt")
        processing_logs.append("Starting final triangulation (1st attempt)")
        
        prompt = self._build_final_triangulation_prompt(product_name, csv_result, pns_specs)
        response = self.llm.invoke([HumanMessage(content=prompt)])
        final_result = response.content
        final_table = self._parse_final_triangulation_result(final_result)
        
        # Validate the result
        logger.info("Validating triangulation result")
        processing_logs.append("Validating triangulation result")
        
        validation_result = self._validate_final_result(final_result, csv_result, pns_specs, product_name)
        logger.debug(f"Validation result: {validation_result}")
        
        if validation_result["is_valid"]:
            logger.info("Validation passed - using first attempt result")
            processing_logs.append("✅ Validation passed - final triangulation successful")
            return final_result, final_table, processing_logs
        
        # Validation failed - retry once with feedback
        logger.info(f"Validation failed: {validation_result['errors']}. Retrying with feedback.")
        processing_logs.append(f"⚠️ Validation failed: {validation_result['summary']}. Retrying...")
        
        retry_prompt = self._build_retry_prompt(
            product_name, csv_result, pns_specs, 
            first_attempt=final_result, 
            validation_errors=validation_result['errors']
        )
        
        try:
            retry_response = self.llm.invoke([HumanMessage(content=retry_prompt)])
            retry_result = retry_response.content
            retry_table = self._parse_final_triangulation_result(retry_result)
            
            logger.info("Retry attempt completed - using retry result")
            processing_logs.append("🔄 Retry completed - using corrected result")
            
            return retry_result, retry_table, processing_logs
            
        except Exception as e:
            logger.error(f"Retry attempt failed: {str(e)}")
            processing_logs.append(f"❌ Retry failed: {str(e)} - using original result")
            
            # Return original result if retry fails
            return final_result, final_table, processing_logs
    
    def _build_final_triangulation_prompt(self, product_name: str, csv_result: str, pns_specs: List[Dict[str, Any]]) -> str:
        """Build prompt for final triangulation between CSV and PNS data"""
        
        # Convert both sources to standardized format for consistent LLM processing
        csv_structured = self._parse_csv_to_structured_format(csv_result)
        pns_structured = self._parse_pns_to_structured_format(pns_specs)
        
        # Prepare standardized CSV data
        csv_data = "\n=== CSV TRIANGULATED SPECIFICATIONS ===\n"
        if csv_structured:
            for i, spec in enumerate(csv_structured, 1):
                csv_data += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Source: CSV\n"
        else:
            csv_data += "No CSV specifications available\n"
        
        # Prepare standardized PNS data
        pns_direct = "\n=== PNS EXTRACTED SPECIFICATIONS ===\n"
        if pns_structured:
            for i, spec in enumerate(pns_structured, 1):
                pns_direct += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Freq: {spec['frequency']} | Status: {spec['status']} | Priority: {spec['priority']} | Source: PNS\n"
        else:
            pns_direct += "No PNS specifications available\n"
        
        prompt = f"""<role>
You are a final consensus specialist identifying specifications that are AGREED UPON by both CSV data sources and PNS expert analysis for {product_name}.
</role>

<task>
Create the final CONSENSUS specification table showing ONLY specifications that appear in BOTH CSV and PNS data sources. This represents true market agreement.
</task>

<consensus_methodology>
Apply this strict consensus process:

STEP 1 - IDENTIFY OVERLAPS ONLY:
• CSV Results: Frequency-based specifications from multiple data sources  
• PNS Specs: Expert-validated specifications with frequency, status, and priority data
• ONLY include specifications that exist in BOTH sources (semantic matching allowed)
• Use PNS frequency and priority data to guide selection when multiple options exist

STEP 2 - SEMANTIC MATCHING:
• Match similar specifications: "Power" = "Motor Power" = "Power Rating"
• Match similar specifications: "Size" = "Grinding Size" = "Chamber Size" 
• Match similar specifications: "Capacity" = "Grinding Capacity" = "Output"
• Use professional judgment for specification equivalence

STEP 3 - CONSENSUS VALIDATION:
• If a specification appears in both sources → INCLUDE IT
• If a specification appears in only CSV → EXCLUDE IT  
• If a specification appears in only PNS → EXCLUDE IT
• Prefer PNS naming and option values for included specs

STEP 4 - FINAL RANKING:
• Rank consensus specs by: 1) PNS priority, 2) Combined frequency/confidence
• If NO common specs found, return "No consensus specifications found"
</consensus_methodology>

<data_sources>
{csv_data}
{pns_direct}
</data_sources>

<consensus_rules>
STRICT INCLUSION CRITERIA:
• Specification MUST appear semantically in both CSV and PNS data
• ALWAYS use PNS specification names for consensus specs (PNS is pre-validated)
• Use PNS option values when both sources cover the same specification
• NO padding with unique specs from either source

SEMANTIC MATCHING EXAMPLES:
• "Power" (CSV) = "Motor Power" (PNS) → MATCH ✅
• "Size" (CSV) = "Size" (PNS) → MATCH ✅  
• "Capacity" (CSV) = "Grinding Capacity" (PNS) → MATCH ✅
• "Material" (CSV only) → EXCLUDE ❌
• "Phase" (PNS only) → EXCLUDE ❌
</consensus_rules>

<output_requirements>
Create the consensus specification table with EXACTLY this format:

| Specification Name | Top Options | Why it matters in the market | Impacts Pricing? |

Requirements:
1. Specification Name: Use PNS naming for matched specifications
2. Top Options: Prefer PNS option values, supplement with CSV if needed
3. Why it matters: Business justification for buyer decision-making  
4. Impacts Pricing: "✅ Yes" or "❌ No" based on market analysis

CRITICAL INSTRUCTIONS:
• ONLY show specifications that exist in BOTH data sources
• If only 1 consensus spec found, show only 1 row
• If 0 consensus specs found, state "No consensus specifications identified"
• Do NOT pad with unique specifications from either source
• Prefer PNS values and naming conventions for consensus specs
</output_requirements>

<final_validation>
Before submitting, ensure:
□ ONLY specifications appearing in both CSV and PNS data are included
□ If no common specifications exist, clearly state this
□ PNS naming and option values are used for consensus specs
□ Business justifications are specific to {product_name}
□ No padding with unique specifications from either source
□ Output matches the required table format exactly
</final_validation>"""
        
        return prompt
    
    def _validate_final_result(self, final_result: str, csv_result: str, pns_specs: List[Dict[str, Any]], product_name: str) -> Dict[str, Any]:
        """
        Validate final triangulation result to ensure only common specs with common options.
        
        Validation checks:
        1. Each spec exists semantically in both CSV and PNS data
        2. Each option exists in both matched CSV and PNS specifications  
        3. Specification names use PNS terminology (preferred)
        4. No extra specifications from only one source
        
        Returns: {"is_valid": bool, "summary": str, "errors": List[str], "correction_needed": str}
        """
        
        # Build validation prompt
        validation_prompt = self._build_validation_prompt(final_result, csv_result, pns_specs, product_name)
        
        logger.info("Sending validation request to LLM")
        
        # Get validation response
        response = self.llm.invoke([HumanMessage(content=validation_prompt)])
        validation_response = response.content
        
        # Parse validation response
        return self._parse_validation_response(validation_response)
    
    def _build_validation_prompt(self, final_result: str, csv_result: str, pns_specs: List[Dict[str, Any]], product_name: str) -> str:
        """Build validation prompt for checking final triangulation result"""
        
        # Convert both sources to standardized format for easier LLM comparison
        csv_structured = self._parse_csv_to_structured_format(csv_result)
        pns_structured = self._parse_pns_to_structured_format(pns_specs)
        
        # Prepare standardized CSV data
        csv_data = "\n=== CSV TRIANGULATED SPECIFICATIONS ===\n"
        if csv_structured:
            for i, spec in enumerate(csv_structured, 1):
                csv_data += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Source: CSV\n"
        else:
            csv_data += "No CSV specifications available\n"
        
        # Prepare standardized PNS data  
        pns_direct = "\n=== PNS EXTRACTED SPECIFICATIONS ===\n"
        if pns_structured:
            for i, spec in enumerate(pns_structured, 1):
                pns_direct += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Freq: {spec['frequency']} | Status: {spec['status']} | Priority: {spec['priority']} | Source: PNS\n"
        else:
            pns_direct += "No PNS specifications available\n"
        
        prompt = f"""<role>
You are a validation specialist checking if a final triangulation result is correct. Your job is to verify that ONLY specifications present in BOTH sources are included, with ONLY common options.
</role>

<task>
Validate this final triangulation result by checking each specification individually.
</task>

<validation_rules>
For each specification in the final result:
1. SEMANTIC MATCHING: The spec must exist in both CSV and PNS (names can differ but meaning should be similar)
2. COMMON OPTIONS ONLY: All options in final result must be present in BOTH the matched CSV spec AND matched PNS spec
3. PNS NAMING: Specification names should use PNS terminology (since PNS is pre-validated)
4. NO EXTRA SPECS: No specifications that don't exist in both sources
5. FREQUENCY CONSIDERATION: Higher frequency PNS options indicate greater market importance
</validation_rules>

<original_sources>
{csv_data}
{pns_direct}
</original_sources>

<final_result_to_validate>
{final_result}
</final_result_to_validate>

<validation_instructions>
For each specification in the final result, check:

1. Does this specification exist semantically in CSV data? (YES/NO + explanation)
2. Does this specification exist semantically in PNS data? (YES/NO + explanation)  
3. Are the options in final result common to BOTH matched specs? (YES/NO + explanation)
4. Is the specification name from PNS? (YES/NO + explanation)

After checking all specs individually, provide:
- OVERALL_VALID: YES/NO
- ERROR_SUMMARY: Brief summary of any errors found
- CORRECTION_NEEDED: What specific changes are needed
</validation_instructions>

<output_format>
SPEC_1_VALIDATION:
- Spec Name: [name from final result]
- Exists in CSV: YES/NO - [explanation]
- Exists in PNS: YES/NO - [explanation]  
- Options are common: YES/NO - [explanation]
- Uses PNS naming: YES/NO - [explanation]

SPEC_2_VALIDATION:
[repeat for each spec]

OVERALL_VALIDATION:
- OVERALL_VALID: YES/NO
- ERROR_SUMMARY: [brief summary]
- CORRECTION_NEEDED: [specific corrections needed]
</output_format>"""
        
        return prompt
    
    def _parse_csv_to_structured_format(self, csv_result: str) -> List[Dict[str, str]]:
        """Parse CSV triangulation result into standardized format"""
        if not csv_result:
            return []
        
        structured_specs = []
        
        try:
            lines = csv_result.strip().split('\n')
            
            # Find table data (skip headers and separators)
            for line in lines:
                line = line.strip()
                
                # Skip empty lines, headers, and separator lines
                if not line or 'Specification Name' in line or line.startswith('|--') or line.startswith('|-'):
                    continue
                
                # Look for table rows (containing | separator)
                if '|' in line:
                    # Clean up the line
                    cleaned_line = line
                    if cleaned_line.startswith('|'):
                        cleaned_line = cleaned_line[1:]
                    if cleaned_line.endswith('|'):
                        cleaned_line = cleaned_line[:-1]
                    
                    parts = [part.strip() for part in cleaned_line.split('|')]
                    
                    # Ensure we have at least spec name and options
                    if len(parts) >= 2 and parts[0] and parts[1]:
                        structured_specs.append({
                            'name': parts[0],
                            'options': parts[1],
                            'source': 'CSV'
                        })
            
            logger.debug(f"Parsed {len(structured_specs)} CSV specs into structured format")
            return structured_specs
            
        except Exception as e:
            logger.warning(f"Error parsing CSV to structured format: {e}")
            return []
    
    def _parse_pns_to_structured_format(self, pns_specs: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Parse PNS specs into standardized format with frequency information"""
        if not pns_specs:
            return []
        
        structured_specs = []
        
        try:
            for spec in pns_specs:
                if isinstance(spec, dict):
                    spec_name = spec.get('spec_name', 'Unknown')
                    spec_options = spec.get('option', 'Unknown')
                    spec_frequency = spec.get('frequency', 'N/A')
                    spec_status = spec.get('spec_status', 'N/A')
                    spec_priority = spec.get('importance_level', 'N/A')
                    
                    # Keep options clean but preserve structure
                    if spec_options and ' / ' in spec_options:
                        # Split by / and clean each option while preserving frequency context
                        options_list = [opt.strip() for opt in spec_options.split(' / ')]
                        cleaned_options = ', '.join(options_list)
                    else:
                        cleaned_options = spec_options
                    
                    structured_specs.append({
                        'name': spec_name,
                        'options': cleaned_options,
                        'frequency': spec_frequency,
                        'status': spec_status,
                        'priority': spec_priority,
                        'source': 'PNS'
                    })
            
            logger.debug(f"Parsed {len(structured_specs)} PNS specs into structured format with frequency data")
            return structured_specs
            
        except Exception as e:
            logger.warning(f"Error parsing PNS to structured format: {e}")
            return []
    
    def _parse_validation_response(self, validation_response: str) -> Dict[str, Any]:
        """Parse LLM validation response into structured format"""
        try:
            # Look for OVERALL_VALID result
            is_valid = "OVERALL_VALID: YES" in validation_response
            
            # Extract error summary
            error_summary = ""
            summary_start = validation_response.find("ERROR_SUMMARY:")
            if summary_start != -1:
                summary_section = validation_response[summary_start:].split('\n')[0]
                error_summary = summary_section.replace("ERROR_SUMMARY:", "").strip()
            
            # Extract correction needed
            correction_needed = ""
            correction_start = validation_response.find("CORRECTION_NEEDED:")
            if correction_start != -1:
                correction_section = validation_response[correction_start:].split('\n')[0]
                correction_needed = correction_section.replace("CORRECTION_NEEDED:", "").strip()
            
            # Extract individual validation errors for detailed feedback
            validation_errors = []
            lines = validation_response.split('\n')
            current_spec = ""
            
            for line in lines:
                line = line.strip()
                if line.startswith("- Spec Name:"):
                    current_spec = line.replace("- Spec Name:", "").strip()
                elif ": NO -" in line and current_spec:
                    validation_errors.append(f"{current_spec}: {line}")
            
            return {
                "is_valid": is_valid,
                "summary": error_summary if error_summary else "No errors found" if is_valid else "Validation failed",
                "errors": validation_errors,
                "correction_needed": correction_needed,
                "raw_response": validation_response
            }
            
        except Exception as e:
            logger.error(f"Error parsing validation response: {e}")
            return {
                "is_valid": False,
                "summary": f"Validation parsing error: {str(e)}",
                "errors": [f"Could not parse validation response: {str(e)}"],
                "correction_needed": "Manual review needed",
                "raw_response": validation_response
            }
    
    def _build_retry_prompt(self, product_name: str, csv_result: str, pns_specs: List[Dict[str, Any]], 
                           first_attempt: str, validation_errors: List[str]) -> str:
        """Build retry prompt with validation feedback"""
        
        # Convert both sources to standardized format for consistent LLM processing
        csv_structured = self._parse_csv_to_structured_format(csv_result)
        pns_structured = self._parse_pns_to_structured_format(pns_specs)
        
        # Prepare standardized CSV data
        csv_data = "\n=== CSV TRIANGULATED SPECIFICATIONS ===\n"
        if csv_structured:
            for i, spec in enumerate(csv_structured, 1):
                csv_data += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Source: CSV\n"
        else:
            csv_data += "No CSV specifications available\n"
        
        # Prepare standardized PNS data
        pns_direct = "\n=== PNS EXTRACTED SPECIFICATIONS ===\n"
        if pns_structured:
            for i, spec in enumerate(pns_structured, 1):
                pns_direct += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Freq: {spec['frequency']} | Status: {spec['status']} | Priority: {spec['priority']} | Source: PNS\n"
        else:
            pns_direct += "No PNS specifications available\n"
        
        # Prepare validation feedback
        validation_feedback = "\n=== VALIDATION ERRORS FROM FIRST ATTEMPT ===\n"
        for error in validation_errors:
            validation_feedback += f"❌ {error}\n"
        
        prompt = f"""<role>
You are a final consensus specialist fixing errors in triangulation. Your previous attempt had validation errors that need to be corrected.
</role>

<task>
Create a CORRECTED final consensus specification table showing ONLY specifications that appear in BOTH CSV and PNS data sources with ONLY common options.
</task>

<critical_corrections_needed>
Your first attempt had these specific errors:
{validation_feedback}

You MUST fix these errors in your corrected response.
</critical_corrections_needed>

<strict_consensus_rules>
APPLY THESE RULES EXACTLY:

STEP 1 - IDENTIFY SEMANTIC MATCHES ONLY:
• Find specifications that exist in BOTH CSV and PNS (names can differ but meaning must be similar)
• Use options overlap to confirm specs are the same (e.g., both have "KVA" values = power specs)

STEP 2 - EXTRACT COMMON OPTIONS ONLY:
• For each matched specification, find options that exist in BOTH the CSV spec AND the PNS spec
• EXCLUDE options that exist in only one source

STEP 3 - USE PNS NAMING AND PRIORITIZATION:
• ALWAYS use the PNS specification name (since PNS is pre-validated)
• Format options using PNS style when possible
• Consider PNS frequency and priority data when selecting common options

STEP 4 - STRICT VALIDATION:
• If a specification doesn't have common options → EXCLUDE IT
• If a specification exists in only one source → EXCLUDE IT
• If no consensus specifications exist → State "No consensus specifications found"
</strict_consensus_rules>

<data_sources>
{csv_data}
{pns_direct}
</data_sources>

<first_attempt_with_errors>
{first_attempt}
</first_attempt_with_errors>

<output_requirements>
Create the corrected consensus specification table with EXACTLY this format:

| Specification Name | Top Options | Why it matters in the market | Impacts Pricing? |

CRITICAL REQUIREMENTS:
• ONLY show specifications that exist semantically in BOTH sources
• ONLY show options that exist in BOTH the matched CSV and PNS specifications
• Use PNS specification names for matched specs
• If no consensus specs exist after strict filtering, state "No consensus specifications identified"
• Address ALL validation errors from your first attempt
</output_requirements>

<final_validation_check>
Before submitting, verify:
□ Each specification exists semantically in both CSV and PNS data
□ Each option exists in both the matched CSV spec AND matched PNS spec
□ Specification names use PNS terminology
□ No specifications from only one source are included
□ All validation errors from first attempt are fixed
</final_validation_check>"""
        
        return prompt
    
    def _parse_final_triangulation_result(self, result: str) -> List[Dict[str, Any]]:
        """Parse final triangulation result into structured table format"""
        try:
            lines = result.strip().split('\n')
            table_data = []
            rank = 1
            
            logger.info(f"Processing {len(lines)} lines for final triangulation parsing")
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Skip empty lines, headers, and separator lines
                if not line:
                    continue
                if 'Specification Name' in line:
                    continue
                if line.startswith('|--') or line.startswith('|-'):
                    continue
                if line.count('|') < 3:
                    continue
                
                # Look for table rows
                if '|' in line:
                    cleaned_line = line
                    if cleaned_line.startswith('|'):
                        cleaned_line = cleaned_line[1:]
                    if cleaned_line.endswith('|'):
                        cleaned_line = cleaned_line[:-1]
                    
                    parts = [part.strip() for part in cleaned_line.split('|')]
                    
                    if len(parts) >= 4:
                        table_data.append({
                            'Rank': rank,
                            'Specification': parts[0],
                            'Top Options': parts[1],
                            'Why it matters': parts[2].replace('in the market', '').strip(),
                            'Impacts Pricing?': parts[3]
                        })
                        rank += 1
                        logger.info(f"Added final triangulation row {rank-1}: {parts[0]}")
            
            logger.info(f"Successfully parsed {len(table_data)} final triangulation table rows")
            return table_data
            
        except Exception as e:
            logger.error(f"Error parsing final triangulation result: {e}")
            return [{
                'Rank': 1,
                'Specification': 'Final Triangulation Parse Error',
                'Top Options': 'Could not parse final result',
                'Why it matters': 'Error in parsing',
                'Impacts Pricing?': 'Unknown'
            }]

def final_triangulate_results(state: SpecExtractionState) -> SpecExtractionState:
    """LangGraph node function for final triangulation"""
    agent = FinalTriangulationAgent()
    return agent.final_triangulate(state)

def check_all_agents_completed(state: SpecExtractionState) -> str:
    """Check if all CSV agents have completed processing"""
    # Get all available CSV sources only (PNS is processed separately)
    available_sources = set(state["uploaded_files"].keys())  # CSV files only
        
    agents_status = get_agents_status(state)
    
    completed_sources = {
        source for source, status in agents_status.items()
        if status == "completed"
    }
    failed_sources = {
        source for source, status in agents_status.items()
        if status == "failed"
    }
    excluded_sources = {
        source for source, status in agents_status.items()
        if status == "excluded"
    }
    
    # If all available sources are either completed, failed, or excluded, we can proceed
    if available_sources <= (completed_sources | failed_sources | excluded_sources):
        if completed_sources:  # At least one completed successfully
            return "triangulate"
        else:  # All failed or excluded
            return "all_failed"
    else:
        return "wait"  # Still processing 