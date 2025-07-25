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
#             model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
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
    """Agent for triangulating results from all sources"""
    
    def __init__(self):
        self.llm = ChatOpenAI(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=0.1,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
    
    def triangulate_results(self, state: SpecExtractionState) -> SpecExtractionState:
        """Triangulate results from all completed agents"""
        start_time = time.time()
        
        try:
            logger.info("Starting triangulation process")
            
            # Get all completed agent results using helper function
            agent_results = get_agent_results(state)
            completed_agents = {
                source: result for source, result in agent_results.items()
                if result.get("status") == "completed"
            }
            
            if not completed_agents:
                raise ValueError("No completed agent results to triangulate")
            
            # Prepare datasets for triangulation prompt
            datasets = []
            all_dataset_outputs = {}
            
            for source, result in completed_agents.items():
                dataset_info = {
                    "source": source,
                    "type": result["source_type"],
                    "rows_processed": result["raw_data_count"],
                    "extracted_specs": result["extracted_specs"]
                }
                datasets.append(dataset_info)
                all_dataset_outputs[source] = result["extracted_specs"]
            
            # Build triangulation prompt using multi-agent consensus and validation techniques
            prompt = self._build_triangulation_prompt(
                product_name=state["product_name"],
                datasets=datasets,
                all_dataset_outputs=all_dataset_outputs
            )
            
            logger.info(f"Sending triangulation request for {len(datasets)} datasets")
            
            # Call LLM for triangulation
            response = self.llm.invoke([HumanMessage(content=prompt)])
            triangulated_result = response.content
            
            # Debug: Log the raw LLM output
            logger.info(f"Raw LLM triangulation output: {triangulated_result}")
            
            # Parse the triangulated result into table format for export
            triangulated_table = self._parse_triangulation_result(triangulated_result)
            
            # Debug: Log the parsed table
            logger.info(f"Parsed triangulation table: {triangulated_table}")
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            logger.info(f"Triangulation completed in {processing_time:.2f}s")
            
            # Return only the keys this function should update
            return {
                "triangulated_result": triangulated_result,
                "triangulated_table": triangulated_table,
                "current_step": "completed",
                "progress_percentage": 100,
                "logs": [f"Triangulation completed successfully in {processing_time:.2f}s"]
            }
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error during triangulation: {error_msg}")
            
            # Return error state updates
            return {
                "current_step": "triangulation_failed",
                "logs": [f"Triangulation failed: {error_msg}"]
            }
    
    def _build_triangulation_prompt(self, product_name: str, datasets: List[Dict], all_dataset_outputs: Dict) -> str:
        """Build triangulation prompt using multi-agent consensus and validation techniques"""
        
        # Research-backed triangulation prompt with enhanced accuracy
        prompt = f"""<role>
You are a senior data triangulation specialist with expertise in multi-source B2B specification analysis. You excel at identifying patterns across diverse datasets and determining which specifications truly drive purchasing decisions for {product_name}.
</role>

<task>
Analyze {len(datasets)} independent extraction results to identify the most critical {product_name} specifications through cross-validation and consensus building.
</task>

<triangulation_methodology>

For the triangulation, give me results and top specifications that came from these datasets. Don't give 
the dataset itself in your response.
Merge Semantically same Specification options and name. Duplicate Specifications name should not be 
there. At least 2 options should be there to display any specification important and Specification name 
and Specification options should not be same or contain same words as in {product_name}.

</triangulation_methodology>

<validation_rules>
INCLUDE specifications that:
✓ Appear in 2+ sources OR have very high frequency in 1 source
✓ Have at least 2 meaningful options
✓ Directly influence {product_name} selection
✓ Represent tangible product attributes

EXCLUDE specifications that:
✗ Are generic descriptors (e.g., "Good Quality", "Best")
✗ Duplicate the product name (e.g., "Generator Type" for generators)
✗ Represent brands/companies (unless brand is a key differentiator)
✗ Are location-specific (unless critical for the product)
</validation_rules>

<datasets_to_analyze>
{json.dumps(all_dataset_outputs, indent=2)}
</datasets_to_analyze>

<output_requirements>
Create a business-focused specification table with EXACTLY this format:

| Specification Name | Top Options (based on data) | Why it matters in the market | Impacts Pricing? |

Requirements for each row:
1. Specification Name: Clear, professional terminology
2. Top Options: 3-5 most frequent options from the data (comma-separated)
3. Why it matters: Concise business justification (buying behavior, compatibility, regulations)
4. Impacts Pricing: "✅ Yes" or "❌ No" based on market analysis

CRITICAL INSTRUCTIONS:
• Limit to 3-5 most impactful specifications
• Use exact options from the data (don't invent new ones)
• Ensure each specification has multiple real options
• Focus on specifications that differentiate products
• Keep explanations concise and business-oriented
</output_requirements>

<example_output>
| Material | Aluminium, Steel, Stainless Steel, Cast Iron | Affects durability, weight, and corrosion resistance - key factors in industrial applications | ✅ Yes |
| Power Rating | 5 KVA, 7.5 KVA, 10 KVA, 15 KVA | Determines suitable applications and load capacity - primary selection criteria | ✅ Yes |
| Phase Configuration | Single Phase, Three Phase | Must match facility electrical infrastructure - non-negotiable compatibility requirement | ✅ Yes |
</example_output>

<final_validation>
Before submitting, ensure:
□ All options come directly from the provided datasets
□ Specifications represent consensus across multiple sources
□ Business justifications are specific to {product_name} market
□ Pricing impact assessment is logical and defensible
□ Output matches the required table format exactly
</final_validation>"""
        
        return prompt
    
    def _parse_triangulation_result(self, result: str) -> List[Dict[str, Any]]:
        """Parse triangulation result into structured table format for export"""
        try:
            lines = result.strip().split('\n')
            table_data = []
            rank = 1
            
            # Debug: log each line being processed
            logger.info(f"Processing {len(lines)} lines for parsing")
            
            # Look for table format in the result
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Debug: log the line being processed
                logger.info(f"Line {i}: '{line}' - Pipe count: {line.count('|')}")
                
                # Skip empty lines, headers, and separator lines
                if not line:
                    continue
                if 'Specification Name' in line:
                    continue
                if line.startswith('|--') or line.startswith('|-'):
                    continue
                if line.count('|') < 3:
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
                    
                    # Ensure we have at least 4 parts (spec, options, why, pricing)
                    if len(parts) >= 4:
                        # Map to competitor's format
                        table_data.append({
                            'Rank': rank,
                            'Specification': parts[0],  # Changed from 'Specification Name'
                            'Top Options': parts[1].replace('(based on data)', '').strip(),  # Remove "(based on data)"
                            'Why it matters': parts[2].replace('in the market', '').strip(),  # Remove "in the market"
                            'Impacts Pricing?': parts[3]  # Changed to include question mark
                        })
                        rank += 1
                        logger.info(f"Successfully added row {rank-1}: {parts[0]}")
            
            # Debug log
            logger.info(f"Successfully parsed {len(table_data)} table rows")
            
            return table_data
            
        except Exception as e:
            logger.error(f"Error parsing triangulation result: {e}")
            # Return a fallback structure with competitor's format
            return [{
                'Rank': 1,
                'Specification': 'Parse Error',
                'Top Options': 'Could not parse result',
                'Why it matters': 'Error in parsing',
                'Impacts Pricing?': 'Unknown'
            }]


def triangulate_all_results(state: SpecExtractionState) -> SpecExtractionState:
    """LangGraph node function for triangulation"""
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
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=0.1,
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
    
    def final_triangulate(self, state: SpecExtractionState) -> SpecExtractionState:
        """Perform final triangulation between CSV triangulated result and PNS specs with validation"""
        start_time = time.time()
        
        try:
            logger.info("Starting final triangulation between CSV results and PNS specs")
            
            csv_result = state.get("triangulated_result", "")
            pns_specs = state.get("pns_extracted_specs", [])
            
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
        pns_data = "\n=== PNS EXTRACTED SPECIFICATIONS ===\n"
        if pns_structured:
            for i, spec in enumerate(pns_structured, 1):
                pns_data += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Freq: {spec['frequency']} | Status: {spec['status']} | Priority: {spec['priority']} | Source: PNS\n"
        else:
            pns_data += "No PNS specifications available\n"
        
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
{pns_data}
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
        pns_data = "\n=== PNS EXTRACTED SPECIFICATIONS ===\n"
        if pns_structured:
            for i, spec in enumerate(pns_structured, 1):
                pns_data += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Freq: {spec['frequency']} | Status: {spec['status']} | Priority: {spec['priority']} | Source: PNS\n"
        else:
            pns_data += "No PNS specifications available\n"
        
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
{pns_data}
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
        pns_data = "\n=== PNS EXTRACTED SPECIFICATIONS ===\n"
        if pns_structured:
            for i, spec in enumerate(pns_structured, 1):
                pns_data += f"{i}. Spec: {spec['name']} | Options: {spec['options']} | Freq: {spec['frequency']} | Status: {spec['status']} | Priority: {spec['priority']} | Source: PNS\n"
        else:
            pns_data += "No PNS specifications available\n"
        
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
{pns_data}
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
    """Check if all agents have completed processing"""
    uploaded_sources = set(state["uploaded_files"].keys())
    agents_status = get_agents_status(state)
    
    completed_sources = {
        source for source, status in agents_status.items()
        if status == "completed"
    }
    failed_sources = {
        source for source, status in agents_status.items()
        if status == "failed"
    }
    
    # If all uploaded sources are either completed or failed, we can proceed
    if uploaded_sources <= (completed_sources | failed_sources):
        if completed_sources:  # At least one completed successfully
            return "triangulate"
        else:  # All failed
            return "all_failed"
    else:
        return "wait"  # Still processing 