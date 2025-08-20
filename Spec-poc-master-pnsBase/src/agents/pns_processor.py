import json
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class PNSProcessor:
    """Handler for PNS JSON processing and spec extraction"""
    
    def __init__(self):
        pass
    
    def process_pns_json(self, pns_json_content: str) -> Dict[str, Any]:
        """Process PNS JSON and extract top 5 specifications from all 4 categories based on frequency"""
        try:
            logger.info("Starting PNS JSON processing")
            
            if not pns_json_content or not pns_json_content.strip():
                logger.warning("No PNS JSON content provided")
                return {
                    "status": "failed",
                    "error": "No PNS JSON content provided",
                    "extracted_specs": []
                }
            
            # Parse JSON content
            pns_data = json.loads(pns_json_content.strip())
            logger.info(f"Successfully parsed PNS JSON with {len(pns_data)} top-level keys")
            logger.info(f"Top-level keys: {list(pns_data.keys())}")
            
            # Check for spec_summary structure
            if "spec_summary" in pns_data:
                spec_summary = pns_data["spec_summary"]
                if isinstance(spec_summary, dict):
                    logger.info(f"Found spec_summary with keys: {list(spec_summary.keys())}")
                else:
                    logger.warning(f"spec_summary is not a dict, type: {type(spec_summary)}")
            else:
                logger.info("No spec_summary found, checking for specs at root level")
            
            # Extract specifications from all 4 categories
            extracted_specs = self._extract_top_specs_from_all_categories(pns_data)
            
            if not extracted_specs:
                logger.warning("No specifications found in PNS JSON data after filtering")
                return {
                    "status": "failed", 
                    "error": "No specifications found in PNS JSON data after filtering",
                    "extracted_specs": []
                }
            
            logger.info(f"Successfully extracted {len(extracted_specs)} specifications from PNS JSON")
            
            return {
                "status": "completed",
                "extracted_specs": extracted_specs,
                "error": ""
            }
            
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON format: {str(e)}"
            logger.error(f"PNS JSON parsing failed: {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "extracted_specs": []
            }
        except Exception as e:
            error_msg = f"PNS processing error: {str(e)}"
            logger.error(f"PNS processing failed: {error_msg}")
            return {
                "status": "failed", 
                "error": error_msg,
                "extracted_specs": []
            }
    
    def _extract_top_specs_from_all_categories(self, pns_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract top 5 specs from all 4 categories based on frequency, excluding product type specs"""
        
        all_specs = []
        
        # Check if specs are nested under spec_summary (new format) or at root level (old format)
        if "spec_summary" in pns_data and isinstance(pns_data["spec_summary"], dict):
            # New format: specs nested under spec_summary
            spec_container = pns_data["spec_summary"]
            logger.info("Using new JSON format: specs nested under spec_summary")
        else:
            # Old format: specs at root level
            spec_container = pns_data
            logger.info("Using legacy JSON format: specs at root level")
        
        # Extract from all 4 categories
        categories = ["primary_specs", "secondary_specs", "tertiary_specs", "quaternary_specs"]
        
        for category in categories:
            if category in spec_container and isinstance(spec_container[category], list):
                logger.info(f"Processing {category}: {len(spec_container[category])} specs found")
                for spec in spec_container[category]:
                    if isinstance(spec, dict):
                        # Filter out product type specs
                        spec_name = spec.get("spec_name", "").lower()
                        if "product type" in spec_name:
                            logger.info(f"Filtering out product type spec: {spec.get('spec_name', 'Unknown')}")
                            continue
                        
                        processed_spec = self._process_spec_combined_options(spec, category.replace("_specs", "").title())
                        if processed_spec:
                            all_specs.append(processed_spec)
                            logger.info(f"Added spec: {spec.get('spec_name', 'Unknown')} from {category}")
            else:
                logger.info(f"Category {category} not found or not a list in spec container")
        
        # Sort by frequency (descending) and take top 5
        all_specs.sort(key=lambda x: x.get("total_frequency", 0), reverse=True)
        top_5_specs = all_specs[:5]
        
        logger.info(f"Extracted top {len(top_5_specs)} specifications from all categories based on frequency")
        return top_5_specs
    
    def _process_spec_combined_options(self, spec: Dict[str, Any], importance_level: str) -> Dict[str, Any]:
        """Process a single specification and combine all its options with / separators"""
        try:
            # Extract required fields with defaults
            spec_name = spec.get("spec_name", "Unknown Specification")
            
            # Process all values and sort by frequency
            all_values = []
            if "values" in spec and isinstance(spec["values"], list):
                for value_data in spec["values"]:
                    if isinstance(value_data, dict):
                        all_values.append({
                            "option": value_data.get("standardized_value", "Unknown Option"),
                            "frequency": value_data.get("frequency", 0),
                            "status": value_data.get("spec_status", "Unknown")
                        })
            
            if not all_values:
                return None
            
            # Sort by frequency (descending)
            all_values.sort(key=lambda x: x["frequency"], reverse=True)
            
            # Combine options, frequencies, and statuses with / separators
            options = []
            frequencies = []
            statuses = []
            total_frequency = 0
            
            for value in all_values:
                options.append(value["option"])
                frequencies.append(str(value["frequency"]))
                total_frequency += value["frequency"]
                
                # Map status to descriptive terms
                status = value["status"]
                if status == "Dominant":
                    status_display = "✅ Dominant"
                elif status == "Emerging":
                    status_display = "🔶 Emerging"  
                elif status == "Exploring":
                    status_display = "🔍 Exploring"
                elif status == "Unknown":
                    status_display = "❓ Unknown"
                else:
                    status_display = status
                
                statuses.append(status_display)
            
            # Format combined strings
            combined_options = " / ".join(options)
            combined_frequency = f"{' / '.join(frequencies)} (Total: {total_frequency})"
            combined_status = " / ".join(statuses)
            
            # Create combined spec format for display
            combined_spec = {
                "spec_name": spec_name,
                "option": combined_options,
                "frequency": combined_frequency,
                "spec_status": combined_status,
                "importance_level": importance_level,
                "total_frequency": total_frequency  # For sorting
            }
            
            return combined_spec
            
        except Exception as e:
            logger.warning(f"Failed to process combined spec: {e}")
            return None

def process_pns_json(pns_json_content: str) -> Dict[str, Any]:
    """Main function to process PNS JSON content"""
    processor = PNSProcessor()
    return processor.process_pns_json(pns_json_content) 