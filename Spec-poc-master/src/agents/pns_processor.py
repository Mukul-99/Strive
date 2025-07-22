import json
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class PNSProcessor:
    """Handler for PNS JSON processing and spec extraction"""
    
    def __init__(self):
        pass
    
    def process_pns_json(self, pns_json_content: str) -> Dict[str, Any]:
        """Process PNS JSON and extract max 5 specifications"""
        try:
            logger.info("Starting PNS JSON processing")
            
            if not pns_json_content or not pns_json_content.strip():
                return {
                    "status": "failed",
                    "error": "No PNS JSON content provided",
                    "extracted_specs": []
                }
            
            # Parse JSON content
            pns_data = json.loads(pns_json_content.strip())
            
            # Extract specifications (prioritize primary, then secondary)
            extracted_specs = self._extract_specs_from_json(pns_data)
            
            if not extracted_specs:
                return {
                    "status": "failed", 
                    "error": "No specifications found in PNS JSON data",
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
    
    def _extract_specs_from_json(self, pns_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract max 5 specs from PNS JSON data, combining all options per spec"""
        
        combined_specs = []
        
        # First priority: Extract from primary_specs
        if "primary_specs" in pns_data and isinstance(pns_data["primary_specs"], list):
            for spec in pns_data["primary_specs"]:
                if len(combined_specs) >= 5:
                    break
                if isinstance(spec, dict):
                    combined_spec = self._process_spec_combined_options(spec, "Primary")
                    if combined_spec:
                        combined_specs.append(combined_spec)
        
        # Second priority: Extract from secondary_specs if we need more
        if len(combined_specs) < 5 and "secondary_specs" in pns_data and isinstance(pns_data["secondary_specs"], list):
            for spec in pns_data["secondary_specs"]:
                if len(combined_specs) >= 5:
                    break
                if isinstance(spec, dict):
                    combined_spec = self._process_spec_combined_options(spec, "Secondary")
                    if combined_spec:
                        combined_specs.append(combined_spec)
        
        logger.info(f"Extracted {len(combined_specs)} combined specifications (max 5)")
        return combined_specs
    
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
                "total_frequency": total_frequency  # For sorting if needed
            }
            
            return combined_spec
            
        except Exception as e:
            logger.warning(f"Failed to process combined spec: {e}")
            return None

def process_pns_json(pns_json_content: str) -> Dict[str, Any]:
    """Main function to process PNS JSON content"""
    processor = PNSProcessor()
    return processor.process_pns_json(pns_json_content) 