"""
Complete LangGraph workflow orchestration for spec extraction
Properly implements the original Streamlit workflow with LangGraph state management
"""

import logging
import copy
from typing import Dict, Any
from datetime import datetime
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from ..utils.state import (
    SpecExtractionState, 
    get_agents_status, 
    get_agent_results, 
    get_errors, 
    create_initial_state, 
    process_pns_data_directly
)
from .extraction_agent import (
    process_search_keywords,
    process_whatsapp_specs,
    process_rejection_comments,
    process_lms_chats,
)
from .triangulation_agent import triangulate_all_results, check_all_agents_completed
from .pns_processor import process_pns_json

logger = logging.getLogger(__name__)

class SpecExtractionWorkflow:
    """Main workflow for spec extraction using LangGraph"""
    
    def __init__(self):
        self.memory = MemorySaver()
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        # Create the StateGraph
        workflow = StateGraph(SpecExtractionState)
        
        # Add individual extraction agent nodes (CSV only)
        workflow.add_node("process_search_keywords", process_search_keywords)
        workflow.add_node("process_whatsapp_specs", process_whatsapp_specs)
        workflow.add_node("process_rejection_comments", process_rejection_comments)
        workflow.add_node("process_lms_chats", process_lms_chats)
        
        # Add coordination nodes
        workflow.add_node("wait_for_completion", self._wait_for_completion)
        workflow.add_node("triangulate_results", triangulate_all_results)
        workflow.add_node("handle_all_failed", self._handle_all_failed)
        
        # Set entry point - 4 CSV agents start simultaneously
        workflow.add_edge(START, "process_search_keywords")
        workflow.add_edge(START, "process_whatsapp_specs") 
        workflow.add_edge(START, "process_rejection_comments")
        workflow.add_edge(START, "process_lms_chats")
        
        # All CSV agents flow to the completion checker
        workflow.add_edge("process_search_keywords", "wait_for_completion")
        workflow.add_edge("process_whatsapp_specs", "wait_for_completion")
        workflow.add_edge("process_rejection_comments", "wait_for_completion")
        workflow.add_edge("process_lms_chats", "wait_for_completion")
        
        # Conditional routing based on completion status
        workflow.add_conditional_edges(
            "wait_for_completion",
            check_all_agents_completed,
            {
                "triangulate": "triangulate_results",
                "all_failed": "handle_all_failed",
                "wait": "wait_for_completion"  # Continue waiting
            }
        )
        
        # Final edges
        workflow.add_edge("triangulate_results", END)
        workflow.add_edge("handle_all_failed", END)
        
        # Compile the graph
        return workflow.compile(checkpointer=self.memory)
    
    def _wait_for_completion(self, state: SpecExtractionState) -> SpecExtractionState:
        """Wait for all agents to complete and update progress"""
        
        # Get status for all available CSV sources only
        available_sources = list(state["uploaded_files"].keys())  # CSV files only
        # PNS is no longer treated as an agent, processed separately
            
        agents_status = get_agents_status(state)
        
        # Count completed, failed, and excluded agents for available sources only
        completed_count = sum(1 for source in available_sources 
                            if agents_status.get(source) == "completed")
        failed_count = sum(1 for source in available_sources 
                         if agents_status.get(source) == "failed")
        excluded_count = sum(1 for source in available_sources 
                           if agents_status.get(source) == "excluded")
        total_count = len(available_sources)
        processed_count = completed_count + failed_count + excluded_count
        
        # Update progress
        progress = int(processed_count / total_count * 90)  # 90% for extraction, 10% for triangulation
        
        # Update current step
        if processed_count == total_count:
            if completed_count > 0:
                current_step = "ready_for_triangulation"
                status_parts = []
                if completed_count > 0:
                    status_parts.append(f"{completed_count} successful")
                if failed_count > 0:
                    status_parts.append(f"{failed_count} failed")
                if excluded_count > 0:
                    status_parts.append(f"{excluded_count} excluded")
                logs = [f"All agents completed: {', '.join(status_parts)}"]
            else:
                current_step = "all_agents_failed"
                logs = ["All agents failed or were excluded - no results to triangulate"]
        else:
            current_step = f"processing ({processed_count}/{total_count} completed)"
            logs = [f"Progress: {processed_count}/{total_count} agents completed"]
        
        return {
            "progress_percentage": progress,
            "current_step": current_step,
            "logs": logs
        }
    
    def _handle_all_failed(self, state: SpecExtractionState) -> SpecExtractionState:
        """Handle case where all agents failed"""
        
        # Collect all errors
        errors = get_errors(state)
        error_summary = []
        for source, error in errors.items():
            if error:  # Only include non-empty errors
                error_summary.append(f"{source}: {error}")
        
        triangulated_result = f"Processing failed for all sources:\n" + "\n".join(error_summary)
        
        return {
            "current_step": "all_failed",
            "progress_percentage": 0,
            "logs": ["Workflow failed: All extraction agents failed"],
            "triangulated_result": triangulated_result
        }
    
    def run_workflow(self, state: SpecExtractionState, config: Dict[str, Any] = None) -> SpecExtractionState:
        """Run the complete workflow"""
        try:
            logger.info(f"Starting workflow for product: {state['product_name']}")
            
            # Set initial state
            state["current_step"] = "starting_extraction"
            state["progress_percentage"] = 5
            state["logs"] = state["logs"] + ["Workflow started - launching extraction agents"]
            
            # Default config
            if config is None:
                config = {"configurable": {"thread_id": "default"}}
            
            # Run the graph
            final_state = self.graph.invoke(state, config=config)
            
            logger.info("Workflow completed successfully")
            return final_state
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            
            # Update state with error
            state["current_step"] = "workflow_failed"
            state["logs"] = state["logs"] + [f"Workflow execution failed: {str(e)}"]
            
            return state
    
    async def run_complete_workflow(self, mcat_id: str, pns_json_content: str, 
                                  csv_sources: Dict[str, Any]) -> Dict[str, Any]:
        """
        Complete workflow execution for FastAPI
        """
        workflow_state = {
            "start_time": datetime.now(),
            "mcat_id": mcat_id,
            "status": "processing"
        }
        
        try:
            logger.info(f"Starting complete workflow for MCAT {mcat_id}")
            
            # Step 1: Process PNS JSON directly
            logger.info("Step 1: Processing PNS JSON")
            pns_result = process_pns_json(pns_json_content)
            
            if pns_result["status"] != "completed":
                raise Exception(f"PNS processing failed: {pns_result.get('error', 'Unknown error')}")
            
            pns_specs = pns_result["extracted_specs"]
            logger.info(f"PNS processing completed: {len(pns_specs)} specifications extracted")
            
            # Step 2: Create initial workflow state
            logger.info("Step 2: Creating initial workflow state")
            initial_state = create_initial_state(
                product_name=mcat_id,
                files=csv_sources,
                pns_json=pns_json_content
            )
            
            # Add PNS processed specs to state
            initial_state["pns_processed_specs"] = pns_specs
            
            # Step 3: Run LangGraph workflow for CSV processing
            logger.info("Step 3: Running LangGraph workflow for CSV extraction")
            final_state = self.run_workflow(initial_state)
            
            # Step 4: Extract results
            logger.info("Step 4: Extracting final results")
            csv_agent_results = get_agent_results(final_state)
            triangulation_result_text = final_state.get("triangulated_result", "")
            triangulation_table = final_state.get("triangulated_table", [])
            
            # Create processing summary
            completed_agents = sum(1 for result in csv_agent_results.values() 
                                 if result.get("status") == "completed")
            total_agents = len(csv_agent_results)
            
            processing_summary = {
                "total_sources": total_agents,
                "successful_extractions": completed_agents,
                "pns_specs_found": len(pns_specs),
                "final_triangulated_specs": len(triangulation_table),
                "processing_time": (datetime.now() - workflow_state["start_time"]).total_seconds(),
                "product_name": final_state.get("product_name", mcat_id)
            }
            
            workflow_state.update({
                "status": "completed",
                "end_time": datetime.now()
            })
            
            return {
                "status": "completed",
                "mcat_id": mcat_id,
                "pns_specs": pns_specs,
                "csv_agent_results": csv_agent_results,
                "triangulation_result": {
                    "triangulated_table": triangulation_table,
                    "triangulated_text": triangulation_result_text
                },
                "processing_summary": processing_summary,
                "workflow_state": workflow_state
            }
            
        except Exception as e:
            logger.error(f"Complete workflow failed for MCAT {mcat_id}: {str(e)}")
            workflow_state.update({
                "status": "failed",
                "end_time": datetime.now(),
                "error": str(e)
            })
            
            return {
                "error": str(e),
                "status": "failed",
                "processing_summary": {
                    "total_sources": 0,
                    "successful_extractions": 0,
                    "pns_specs_found": 0,
                    "final_triangulated_specs": 0,
                    "processing_time": 0,
                    "product_name": mcat_id,
                    "error": str(e)
                },
                "workflow_state": {
                    "mcat_id": mcat_id,
                    "failed_at": workflow_state["end_time"].isoformat(),
                    "error_message": str(e)
                }
            }

# Global workflow instance for reuse
_workflow_instance = None

def get_workflow() -> SpecExtractionWorkflow:
    """Get or create single workflow instance"""
    global _workflow_instance
    if _workflow_instance is None:
        _workflow_instance = SpecExtractionWorkflow()
    return _workflow_instance

async def run_spec_extraction_workflow(mcat_id: str, pns_json_content: str, 
                                     csv_sources: Dict[str, Any]) -> Dict[str, Any]:
    """Main entry point for running complete spec extraction workflow"""
    workflow = get_workflow()
    return await workflow.run_complete_workflow(mcat_id, pns_json_content, csv_sources)

def run_spec_extraction(state: SpecExtractionState) -> SpecExtractionState:
    """Convenience function for running workflow with state"""
    workflow = get_workflow()
    return workflow.run_workflow(state)