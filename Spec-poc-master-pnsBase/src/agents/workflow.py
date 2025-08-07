import logging
import copy
from typing import Dict, Any
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from ..utils.state import SpecExtractionState, get_agents_status, get_agent_results, get_errors
from .extraction_agent import (
    process_search_keywords,
    process_whatsapp_specs,
    # process_pns_calls,  # Commented out - now JSON processing
    process_rejection_comments,
    process_lms_chats,
    # process_pns_data  # REMOVED: PNS no longer an agent
)
from .triangulation_agent import triangulate_all_results, check_all_agents_completed
# , meta_ensemble_triangulate  # Commented out - no longer used

logger = logging.getLogger(__name__)

# COMMENTED OUT - Meta-ensemble workflow no longer used
# class MetaEnsembleWorkflow:
#     """Meta-ensemble workflow that runs the extraction process 3 times and performs final ensemble triangulation"""
#     
#     def __init__(self):
#         self.single_workflow = SpecExtractionWorkflow()
#     
#     def run_meta_ensemble(self, state: SpecExtractionState, config: Dict[str, Any] = None) -> SpecExtractionState:
#         """Run the meta-ensemble process: 3 sequential runs + final ensemble"""
#         try:
#             logger.info(f"Starting meta-ensemble for product: {state['product_name']}")
#             
#             # Initialize meta-ensemble state
#             state["current_step"] = "meta_ensemble_starting"
#             state["progress_percentage"] = 5
#             state["logs"] = state["logs"] + ["Meta-ensemble started - running 3 sequential extractions"]
#             
#             run_results = []
#             
#             # Run the extraction process 3 times
#             for run_num in range(1, 4):
#                 logger.info(f"Starting meta-ensemble run {run_num}/3")
#                 
#                 # Update state for current run
#                 state["current_run"] = run_num
#                 state["current_step"] = f"meta_ensemble_run_{run_num}"
#                 state["progress_percentage"] = 5 + (run_num - 1) * 30  # 5%, 35%, 65%
#                 state["logs"] = state["logs"] + [f"Starting run {run_num}/3"]
#                 
#                 # Create a fresh copy of state for this run (reset agent results)
#                 run_state = self._prepare_run_state(state)
#                 
#                 # Run single workflow
#                 if config is None:
#                     config = {"configurable": {"thread_id": f"meta_run_{run_num}"}}
#                 else:
#                     config["configurable"]["thread_id"] = f"meta_run_{run_num}"
#                 
#                 run_result = self.single_workflow.run_workflow(run_state, config)
#                 
#                 # Store the triangulation result
#                 if run_result.get("triangulated_result"):
#                     run_results.append({
#                         "run_number": run_num,
#                         "triangulated_result": run_result["triangulated_result"],
#                         "triangulated_table": run_result.get("triangulated_table", []),
#                         "agent_results": get_agent_results(run_result)
#                     })
#                     logger.info(f"Completed meta-ensemble run {run_num}/3 successfully")
#                 else:
#                     logger.warning(f"Meta-ensemble run {run_num}/3 failed or had no results")
#                     run_results.append({
#                         "run_number": run_num,
#                         "triangulated_result": "Run failed",
#                         "triangulated_table": [],
#                         "agent_results": {}
#                     })
#                 
#                 # Update progress
#                 state["progress_percentage"] = 5 + run_num * 30  # 35%, 65%, 95%
#                 state["logs"] = state["logs"] + [f"Completed run {run_num}/3"]
#             
#             # Store all run results
#             state["run_results"] = run_results
#             state["current_step"] = "meta_ensemble_triangulating"
#             state["progress_percentage"] = 95
#             state["logs"] = state["logs"] + ["All 3 runs completed - starting final ensemble triangulation"]
#             
#             # Perform final ensemble triangulation
#             logger.info("Starting final ensemble triangulation")
#             ensemble_result = meta_ensemble_triangulate(state)
#             
#             # Update state with ensemble results
#             state.update(ensemble_result)
#             
#             logger.info("Meta-ensemble completed successfully")
#             return state
#             
#         except Exception as e:
#             logger.error(f"Meta-ensemble execution failed: {str(e)}")
#             
#             # Update state with error
#             state["current_step"] = "meta_ensemble_failed"
#             state["logs"] = state["logs"] + [f"Meta-ensemble execution failed: {str(e)}"]
#             
#             return state

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
        # workflow.add_node("process_pns_calls", process_pns_calls)  # Commented out - now JSON processing
        workflow.add_node("process_rejection_comments", process_rejection_comments)
        workflow.add_node("process_lms_chats", process_lms_chats)
        # workflow.add_node("process_pns_data", process_pns_data) # REMOVED: PNS no longer an agent
        
        # Add coordination nodes
        workflow.add_node("wait_for_completion", self._wait_for_completion)
        workflow.add_node("triangulate_results", triangulate_all_results)
        workflow.add_node("handle_all_failed", self._handle_all_failed)
        
        # Set entry point - 4 CSV agents start simultaneously
        workflow.add_edge(START, "process_search_keywords")
        workflow.add_edge(START, "process_whatsapp_specs") 
        # workflow.add_edge(START, "process_pns_calls")  # Commented out - now JSON processing
        workflow.add_edge(START, "process_rejection_comments")
        workflow.add_edge(START, "process_lms_chats")
        # workflow.add_edge(START, "process_pns_data") # REMOVED: PNS no longer an agent
        
        # All CSV agents flow to the completion checker
        workflow.add_edge("process_search_keywords", "wait_for_completion")
        workflow.add_edge("process_whatsapp_specs", "wait_for_completion")
        # workflow.add_edge("process_pns_calls", "wait_for_completion")  # Commented out
        workflow.add_edge("process_rejection_comments", "wait_for_completion")
        workflow.add_edge("process_lms_chats", "wait_for_completion")
        # workflow.add_edge("process_pns_data", "wait_for_completion") # REMOVED: PNS no longer an agent
        
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
            result = self.graph.invoke(state, config=config)
            
            logger.info("Workflow completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            
            # Update state with error
            state["current_step"] = "workflow_failed"
            state["logs"] = state["logs"] + [f"Workflow execution failed: {str(e)}"]
            
            return state
    
    def stream_workflow(self, state: SpecExtractionState, config: Dict[str, Any] = None):
        """Stream workflow execution for real-time updates"""
        try:
            logger.info(f"Starting streaming workflow for product: {state['product_name']}")
            
            # Set initial state
            state["current_step"] = "starting_extraction"
            state["progress_percentage"] = 5
            state["logs"] = state["logs"] + ["Workflow started - launching extraction agents"]
            
            # Default config
            if config is None:
                config = {"configurable": {"thread_id": "default"}}
            
            # Stream the graph execution with proper error handling
            try:
                for chunk in self.graph.stream(state, config=config):
                    yield chunk
                    
            except GeneratorExit:
                # Handle graceful shutdown - this is normal when Streamlit closes the generator
                logger.info("Streaming stopped - generator closed gracefully")
                return
                
            except StopIteration:
                # Handle normal completion
                logger.info("Streaming completed normally")
                return
                
        except GeneratorExit:
            # Handle outer GeneratorExit
            logger.info("Streaming workflow generator closed")
            return
            
        except Exception as e:
            logger.error(f"Streaming workflow failed: {str(e)}")
            
            # Yield error state
            state["current_step"] = "workflow_failed"
            state["logs"] = state["logs"] + [f"Workflow execution failed: {str(e)}"]
            
            try:
                yield {"error": state}
            except GeneratorExit:
                # Even error yielding can be interrupted
                logger.info("Error state yielding interrupted by generator close")
                return


# Global workflow instances
_workflow_instance = None
# _meta_ensemble_instance = None  # Commented out - no longer used

def get_workflow() -> SpecExtractionWorkflow:
    """Get or create single workflow instance"""
    global _workflow_instance
    if _workflow_instance is None:
        _workflow_instance = SpecExtractionWorkflow()
    return _workflow_instance

# COMMENTED OUT - Meta-ensemble workflow no longer used
# def get_meta_ensemble_workflow() -> MetaEnsembleWorkflow:
#     """Get or create meta-ensemble workflow instance"""
#     global _meta_ensemble_instance
#     if _meta_ensemble_instance is None:
#         _meta_ensemble_instance = MetaEnsembleWorkflow()
#     return _meta_ensemble_instance

def run_spec_extraction(state: SpecExtractionState) -> SpecExtractionState:
    """Main entry point for running spec extraction with single workflow"""
    workflow = get_workflow()
    return workflow.run_workflow(state)

def stream_spec_extraction(state: SpecExtractionState):
    """Stream spec extraction with single workflow for real-time updates"""
    workflow = get_workflow()
    return workflow.stream_workflow(state) 