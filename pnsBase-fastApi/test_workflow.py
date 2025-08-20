"""
Comprehensive test script for the core workflow with hardcoded sample data
This bypasses Redis and BigQuery dependencies for testing the core functionality
"""

import os
import sys
import json
import time
import asyncio
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the app directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

def load_sample_data():
    """Load sample data from the sample2 directory"""
    sample_data = {}
    
    # Load CSV files
    csv_files = {
        'search_keywords': 'sample2/searchKW.csv',
        'whatsapp_specs': 'sample2/custom_spec.csv', 
        'rejection_comments': 'sample2/BLNI.csv',
        'lms_chats': 'sample2/LMS.csv'
    }
    
    for source_name, file_path in csv_files.items():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sample_data[source_name] = f.read()
            print(f"   ✅ Loaded {source_name}: {len(sample_data[source_name])} characters")
        except Exception as e:
            print(f"   ❌ Failed to load {source_name}: {e}")
            sample_data[source_name] = ""
    
    # Load PNS JSON
    try:
        with open('sample2/pnsSample', 'r', encoding='utf-8') as f:
            sample_data['pns_json'] = f.read()
        print(f"   ✅ Loaded PNS JSON: {len(sample_data['pns_json'])} characters")
    except Exception as e:
        print(f"   ❌ Failed to load PNS JSON: {e}")
        sample_data['pns_json'] = ""
    
    return sample_data

def test_individual_components():
    """Test individual components separately"""
    print("🧪 Testing Individual Components")
    print("=" * 50)
    
    try:
        # Test PNS Processor
        print("1. Testing PNS Processor...")
        from app.services.pns_processor import process_pns_json
        
        with open('sample2/pnsSample', 'r', encoding='utf-8') as f:
            pns_content = f.read()
        
        pns_result = process_pns_json(pns_content)
        print(f"   ✅ PNS Processor: {pns_result['status']}")
        if pns_result['status'] == 'completed':
            print(f"      Extracted {len(pns_result['extracted_specs'])} specifications")
        
        # Test Data Processor
        print("2. Testing Data Processor...")
        from app.services.data_processor import DataProcessor
        
        with open('sample2/searchKW.csv', 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        processed_chunks = DataProcessor.process_csv_data(csv_content, 'search_keywords')
        print(f"   ✅ Data Processor: {len(processed_chunks)} chunks created")
        
        # Test Extraction Agent
        print("3. Testing Extraction Agent...")
        from app.services.extraction_agent import ExtractionAgent
        
        agent = ExtractionAgent()
        result = agent.process_source('search_keywords', 'Commercial Atta Chakki Machine', csv_content)
        print(f"   ✅ Extraction Agent: {result['status']} in {result['processing_time']:.2f}s")
        if result['status'] == 'completed':
            print(f"      Processed {result['chunks_processed']} chunks")
        
        print()
        print("✅ Individual Component Tests Completed Successfully!")
        
    except Exception as e:
        print(f"❌ Individual component test failed: {e}")
        import traceback
        traceback.print_exc()

def test_core_workflow():
    """Test the core workflow directly with sample data (bypasses Redis/BigQuery)"""
    print("🧪 Testing Core Workflow with Sample Data")
    print("=" * 50)
    
    try:
        # Import required components
        from app.services.workflow import run_spec_extraction
        from app.utils.state import create_initial_state
        from app.services.pns_processor import process_pns_json
        
        print("1. Loading sample data...")
        sample_data = load_sample_data()
        
        if not sample_data['pns_json']:
            print("   ❌ No PNS data available - cannot proceed with workflow test")
            return
        
        print("2. Processing PNS JSON...")
        pns_result = process_pns_json(sample_data['pns_json'])
        
        if pns_result['status'] == 'failed':
            print(f"   ❌ PNS processing failed: {pns_result['error']}")
            return
        
        print(f"   ✅ PNS processed successfully: {len(pns_result['extracted_specs'])} specifications extracted")
        
        print("3. Creating initial state...")
        initial_state = create_initial_state(
            product_name="Unknown Product",  # Test product name extraction
            files={
                'search_keywords': sample_data['search_keywords'],
                'whatsapp_specs': sample_data['whatsapp_specs'],
                'rejection_comments': sample_data['rejection_comments'],
                'lms_chats': sample_data['lms_chats']
            },
            pns_json=sample_data['pns_json']
        )
        
        # IMPORTANT: Add the processed PNS specs to the state (this was missing!)
        initial_state['pns_processed_specs'] = pns_result['extracted_specs']
        initial_state['pns_processing_error'] = ""
        
        print(f"   ✅ Initial state created with {len(initial_state['uploaded_files'])} CSV sources")
        print(f"   ✅ Added {len(pns_result['extracted_specs'])} PNS specifications to state")
        
        print("4. Running complete workflow...")
        start_time = time.time()
        
        # Run the workflow using the correct function
        final_state = run_spec_extraction(initial_state)
        
        processing_time = time.time() - start_time
        
        print(f"   ✅ Workflow completed in {processing_time:.2f} seconds")
        
        # Display results
        print("5. Workflow Results:")
        print("   📊 Current Step:", final_state.get('current_step', 'unknown'))
        print("   📊 Progress:", final_state.get('progress_percentage', 0), "%")
        print("   📊 Product Name:", final_state.get('product_name', 'Unknown'))
        
        if 'triangulated_result' in final_state:
            print("   📊 Triangulation Result: Available")
            triangulated_text = final_state['triangulated_result']
            if len(triangulated_text) > 100:
                print(f"      Preview: {triangulated_text[:100]}...")
            else:
                print(f"      Content: {triangulated_text}")
        
        if 'triangulated_table' in final_state:
            print(f"   📊 Triangulation Table: {len(final_state['triangulated_table'])} rows")
        
        # Show agent statuses
        print("6. Agent Statuses:")
        for source in ['search_keywords', 'whatsapp_specs', 'rejection_comments', 'lms_chats']:
            status = final_state.get(f'{source}_status', 'unknown')
            result = final_state.get(f'{source}_result', {})
            if result:
                processing_time = result.get('processing_time', 0)
                extracted_count = len(result.get('extracted_specs', '').split('\n')) if result.get('extracted_specs') else 0
                print(f"   📊 {source}: {status} ({processing_time:.2f}s, {extracted_count} specs)")
            else:
                print(f"   📊 {source}: {status}")
        
        # Show logs
        if 'logs' in final_state:
            print("7. Workflow Logs:")
            for log in final_state['logs'][-5:]:  # Show last 5 logs
                print(f"   📝 {log}")
        
        print()
        print("✅ Core Workflow Test Completed Successfully!")
        
    except Exception as e:
        print(f"❌ Core workflow test failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main test function"""
    print("🚀 Starting Comprehensive Workflow Tests...")
    print("=" * 60)
    
    # Test 1: Individual components (no external dependencies)
    print()
    test_individual_components()
    
    # Test 2: Core workflow with sample data (no external dependencies)
    print()
    test_core_workflow()
    
    print()
    print("🎯 All Tests Complete!")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Test error: {e}")
        import traceback
        traceback.print_exc()
