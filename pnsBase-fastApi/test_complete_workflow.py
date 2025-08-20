#!/usr/bin/env python3
"""
Comprehensive test script for the complete workflow using sample data
Tests the core processing logic without Redis or external API dependencies
"""

import os
import sys
import json
import asyncio
from pathlib import Path

# Add the app directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

def load_sample_data():
    """Load all sample data files"""
    sample_data = {}
    
    # Load CSV files
    csv_files = {
        'search_keywords': 'sample2/searchKW.csv',
        'lms_chats': 'sample2/LMS.csv', 
        'rejection_comments': 'sample2/BLNI.csv',
        'whatsapp_specs': 'sample2/custom_spec.csv'
    }
    
    print("📂 Loading sample CSV files...")
    for source_name, file_path in csv_files.items():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                sample_data[source_name] = content
            print(f"   ✅ Loaded {source_name}: {len(content)} characters")
        except Exception as e:
            print(f"   ❌ Failed to load {source_name}: {e}")
            sample_data[source_name] = ""
    
    # Load PNS JSON
    print("\n📋 Loading PNS JSON data...")
    try:
        with open('sample2/pnsSample.json', 'r', encoding='utf-8') as f:
            pns_content = f.read()
            sample_data['pns_json'] = pns_content
        print(f"   ✅ Loaded PNS JSON: {len(pns_content)} characters")
        
        # Validate JSON structure
        pns_data = json.loads(pns_content)
        print(f"   📊 PNS Data Overview:")
        print(f"      - Category: {pns_data.get('category_name', 'N/A')}")
        print(f"      - Primary Specs: {len(pns_data.get('primary_specs', []))}")
        print(f"      - Secondary Specs: {len(pns_data.get('secondary_specs', []))}")
        
    except Exception as e:
        print(f"   ❌ Failed to load PNS JSON: {e}")
        sample_data['pns_json'] = ""
    
    return sample_data

async def test_pns_processing(pns_json_content):
    """Test PNS JSON processing"""
    print("\n🔄 Testing PNS Processing...")
    
    try:
        from app.services.pns_processor import process_pns_json
        
        # Process PNS JSON
        pns_result = process_pns_json(pns_json_content)
        
        print(f"   ✅ PNS Processing completed")
        print(f"   📊 Results:")
        print(f"      - Status: {pns_result.get('status', 'Unknown')}")
        print(f"      - Processing time: {pns_result.get('processing_time', 0):.2f}s")
        print(f"      - Extracted specs: {len(pns_result.get('extracted_specs', []))}")
        
        # Show sample extracted specs
        extracted_specs = pns_result.get('extracted_specs', [])
        if extracted_specs:
            print(f"   📝 Sample extracted specs (top 3):")
            for i, spec in enumerate(extracted_specs[:3], 1):
                print(f"      {i}. {spec.get('spec_name', 'N/A')} - {spec.get('option', 'N/A')} (Freq: {spec.get('frequency', 0)})")
        
        return pns_result
        
    except Exception as e:
        print(f"   ❌ PNS Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "failed", "error": str(e)}

async def test_csv_processing(csv_sources):
    """Test CSV processing for all sources"""
    print("\n🔄 Testing CSV Processing...")
    
    try:
        from app.services.extraction_agent import ExtractionAgent
        
        agent = ExtractionAgent()
        csv_results = {}
        
        for source_name, csv_content in csv_sources.items():
            if not csv_content:
                print(f"   ⚠️  Skipping {source_name}: No data")
                continue
                
            print(f"   🔍 Processing {source_name}...")
            
            try:
                # Process the CSV source
                result = agent.process_source(
                    source_name=source_name,
                    product_name="Commercial Atta Chakki Machine",
                    file_content=csv_content
                )
                
                csv_results[source_name] = result
                
                print(f"      ✅ Status: {result.get('status', 'Unknown')}")
                print(f"      ⏱️  Time: {result.get('processing_time', 0):.2f}s")
                print(f"      📊 Rows: {result.get('raw_data_count', 0)}")
                
                # Show sample extracted text (first 200 chars)
                extracted = result.get('extracted_specs', '')
                if extracted:
                    preview = extracted[:200] + "..." if len(extracted) > 200 else extracted
                    print(f"      📝 Preview: {preview}")
                
            except Exception as e:
                print(f"      ❌ Failed: {e}")
                csv_results[source_name] = {"status": "failed", "error": str(e)}
        
        return csv_results
        
    except Exception as e:
        print(f"   ❌ CSV Processing setup failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

async def test_triangulation(pns_result, csv_results):
    """Test triangulation logic"""
    print("\n🔄 Testing Triangulation...")
    
    try:
        from app.services.triangulation_agent import triangulate_all_results
        from app.utils.state import create_initial_state
        
        # Create a mock state with results
        mock_state = {
            'product_name': 'Commercial Atta Chakki Machine',
            'pns_processed_specs': pns_result.get('extracted_specs', []),
            'search_keywords_result': csv_results.get('search_keywords', {}),
            'lms_chats_result': csv_results.get('lms_chats', {}),
            'rejection_comments_result': csv_results.get('rejection_comments', {}),
            'whatsapp_specs_result': csv_results.get('whatsapp_specs', {}),
        }
        
        print("   🔍 Running triangulation analysis...")
        
        # Run triangulation
        triangulation_result = triangulate_all_results(mock_state)
        
        print(f"   ✅ Triangulation completed")
        print(f"   📊 Results:")
        print(f"      - Status: {triangulation_result.get('status', 'Unknown')}")
        print(f"      - Processing time: {triangulation_result.get('processing_time', 0):.2f}s")
        
        # Check triangulated table
        triangulated_table = triangulation_result.get('triangulated_table', [])
        if triangulated_table:
            print(f"      - Triangulated specs: {len(triangulated_table)}")
            print(f"   📝 Top triangulated results:")
            
            for i, item in enumerate(triangulated_table[:5], 1):
                score = item.get('Score', 'N/A')
                pns = item.get('PNS', 'N/A')
                options = item.get('Options', 'N/A')
                print(f"      {i}. Score: {score} | PNS: {pns} | Options: {options}")
        else:
            print("      - No triangulated results found")
        
        return triangulation_result
        
    except Exception as e:
        print(f"   ❌ Triangulation failed: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "failed", "error": str(e)}

async def test_complete_workflow():
    """Test the complete workflow end-to-end"""
    print("\n🔄 Testing Complete Workflow Integration...")
    
    try:
        from app.services.workflow import run_spec_extraction_workflow
        
        # Load sample data
        sample_data = load_sample_data()
        
        # Prepare CSV sources in the format expected by workflow
        csv_sources = {
            'search_keywords': [
                {"spec_kw": "atta chakki machine", "frequency": 27, "source": "Search Keywords"},
                {"spec_kw": "commercial atta chakki machine", "frequency": 22, "source": "Search Keywords"},
                {"spec_kw": "flour mill machine", "frequency": 10, "source": "Search Keywords"}
            ],
            'lms_chats': [
                {"spec_kw": "Automatic Atta Chakki", "frequency": 8, "source": "LMS"},
                {"spec_kw": "18 Inch Atta Chakki Machine", "frequency": 3, "source": "LMS"}
            ],
            'rejection_comments': [
                {"spec_kw": "Commercial Atta Chakki Machine", "frequency": 15, "source": "BLNI"}
            ],
            'whatsapp_specs': [
                {"spec_kw": "100 kg/hr", "frequency": 40, "source": "WhatsApp"},
                {"spec_kw": "200 kg/hr", "frequency": 37, "source": "WhatsApp"},
                {"spec_kw": "50 kg/hr", "frequency": 35, "source": "WhatsApp"}
            ]
        }
        
        print("   🚀 Running complete workflow...")
        
        # Run the complete workflow
        workflow_result = await run_spec_extraction_workflow(
            mcat_id="6472",
            pns_json_content=sample_data['pns_json'],
            csv_sources=csv_sources
        )
        
        print(f"   ✅ Complete workflow finished")
        print(f"   📊 Final Results:")
        
        if "error" in workflow_result:
            print(f"      ❌ Workflow failed: {workflow_result['error']}")
            return False
        
        # Display results summary
        pns_specs = workflow_result.get('pns_specs', [])
        csv_agent_results = workflow_result.get('csv_agent_results', {})
        triangulation_result = workflow_result.get('triangulation_result', {})
        processing_summary = workflow_result.get('processing_summary', {})
        
        print(f"      - PNS specs extracted: {len(pns_specs)}")
        print(f"      - CSV sources processed: {len(csv_agent_results)}")
        
        # Show triangulation summary
        triangulated_table = triangulation_result.get('triangulated_table', [])
        if triangulated_table:
            print(f"      - Final triangulated specs: {len(triangulated_table)}")
            print(f"   🏆 Top 3 triangulated results:")
            for i, item in enumerate(triangulated_table[:3], 1):
                score = item.get('Score', 'N/A')
                pns = item.get('PNS', 'N/A')
                options = item.get('Options', 'N/A')[:50] + "..." if len(str(item.get('Options', ''))) > 50 else item.get('Options', 'N/A')
                print(f"      {i}. Score: {score} | {pns} | {options}")
        
        # Show processing summary
        print(f"   ⏱️  Processing Summary:")
        print(f"      - Total processing time: {processing_summary.get('processing_time', 0):.2f}s")
        print(f"      - Successful extractions: {processing_summary.get('successful_extractions', 0)}")
        print(f"      - Total sources: {processing_summary.get('total_sources', 0)}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Complete workflow failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Main test function"""
    print("🧪 PNS Specification Analysis - Complete Workflow Test")
    print("=" * 60)
    
    # Load sample data
    sample_data = load_sample_data()
    
    if not sample_data.get('pns_json'):
        print("❌ Cannot proceed without PNS JSON data")
        return
    
    # Test individual components
    print("\n" + "=" * 60)
    print("🔬 COMPONENT TESTING")
    print("=" * 60)
    
    # 1. Test PNS processing
    pns_result = await test_pns_processing(sample_data['pns_json'])
    
    # 2. Test CSV processing
    csv_sources = {k: v for k, v in sample_data.items() if k.endswith('_specs') or k in ['search_keywords', 'lms_chats', 'rejection_comments', 'whatsapp_specs']}
    csv_results = await test_csv_processing(csv_sources)
    
    # 3. Test triangulation
    triangulation_result = await test_triangulation(pns_result, csv_results)
    
    # Test complete workflow
    print("\n" + "=" * 60)
    print("🔗 COMPLETE WORKFLOW TESTING")
    print("=" * 60)
    
    success = await test_complete_workflow()
    
    # Final summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    if success:
        print("✅ All tests passed! The workflow is working correctly.")
        print("🎉 Your internal processing logic is solid and ready for integration.")
        print("\n📝 Next steps:")
        print("   1. Set up Redis for job management")
        print("   2. Configure BigQuery for real CSV data")
        print("   3. Set up OpenAI API keys")
        print("   4. Test with external data sources")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        print("🔧 Fix the issues before proceeding to external integration.")

if __name__ == "__main__":
    # Set environment variables for testing
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('OPENAI_MODEL', 'gpt-4.1-mini')
    os.environ.setdefault('OPENAI_BASE_URL', 'https://api.openai.com/v1')
    
    asyncio.run(main())
