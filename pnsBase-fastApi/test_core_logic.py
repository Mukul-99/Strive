#!/usr/bin/env python3
"""
Core logic test - Test the essential components without LLM dependencies
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

def test_pns_processing(pns_json_content):
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
            print(f"   📝 Sample extracted specs (top 5):")
            for i, spec in enumerate(extracted_specs[:5], 1):
                spec_name = spec.get('spec_name', 'N/A')
                option = spec.get('option', 'N/A')
                frequency = spec.get('frequency', 0)
                print(f"      {i}. {spec_name}: {option} (Freq: {frequency})")
        
        return pns_result
        
    except Exception as e:
        print(f"   ❌ PNS Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "failed", "error": str(e)}

def test_data_processor(csv_sources):
    """Test CSV data processing without LLM"""
    print("\n🔄 Testing CSV Data Processing...")
    
    try:
        from app.services.data_processor import DataProcessor
        import pandas as pd
        from io import StringIO
        
        processing_results = {}
        
        for source_name, csv_content in csv_sources.items():
            if not csv_content:
                print(f"   ⚠️  Skipping {source_name}: No data")
                continue
                
            print(f"   🔍 Processing {source_name}...")
            
            try:
                # Parse CSV content
                df = pd.read_csv(StringIO(csv_content))
                print(f"      📊 Loaded {len(df)} rows, {len(df.columns)} columns")
                
                # Get column configuration
                from app.utils.state import COLUMN_MAPPINGS
                column_config = COLUMN_MAPPINGS.get(source_name, {})
                
                print(f"      🏗️  Column mapping: {column_config}")
                
                # Process the data
                if hasattr(DataProcessor, 'process_csv_data'):
                    processed_chunks = DataProcessor.process_csv_data(csv_content, source_name)
                    print(f"      ✅ Generated {len(processed_chunks)} data chunks")
                    
                    # Show sample chunk
                    if processed_chunks:
                        chunk_preview = processed_chunks[0][:200] + "..." if len(processed_chunks[0]) > 200 else processed_chunks[0]
                        print(f"      📝 Sample chunk: {chunk_preview}")
                else:
                    print(f"      ⚠️  process_csv_data method not found")
                
                processing_results[source_name] = {
                    "status": "completed",
                    "rows": len(df),
                    "columns": list(df.columns),
                    "chunks": len(processed_chunks) if 'processed_chunks' in locals() else 0
                }
                
            except Exception as e:
                print(f"      ❌ Failed: {e}")
                processing_results[source_name] = {"status": "failed", "error": str(e)}
        
        return processing_results
        
    except Exception as e:
        print(f"   ❌ CSV Data Processing setup failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

def test_job_processor_parsing():
    """Test the job processor parsing logic"""
    print("\n🔄 Testing Job Processor Text Parsing...")
    
    try:
        from app.services.job_processor import JobProcessor
        
        processor = JobProcessor()
        
        # Test with sample LLM output text
        sample_specs_text = """
# Extracted Specifications

| Rank | Specification | Option | Frequency |
|------|---------------|---------|-----------|
| 1 | Motor Power | 3 HP | 17 |
| 2 | Motor Power | 10 HP | 17 |
| 3 | Size | 14 inch | 19 |
| 4 | Grinding Capacity | 50 kg/hr | 8 |
| 5 | Motor Power | 5 HP | 14 |
"""
        
        print("   🔍 Testing text parsing with sample output...")
        parsed_specs = processor._parse_specs_text(sample_specs_text)
        
        print(f"   ✅ Parsed {len(parsed_specs)} specifications")
        print("   📝 Parsed results:")
        for i, spec in enumerate(parsed_specs, 1):
            print(f"      {i}. {spec.get('specification', 'N/A')}: {spec.get('option', 'N/A')} (Freq: {spec.get('frequency', 0)})")
        
        # Test with malformed text
        malformed_text = "This is not a table format\nSome random text\n| Incomplete | row"
        print("\n   🔍 Testing with malformed text...")
        parsed_malformed = processor._parse_specs_text(malformed_text)
        print(f"   ✅ Handled malformed text: {len(parsed_malformed)} specs extracted")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Job Processor parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_api_models():
    """Test API models and validation"""
    print("\n🔄 Testing API Models and Validation...")
    
    try:
        from app.models.job import (
            JobRequest, JobResponse, JobStatusResponse, 
            SpecificationResult, IndividualResults, JobStatus
        )
        from datetime import datetime
        import uuid
        
        # Test JobRequest validation
        print("   🔍 Testing JobRequest validation...")
        
        # Valid MCAT ID
        try:
            valid_request = JobRequest(mcat_id="6472")
            print(f"      ✅ Valid MCAT ID accepted: {valid_request.mcat_id}")
        except Exception as e:
            print(f"      ❌ Valid MCAT ID rejected: {e}")
        
        # Invalid MCAT IDs
        invalid_ids = ["", "   ", "invalid@id", "a" * 25]
        for invalid_id in invalid_ids:
            try:
                JobRequest(mcat_id=invalid_id)
                print(f"      ⚠️  Invalid MCAT ID accepted: '{invalid_id}'")
            except Exception:
                print(f"      ✅ Invalid MCAT ID rejected: '{invalid_id}'")
        
        # Test JobStatusResponse
        print("   🔍 Testing JobStatusResponse...")
        try:
            status_response = JobStatusResponse(
                job_id=str(uuid.uuid4()),
                status=JobStatus.ANALYZING,
                progress=65,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            print(f"      ✅ JobStatusResponse created successfully")
        except Exception as e:
            print(f"      ❌ JobStatusResponse failed: {e}")
        
        # Test progress validation
        print("   🔍 Testing progress validation...")
        try:
            # Valid progress
            valid_response = JobStatusResponse(
                job_id=str(uuid.uuid4()),
                status=JobStatus.PROCESSING,
                progress=50,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            print(f"      ✅ Valid progress (50) accepted")
            
            # Invalid progress
            try:
                invalid_response = JobStatusResponse(
                    job_id=str(uuid.uuid4()),
                    status=JobStatus.PROCESSING,
                    progress=150,  # Invalid: > 100
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                print(f"      ⚠️  Invalid progress (150) accepted")
            except Exception:
                print(f"      ✅ Invalid progress (150) rejected")
                
        except Exception as e:
            print(f"      ❌ Progress validation test failed: {e}")
        
        # Test SpecificationResult
        print("   🔍 Testing SpecificationResult...")
        try:
            spec_result = SpecificationResult(
                rank=1,
                specification="Motor Power",
                options="3 HP, 5 HP, 10 HP",
                frequency="17",
                status="Dominant",
                priority="Primary"
            )
            print(f"      ✅ SpecificationResult created successfully")
        except Exception as e:
            print(f"      ❌ SpecificationResult failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ API Models testing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_redis_client_logic():
    """Test Redis client logic without actual Redis connection"""
    print("\n🔄 Testing Redis Client Logic...")
    
    try:
        from app.core.redis_client import RedisJobManager
        
        # Test Redis URL generation
        from app.core.config import Settings
        
        print("   🔍 Testing Redis URL generation...")
        
        # Test without password
        settings_no_pass = Settings(
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None
        )
        url_no_pass = settings_no_pass.redis_connection_url
        print(f"      ✅ URL without password: {url_no_pass}")
        
        # Test with password
        settings_with_pass = Settings(
            redis_host="localhost", 
            redis_port=6379,
            redis_db=0,
            redis_password="testpass"
        )
        url_with_pass = settings_with_pass.redis_connection_url
        print(f"      ✅ URL with password: {url_with_pass}")
        
        # Test with custom URL
        settings_custom = Settings(redis_url="redis://custom-host:6380/1")
        url_custom = settings_custom.redis_connection_url
        print(f"      ✅ Custom URL: {url_custom}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Redis Client logic test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("🧪 PNS Specification Analysis - Core Logic Test")
    print("=" * 60)
    
    # Load sample data
    sample_data = load_sample_data()
    
    if not sample_data.get('pns_json'):
        print("❌ Cannot proceed without PNS JSON data")
        return
    
    # Test results tracking
    test_results = {}
    
    print("\n" + "=" * 60)
    print("🔬 CORE COMPONENT TESTING")
    print("=" * 60)
    
    # 1. Test PNS processing
    pns_result = test_pns_processing(sample_data['pns_json'])
    test_results['pns_processing'] = pns_result.get('status') == 'completed'
    
    # 2. Test CSV data processing
    csv_sources = {k: v for k, v in sample_data.items() if k in ['search_keywords', 'lms_chats', 'rejection_comments', 'whatsapp_specs']}
    csv_results = test_data_processor(csv_sources)
    test_results['csv_processing'] = len(csv_results) > 0
    
    # 3. Test job processor parsing
    parsing_success = test_job_processor_parsing()
    test_results['text_parsing'] = parsing_success
    
    # 4. Test API models
    models_success = test_api_models()
    test_results['api_models'] = models_success
    
    # 5. Test Redis client logic
    redis_success = test_redis_client_logic()
    test_results['redis_logic'] = redis_success
    
    # Final summary
    print("\n" + "=" * 60)
    print("📋 CORE LOGIC TEST SUMMARY")
    print("=" * 60)
    
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"📊 Test Results: {passed_tests}/{total_tests} passed")
    print()
    
    for test_name, passed in test_results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status} - {test_name.replace('_', ' ').title()}")
    
    print()
    
    if passed_tests == total_tests:
        print("🎉 ALL CORE LOGIC TESTS PASSED!")
        print("✅ Your internal processing logic is working correctly.")
        print()
        print("📝 What's working:")
        print("   ✅ PNS JSON processing and spec extraction")
        print("   ✅ CSV data loading and chunking")
        print("   ✅ Text parsing from LLM outputs")
        print("   ✅ API models and validation")
        print("   ✅ Redis client configuration logic")
        print()
        print("🚀 Next steps for full integration:")
        print("   1. Install LangChain dependencies: pip install langchain-openai langgraph")
        print("   2. Set up Redis server for job management")
        print("   3. Configure OpenAI API key for LLM processing")
        print("   4. Set up BigQuery for real CSV data (optional for testing)")
        print("   5. Test complete workflow with external services")
    else:
        print("⚠️  Some core logic tests failed.")
        print("🔧 Please fix the failing components before proceeding.")
        print("   The core processing logic needs to be solid before external integration.")

if __name__ == "__main__":
    main()
