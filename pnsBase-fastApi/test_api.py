"""
Simple test script for the FastAPI application
"""

import asyncio
import httpx
import json
import time

BASE_URL = "http://localhost:8000"

async def test_api():
    """Test the API endpoints"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        
        print("🧪 Testing PNS Analysis API")
        print("=" * 50)
        
        # Test 1: Health check
        print("1. Testing health endpoint...")
        try:
            response = await client.get(f"{BASE_URL}/api/v1/health")
            if response.status_code == 200:
                print("   ✅ Health check passed")
                print(f"   📊 Response: {response.json()}")
            else:
                print(f"   ❌ Health check failed: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Health check error: {e}")
        
        print()
        
        # Test 2: Create analysis job
        print("2. Testing job creation...")
        try:
            job_data = {"mcat_id": "6472"}
            response = await client.post(f"{BASE_URL}/api/v1/analyze", json=job_data)
            
            if response.status_code == 200:
                job_response = response.json()
                job_id = job_response["job_id"]
                print("   ✅ Job created successfully")
                print(f"   📋 Job ID: {job_id}")
                print(f"   📊 Response: {job_response}")
                
                # Test 3: Check job status
                print()
                print("3. Testing job status...")
                
                # Poll for job status (simplified)
                for i in range(3):
                    await asyncio.sleep(2)  # Wait 2 seconds
                    status_response = await client.get(f"{BASE_URL}/api/v1/jobs/{job_id}/status")
                    
                    if status_response.status_code == 200:
                        status_data = status_response.json()
                        print(f"   📊 Status check {i+1}: {status_data['status']} ({status_data.get('progress', 0)}%)")
                        
                        if status_data['status'] == 'completed':
                            # Test 4: Get results
                            print()
                            print("4. Testing job results...")
                            results_response = await client.get(f"{BASE_URL}/api/v1/jobs/{job_id}/results")
                            
                            if results_response.status_code == 200:
                                results_data = results_response.json()
                                print("   ✅ Results retrieved successfully")
                                print(f"   📊 MCAT ID: {results_data['mcat_id']}")
                                print(f"   📊 PNS Individual Results: {len(results_data['individual_results']['pns_individual'])} items")
                                print(f"   📊 Final Validation: {len(results_data['final_validation'])} items")
                            else:
                                print(f"   ❌ Failed to get results: {results_response.status_code}")
                            break
                        elif status_data['status'] == 'failed':
                            print(f"   ❌ Job failed: {status_data.get('error', 'Unknown error')}")
                            break
                    else:
                        print(f"   ❌ Status check failed: {status_response.status_code}")
                        break
                
            else:
                print(f"   ❌ Job creation failed: {response.status_code}")
                print(f"   📊 Response: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Job creation error: {e}")
        
        print()
        print("🎯 API Test Complete!")

if __name__ == "__main__":
    print("🚀 Starting API tests...")
    print("⚠️  Make sure the API server is running: uvicorn app.main:app --reload")
    print()
    
    try:
        asyncio.run(test_api())
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Test error: {e}")
