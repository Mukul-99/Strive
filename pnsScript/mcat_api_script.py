import requests
import csv
import json
import os
from datetime import datetime
from typing import List, Dict, Any

# Hardcoded MCAT IDs array
MCAT_IDS = [
   40871
]

# API Configuration
API_BASE_URL = "https://extract-product-936671953004.asia-south1.run.app"
API_ENDPOINT = "/process-mcat-from-gcs"

def create_output_directory():
    """Create output directory if it doesn't exist"""
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def generate_csv_filename():
    """Generate CSV filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"mcat_results_{timestamp}.csv"

def call_mcat_api(mcat_id: int) -> Dict[str, Any]:
    """Call the MCAT API for a given MCAT ID"""
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    params = {"mcat_id": mcat_id}
    headers = {"Content-Type": "application/json"}
    data = {}
    
    # Log the actual request
    print("\n" + "="*50)
    print("REQUEST:")
    print(f"URL: {url}")
    print(f"Method: POST")
    print(f"Params: {params}")
    print(f"Headers: {headers}")
    print(f"Body: {data}")
    print("="*50)
    
    try:
        response = requests.post(url, params=params, headers=headers, json=data, timeout=60)
        
        # Log the actual response
        print("\nRESPONSE:")
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
        try:
            response_json = response.json()
            print(f"Body: {json.dumps(response_json, indent=2)}")
        except:
            print(f"Body (text): {response.text}")
        print("="*50)
        
        response.raise_for_status()  # Raise exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"\nERROR: {str(e)}")
        print("="*50)
        return {"error": f"API call failed: {str(e)}"}

def extract_signed_url(api_response: Dict[str, Any]) -> str:
    """Extract signed_url from API response"""
    try:
        # Check if gcs_urls exists and has signed_url
        if "gcs_urls" in api_response and "signed_url" in api_response["gcs_urls"]:
            signed_url = api_response["gcs_urls"]["signed_url"]
            # Check if signed_url is not empty
            if signed_url and signed_url.strip():
                return signed_url
            else:
                return "NA"
        else:
            return "NA"
    except Exception as e:
        return f"Error extracting signed_url: {str(e)}"

def process_mcat_ids(mcat_ids: List[int]) -> List[Dict[str, Any]]:
    """Process all MCAT IDs and return results"""
    results = []
    
    print(f"Starting processing for {len(mcat_ids)} MCAT IDs...")
    
    for i, mcat_id in enumerate(mcat_ids, 1):
        print(f"Processing MCAT ID {mcat_id} ({i}/{len(mcat_ids)})...")
        
        # Call API
        api_response = call_mcat_api(mcat_id)
        
        # Extract signed_url status
        signed_url_status = extract_signed_url(api_response)
        
        # Store result
        result = {
            "mcat_id": mcat_id,
            "signed_url_status": signed_url_status
        }
        results.append(result)
        
        print(f"  → signed_url_status: {signed_url_status}")
    
    return results

def save_results_to_csv(results: List[Dict[str, Any]], output_dir: str, filename: str):
    """Save results to CSV file"""
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['mcat_id', 'signed_url_status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data
        for result in results:
            writer.writerow(result)
    
    print(f"\n✅ Results saved to: {filepath}")
    return filepath

def main():
    """Main function to orchestrate the process"""
    print("🚀 MCAT API Processing Script")
    print("=" * 50)
    
    # Create output directory
    output_dir = create_output_directory()
    print(f"📁 Output directory: {output_dir}")
    
    # Generate filename
    filename = generate_csv_filename()
    print(f"📄 Output filename: {filename}")
    
    # Process MCAT IDs
    results = process_mcat_ids(MCAT_IDS)
    
    # Save results to CSV
    filepath = save_results_to_csv(results, output_dir, filename)
    
    # Summary
    total_processed = len(results)
    successful_urls = sum(1 for r in results if r["signed_url_status"] != "NA" and not r["signed_url_status"].startswith("Error"))
    na_count = sum(1 for r in results if r["signed_url_status"] == "NA")
    error_count = sum(1 for r in results if r["signed_url_status"].startswith("Error"))
    
    print("\n📊 Processing Summary:")
    print(f"  Total MCAT IDs processed: {total_processed}")
    print(f"  Successful signed URLs: {successful_urls}")
    print(f"  NA responses: {na_count}")
    print(f"  Errors: {error_count}")
    
    print(f"\n✅ Script completed successfully!")
    print(f"📄 Results saved to: {filepath}")

if __name__ == "__main__":
    main() 