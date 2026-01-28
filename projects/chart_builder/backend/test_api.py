#!/usr/bin/env python3
"""
Local testing script for Redfin API
Run this before deploying to verify everything works
"""

import requests
import json
import sys

API_URL = "http://localhost:5000/api"

def test_endpoint(name, url, method="GET", data=None):
    """Test an API endpoint"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    print(f"Method: {method}")
    
    try:
        if method == "GET":
            response = requests.get(url, timeout=10)
        else:
            response = requests.post(url, json=data, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success!")
            print(f"Response preview:")
            print(json.dumps(result, indent=2)[:500] + "...")
            return True
        else:
            print(f"❌ Failed!")
            print(f"Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Connection Error - Is the Flask server running?")
        print(f"   Start it with: python app.py")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("🧪 Redfin API Test Suite")
    print("="*60)
    
    results = []
    
    # Test 1: Health Check
    results.append(test_endpoint(
        "Health Check",
        f"{API_URL}/health"
    ))
    
    # Test 2: Schema
    results.append(test_endpoint(
        "Get Schema",
        f"{API_URL}/schema"
    ))
    
    # Test 3: States
    results.append(test_endpoint(
        "Get States",
        f"{API_URL}/states"
    ))
    
    # Test 4: Basic Data Query
    results.append(test_endpoint(
        "Basic Data Query",
        f"{API_URL}/data?limit=5"
    ))
    
    # Test 5: Filtered Data Query
    results.append(test_endpoint(
        "Filtered Data Query (California)",
        f"{API_URL}/data?state=CA&limit=10"
    ))
    
    # Test 6: Aggregated Data Query
    # Note: You'll need to update column names based on your actual data
    results.append(test_endpoint(
        "Aggregated Data Query",
        f"{API_URL}/data?state=CA&metric=median_sale_price&group_by=period_begin&limit=20"
    ))
    
    # Test 7: Advanced Aggregation
    results.append(test_endpoint(
        "Advanced Aggregation (POST)",
        f"{API_URL}/aggregate",
        method="POST",
        data={
            "filters": {"state": "CA"},
            "group_by": ["period_begin"],
            "aggregations": {
                "median_sale_price": ["mean", "median"]
            }
        }
    ))
    
    # Summary
    print("\n" + "="*60)
    print("📊 Test Summary")
    print("="*60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✅ All tests passed! Ready to deploy!")
        sys.exit(0)
    else:
        print("❌ Some tests failed. Check the output above.")
        sys.exit(1)

if __name__ == "__main__":
    main()