"""
Data Source Connectivity Tester (Fixed V2)
Verifies access to IRS, Zillow, HUD, and FCC data sources.
Includes fixes for FCC WAF (Web Application Firewall) and Zillow file paths.
"""

import requests
import json
import sys
import time

# ============================================================
# CONFIGURATION
# ============================================================
HUD_API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI2IiwianRpIjoiYTgzZjE4NWRlODZhOWYwODYwZDZiOGU5ZDRmMGMxZTZlZmU1OWZkZjlkMmI2YmQzNGQ1YjUyODQwZTg2Y2Y1YjlhZTZhOWU1YmMzYWVmMjUiLCJpYXQiOjE3NjkwMjYwNjkuMjExNTI3LCJuYmYiOjE3NjkwMjYwNjkuMjExNTMsImV4cCI6MjA4NDU1ODg2OS4xOTc3NzksInN1YiI6IjExNzg3MiIsInNjb3BlcyI6W119.I3mmAjvo-3Tgm9Y42YAfc86TZO-nPbmEx_JXvJaMwZqMKVzbsg81TnBM5At-NX_xVeWCRv8ddrezx3C2ox8p7w"

# Test Zip Code (Beverly Hills, CA)
TEST_ZIP = "90210"

def print_header(title):
    print("\n" + "="*70)
    print(f"TESTING: {title}")
    print("="*70)

def test_hud_api():
    print_header("HUD USPS Vacancy API")
    url = "https://www.huduser.gov/hudapi/public/usps"
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    params = {
        'type': '1', # Type 1 = Zip Code level
        'query': TEST_ZIP,
        'year': '2023',
        'quarter': '1'
    }
    
    print(f"Connecting to: {url}...")
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        print(f"Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            print("✅ SUCCESS! Data retrieved.")
            if 'data' in data and 'results' in data['data'] and len(data['data']['results']) > 0:
                 print(json.dumps(data['data']['results'][0], indent=2))
            else:
                 print("   (Response valid, but no specific data for this query)")
        elif resp.status_code == 404:
            print("⚠ Connection successful, but no data found for this Zip/Year.")
        else:
            print(f"❌ Failed. Reason: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_irs_soi():
    print_header("IRS Statistics of Income (Public CSV)")
    # 2021 Individual Income Tax Zip Code Data (Latest stable year often available)
    url = "https://www.irs.gov/pub/irs-soi/21zpallagi.csv"
    
    print(f"Attempting to stream header from: {url}")
    try:
        # Stream=True prevents downloading the whole 200MB+ file
        with requests.get(url, stream=True, timeout=15) as r:
            r.raise_for_status()
            print("✅ Connection Established!")
            
            # Read just the first chunk to get headers
            first_chunk = next(r.iter_content(chunk_size=1024)).decode('utf-8')
            first_line = first_chunk.split('\n')[0]
            print(f"\nHeader Sample:\n{first_line[:100]}...")
            
            if "STATE" in first_line and "zipcode" in first_line:
                print("\n✅ Valid IRS Data Structure detected.")
            else:
                print("\n⚠ Warning: Unexpected file structure.")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_zillow_research():
    print_header("Zillow Research Data (ZHVI CSV)")
    # UPDATED URL: Testing the 'Zip Code' file instead of 'Metro'
    # "ZHVI All Homes (SFR + Condo/Co-op), Smoothed, Seasonally Adjusted"
    url = "https://files.zillowstatic.com/research/public_v2/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    
    print(f"Attempting to stream header from: {url}")
    try:
        # Zillow requires a browser-like User-Agent
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        with requests.get(url, stream=True, headers=headers, timeout=15) as r:
            if r.status_code == 200:
                print("✅ Connection Established!")
                first_chunk = next(r.iter_content(chunk_size=1024)).decode('utf-8')
                first_line = first_chunk.split('\n')[0]
                print(f"\nHeader Sample:\n{first_line[:100]}...")
                
                if "RegionName" in first_line:
                    print("\n✅ Valid Zillow Data Structure detected.")
            else:
                print(f"❌ Error: {r.status_code} - {r.reason}")
                print("   (If 404, Zillow may have updated the filename. Check: https://www.zillow.com/research/data/)")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_fcc_broadband():
    print_header("FCC National Broadband Map API")
    # Endpoint: 'listAsOfDates' usually works to test public access
    url = "https://broadbandmap.fcc.gov/api/public/map/listAsOfDates"
    
    # FIXED HEADERS: FCC WAF requires 'Referer' and 'Origin' to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://broadbandmap.fcc.gov/home',
        'Origin': 'https://broadbandmap.fcc.gov'
    }
    
    print(f"Ping Test: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        print(f"Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            print("✅ FCC API is reachable!")
            data = resp.json()
            if 'data' in data:
                print(f"Sample Data (Available Dates): {data['data'][:2]}") 
        elif resp.status_code == 403 or resp.status_code == 401:
             print(f"❌ Access Denied ({resp.status_code}).")
             print("   The FCC API likely detects this script as a bot. Use the CSV download method instead.")
        else:
            print(f"❌ Failed to connect to FCC API. Response: {resp.text[:100]}...")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    print("STARTING MULTI-SOURCE CONNECTIVITY TEST (V2)...")
    
    test_hud_api()
    time.sleep(1)
    
    test_irs_soi()
    time.sleep(1)
    
    test_zillow_research()
    time.sleep(1)
    
    test_fcc_broadband()
    
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)

if __name__ == '__main__':
    main()