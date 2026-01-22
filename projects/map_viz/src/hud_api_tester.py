"""
HUD API Explorer & Tester (Updated with Authentication)
Tests HUD User API endpoints for Fair Market Rents, Income Limits, and CBSA data
Documentation: https://www.huduser.gov/portal/dataset/fmr-api.html

IMPORTANT: You must register and get an API token at:
https://www.huduser.gov/hudapi/public/register
"""

import requests
import json
from datetime import datetime

# ============================================================
# CONFIGURATION - ADD YOUR HUD API TOKEN HERE
# ============================================================
# Sign up at: https://www.huduser.gov/hudapi/public/register
# After login, get token at: Create New Token
HUD_API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI2IiwianRpIjoiYTgzZjE4NWRlODZhOWYwODYwZDZiOGU5ZDRmMGMxZTZlZmU1OWZkZjlkMmI2YmQzNGQ1YjUyODQwZTg2Y2Y1YjlhZTZhOWU1YmMzYWVmMjUiLCJpYXQiOjE3NjkwMjYwNjkuMjExNTI3LCJuYmYiOjE3NjkwMjYwNjkuMjExNTMsImV4cCI6MjA4NDU1ODg2OS4xOTc3NzksInN1YiI6IjExNzg3MiIsInNjb3BlcyI6W119.I3mmAjvo-3Tgm9Y42YAfc86TZO-nPbmEx_JXvJaMwZqMKVzbsg81TnBM5At-NX_xVeWCRv8ddrezx3C2ox8p7w"# ============================================================

# HUD API Base URLs
HUD_FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"
HUD_IL_BASE = "https://www.huduser.gov/hudapi/public/il"

def get_headers():
    """Return headers with authentication token"""
    if HUD_API_TOKEN == "YOUR_HUD_API_TOKEN_HERE":
        print("\n⚠️  WARNING: No HUD API token configured!")
        print("   Please sign up at: https://www.huduser.gov/hudapi/public/register")
        print("   Then add your token to the HUD_API_TOKEN variable in this script")
        return None
    
    return {
        "Authorization": f"Bearer {HUD_API_TOKEN}",
        "Accept": "application/json"
    }

def test_list_states():
    """Get list of all states"""
    print(f"\n{'='*60}")
    print(f"Testing List States")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_FMR_BASE}/listStates"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Handle both list and dict responses
        if isinstance(data, dict):
            state_list = data.get('data', [])
        else:
            state_list = data
        
        print(f"\n✅ SUCCESS - Retrieved {len(state_list)} states")
        
        if state_list:
            print(f"\n  Sample states (first 5):")
            for state in state_list[:5]:
                print(f"    {state['state_name']} ({state['state_code']}) - Num: {state['state_num']}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_list_metro_areas(year=2024):
    """
    Get list of all Metropolitan Areas (CBSAs)
    This is the key endpoint for CBSA data!
    """
    print(f"\n{'='*60}")
    print(f"Testing List Metropolitan Areas (CBSAs)")
    print(f"Year: {year}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_FMR_BASE}/listMetroAreas"
    params = {'year': year} if year else {}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Handle both list and dict responses
        if isinstance(data, dict):
            metro_list = data.get('data', [])
        else:
            metro_list = data
        
        print(f"\n✅ SUCCESS - Retrieved {len(metro_list)} metro areas")
        
        if metro_list:
            print(f"\n  Sample metro areas (first 10):")
            for metro in metro_list[:10]:
                print(f"    {metro['area_name']}")
                print(f"      CBSA Code: {metro['cbsa_code']}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_list_counties(state_code, year=2024):
    """Get list of all counties in a state"""
    print(f"\n{'='*60}")
    print(f"Testing List Counties")
    print(f"State: {state_code}, Year: {year}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_FMR_BASE}/listCounties/{state_code}"
    params = {'year': year} if year else {}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Handle both list and dict responses
        if isinstance(data, dict):
            county_list = data.get('data', [])
        else:
            county_list = data
        
        print(f"\n✅ SUCCESS - Retrieved {len(county_list)} counties")
        
        if county_list:
            print(f"\n  Sample counties (first 10):")
            for county in county_list[:10]:
                print(f"    {county['county_name']} - FIPS: {county['fips_code']}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_fmr_by_cbsa(cbsa_code, year=2024):
    """
    Test Fair Market Rent by CBSA (Metro Area)
    
    Args:
        cbsa_code: CBSA code (e.g., 'METRO31080M31080' for LA)
        year: Year for data
    """
    print(f"\n{'='*60}")
    print(f"Testing FMR by CBSA (Metro Area)")
    print(f"CBSA: {cbsa_code}, Year: {year}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_FMR_BASE}/data/{cbsa_code}"
    params = {'year': year}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"\n✅ SUCCESS - Retrieved FMR data for CBSA")
        print(json.dumps(data, indent=4))
        
        # Parse key fields
        if 'data' in data and 'basicdata' in data['data']:
            basic = data['data']['basicdata']
            print(f"\n📊 Key Metrics:")
            print(f"  Metro: {data['data'].get('metro_name', 'N/A')}")
            print(f"  0-Bedroom FMR: ${basic.get('Efficiency', 'N/A')}")
            print(f"  1-Bedroom FMR: ${basic.get('One-Bedroom', 'N/A')}")
            print(f"  2-Bedroom FMR: ${basic.get('Two-Bedroom', 'N/A')}")
            print(f"  3-Bedroom FMR: ${basic.get('Three-Bedroom', 'N/A')}")
            print(f"  4-Bedroom FMR: ${basic.get('Four-Bedroom', 'N/A')}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_fmr_by_state(state_code, year=2024):
    """Test Fair Market Rent API - Get FMR data by state"""
    print(f"\n{'='*60}")
    print(f"Testing FMR API - State Level")
    print(f"State: {state_code}, Year: {year}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_FMR_BASE}/statedata/{state_code}"
    params = {'year': year}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"\n✅ SUCCESS - Retrieved FMR data for {state_code}")
        
        if 'data' in data:
            metro_count = len(data['data'].get('metroareas', []))
            county_count = len(data['data'].get('counties', []))
            
            print(f"  Year: {data['data'].get('year')}")
            print(f"  Metro areas: {metro_count}")
            print(f"  Counties: {county_count}")
            
            if data['data'].get('metroareas'):
                print(f"\n  Sample metro area (first one):")
                sample = data['data']['metroareas'][0]
                print(json.dumps(sample, indent=4))
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_income_limits_by_state(state_code, year=2024):
    """Test Income Limits API - Get income limits by state"""
    print(f"\n{'='*60}")
    print(f"Testing Income Limits API - State Level")
    print(f"State: {state_code}, Year: {year}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_IL_BASE}/statedata/{state_code}"
    params = {'year': year}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"\n✅ SUCCESS - Retrieved income limits for {state_code}")
        
        if 'data' in data:
            print(f"\n  Total records: {len(data['data'])}")
            if data['data']:
                print(f"\n  Sample record (first metro area):")
                sample = data['data'][0]
                print(json.dumps(sample, indent=4))
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_income_limits_by_cbsa(entity_id, year=2024):
    """
    Test Income Limits by CBSA/County
    
    Args:
        entity_id: CBSA code or county FIPS code
        year: Year for data
    """
    print(f"\n{'='*60}")
    print(f"Testing Income Limits by Entity (CBSA/County)")
    print(f"Entity: {entity_id}, Year: {year}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    url = f"{HUD_IL_BASE}/data/{entity_id}"
    params = {'year': year}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"\n✅ SUCCESS - Retrieved income limits")
        print(json.dumps(data, indent=4))
        
        # Parse key fields
        if 'data' in data:
            il_data = data['data']
            print(f"\n📊 Key Metrics:")
            print(f"  Area: {il_data.get('metro_name', il_data.get('county_name', 'N/A'))}")
            print(f"  Median Family Income: ${il_data.get('median_income', 'N/A')}")
            
            # Show 80% AMI levels
            if 'low' in il_data:
                print(f"\n  80% AMI (Low Income Limits):")
                for i in range(1, 5):
                    val = il_data['low'].get(f'il80_p{i}')
                    if val:
                        print(f"    {i} person: ${val}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERROR: {e}")
        return None

def test_all_years_availability(state_code='CA'):
    """Test which years are available for a given state"""
    print(f"\n{'='*60}")
    print(f"Testing Year Availability for {state_code}")
    print(f"{'='*60}")
    
    headers = get_headers()
    if not headers:
        return None
    
    current_year = datetime.now().year
    available_years = {'fmr': [], 'income_limits': []}
    
    # Test FMR
    print(f"\nTesting FMR availability...")
    for year in range(current_year, current_year - 10, -1):
        url = f"{HUD_FMR_BASE}/statedata/{state_code}"
        try:
            response = requests.get(url, headers=headers, params={'year': year}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data']:
                    available_years['fmr'].append(year)
                    print(f"  ✓ {year}: Available")
                else:
                    print(f"  ✗ {year}: No data")
            else:
                print(f"  ✗ {year}: Failed ({response.status_code})")
        except:
            print(f"  ✗ {year}: Error")
    
    # Test Income Limits
    print(f"\nTesting Income Limits availability...")
    for year in range(current_year, current_year - 10, -1):
        url = f"{HUD_IL_BASE}/statedata/{state_code}"
        try:
            response = requests.get(url, headers=headers, params={'year': year}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data']:
                    available_years['income_limits'].append(year)
                    print(f"  ✓ {year}: Available")
                else:
                    print(f"  ✗ {year}: No data")
            else:
                print(f"  ✗ {year}: Failed ({response.status_code})")
        except:
            print(f"  ✗ {year}: Error")
    
    print(f"\n📅 Summary:")
    print(f"  FMR: Latest = {max(available_years['fmr']) if available_years['fmr'] else 'None'}")
    print(f"  Income Limits: Latest = {max(available_years['income_limits']) if available_years['income_limits'] else 'None'}")
    
    return available_years

def explore_cbsa_data(state_code='CA', year=2024):
    """
    Comprehensive exploration of CBSA/Metro Area data
    """
    print(f"\n{'='*70}")
    print(f"COMPREHENSIVE CBSA/METRO AREA DATA EXPLORATION")
    print(f"State: {state_code}, Year: {year}")
    print(f"{'='*70}")
    
    headers = get_headers()
    if not headers:
        return None
    
    results = {}
    
    # 1. List all Metro Areas
    print(f"\n\n1️⃣ LIST ALL METRO AREAS (CBSAs)")
    print(f"{'─'*70}")
    metro_list = test_list_metro_areas(year)
    results['metro_list'] = metro_list
    
    # 2. Get detailed FMR for a specific CBSA (LA Metro example)
    if metro_list:
        # Handle both list and dict responses
        metros = metro_list.get('data', []) if isinstance(metro_list, dict) else metro_list
        
        # Find LA Metro
        la_metro = next((m for m in metros if 'Los Angeles' in m['area_name']), None)
        
        if la_metro:
            print(f"\n\n2️⃣ DETAILED FMR FOR CBSA (LA Metro Example)")
            print(f"{'─'*70}")
            cbsa_fmr = test_fmr_by_cbsa(la_metro['cbsa_code'], year)
            results['cbsa_fmr'] = cbsa_fmr
            
            print(f"\n\n3️⃣ INCOME LIMITS FOR CBSA (LA Metro Example)")
            print(f"{'─'*70}")
            cbsa_il = test_income_limits_by_cbsa(la_metro['cbsa_code'], year)
            results['cbsa_il'] = cbsa_il
    
    # 3. State-level summary
    print(f"\n\n4️⃣ STATE-LEVEL FMR SUMMARY")
    print(f"{'─'*70}")
    state_fmr = test_fmr_by_state(state_code, year)
    results['state_fmr'] = state_fmr
    
    return results

def main():
    """Main test suite"""
    print("╔═════════════════════════════════════════════════════════╗")
    print("║         HUD API EXPLORER & TESTER v2.0                 ║")
    print("║  Now with Authentication & CBSA Support                ║")
    print("╚═════════════════════════════════════════════════════════╝")
    
    # Check if token is configured
    if HUD_API_TOKEN == "YOUR_HUD_API_TOKEN_HERE":
        print("\n" + "="*70)
        print("⚠️  SETUP REQUIRED")
        print("="*70)
        print("\nYou need to register for a HUD API token:")
        print("\n1. Go to: https://www.huduser.gov/hudapi/public/register")
        print("2. Sign up for an account and select 'FMR and IL Dataset API'")
        print("3. Confirm your email")
        print("4. Log in at: https://www.huduser.gov/hudapi/public/login")
        print("5. Click 'Create New Token'")
        print("6. Copy your token and paste it into the HUD_API_TOKEN variable")
        print("\nOnce you have your token, run this script again!")
        print("="*70)
        return
    
    # Test 1: List states
    print("\n\n" + "="*70)
    print("TEST 1: List States")
    print("="*70)
    test_list_states()
    
    # Test 2: Check year availability
    print("\n\n" + "="*70)
    print("TEST 2: Year Availability Check")
    print("="*70)
    available = test_all_years_availability('CA')
    
    # Test 3: Comprehensive CBSA exploration
    print("\n\n" + "="*70)
    print("TEST 3: Comprehensive CBSA/Metro Area Exploration")
    print("="*70)
    cbsa_results = explore_cbsa_data('CA', 2024)
    
    # Test 4: Multi-state testing
    print("\n\n" + "="*70)
    print("TEST 4: Multi-State Testing")
    print("="*70)
    
    test_states = ['CA', 'NY', 'TX', 'FL']
    for state in test_states:
        print(f"\n{'─'*70}")
        print(f"Testing {state}...")
        print(f"{'─'*70}")
        test_list_counties(state, 2024)
    
    print("\n\n╔═════════════════════════════════════════════════════════╗")
    print("║              ALL TESTS COMPLETE                         ║")
    print("╚═════════════════════════════════════════════════════════╝")
    
    print("\n📋 KEY FINDINGS:")
    print("  • HUD API now requires authentication (token)")
    print("  • CBSA data available via /listMetroAreas endpoint")
    print("  • Can get FMR and Income Limits by CBSA code")
    print("  • State-level data includes all metros and counties")
    print("  • County-level data available via FIPS codes")
    
    print("\n📋 NEXT STEPS:")
    print("  1. Review the CBSA codes and structures")
    print("  2. Decide which geographic levels to fetch (State/CBSA/County)")
    print("  3. Ready to integrate into master script!")

if __name__ == '__main__':
    main()