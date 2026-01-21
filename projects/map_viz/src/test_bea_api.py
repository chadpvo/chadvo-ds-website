"""
BEA API Test Script
Run with: python test_bea_api.py

This script tests different BEA API endpoints to find which one works for state GDP data.
"""

import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Your BEA API Key
BEA_API_KEY = '13B14004-10BE-45AF-BC61-3B7A3F127435'

def create_session():
    """Create session with retry logic"""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def test_1_regional_dataset():
    """Test 1: Regional dataset - SAGDP2N table"""
    print('\n' + '='*60)
    print('TEST 1: Regional Dataset - SAGDP2N (GDP by state)')
    print('='*60)
    
    session = create_session()
    
    url = (
        f'https://apps.bea.gov/api/data/'
        f'?UserID={BEA_API_KEY}'
        f'&method=GetData'
        f'&datasetname=Regional'
        f'&TableName=SAGDP2N'
        f'&LineCode=1'
        f'&Year=2022'
        f'&GeoFips=STATE'
        f'&ResultFormat=JSON'
    )
    
    print(f'URL: {url}\n')
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Pretty print the full response
        print('Full Response:')
        print(json.dumps(data, indent=2)[:2000])  # First 2000 chars
        print('\n...(truncated)\n')
        
        # Check for data
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'Data' in data['BEAAPI']['Results']:
                records = data['BEAAPI']['Results']['Data']
                print(f'✓ Found {len(records)} data records')
                
                # Show first 3 records
                print('\nFirst 3 Records:')
                for i, record in enumerate(records[:3]):
                    print(f'\nRecord {i+1}:')
                    print(json.dumps(record, indent=2))
                
                return True
            elif 'Error' in data['BEAAPI']['Results']:
                error = data['BEAAPI']['Results']['Error']
                print(f'❌ BEA API Error: {error}')
                return False
        
        print('❌ Unexpected response structure')
        return False
        
    except Exception as e:
        print(f'❌ Request failed: {e}')
        return False


def test_2_get_parameter_values():
    """Test 2: Check what tables are available in Regional dataset"""
    print('\n' + '='*60)
    print('TEST 2: Get Available Tables in Regional Dataset')
    print('='*60)
    
    session = create_session()
    
    url = (
        f'https://apps.bea.gov/api/data/'
        f'?UserID={BEA_API_KEY}'
        f'&method=GetParameterValues'
        f'&datasetname=Regional'
        f'&ParameterName=TableName'
        f'&ResultFormat=JSON'
    )
    
    print(f'URL: {url}\n')
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'ParamValue' in data['BEAAPI']['Results']:
                tables = data['BEAAPI']['Results']['ParamValue']
                
                # Filter for GDP tables
                gdp_tables = [t for t in tables if 'GDP' in t.get('Desc', '').upper()]
                
                print(f'✓ Found {len(gdp_tables)} GDP-related tables:\n')
                for table in gdp_tables[:10]:
                    print(f"  {table.get('Key')}: {table.get('Desc')}")
                
                return True
        
        print('❌ Could not retrieve table list')
        return False
        
    except Exception as e:
        print(f'❌ Request failed: {e}')
        return False


def test_3_specific_state():
    """Test 3: Try to get California's GDP specifically"""
    print('\n' + '='*60)
    print('TEST 3: Get California GDP (GeoFips=06000)')
    print('='*60)
    
    session = create_session()
    
    url = (
        f'https://apps.bea.gov/api/data/'
        f'?UserID={BEA_API_KEY}'
        f'&method=GetData'
        f'&datasetname=Regional'
        f'&TableName=SAGDP2N'
        f'&LineCode=1'
        f'&Year=2022'
        f'&GeoFips=06000'
        f'&ResultFormat=JSON'
    )
    
    print(f'URL: {url}\n')
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'Data' in data['BEAAPI']['Results']:
                records = data['BEAAPI']['Results']['Data']
                print(f'✓ Found {len(records)} record(s) for California')
                
                for record in records:
                    print('\nCalifornia GDP Data:')
                    print(json.dumps(record, indent=2))
                
                return True
        
        print('❌ No data found')
        return False
        
    except Exception as e:
        print(f'❌ Request failed: {e}')
        return False


def test_4_alternative_table():
    """Test 4: Try SAGDP1 table (GDP summary)"""
    print('\n' + '='*60)
    print('TEST 4: Alternative Table - SAGDP1 (GDP Summary)')
    print('='*60)
    
    session = create_session()
    
    url = (
        f'https://apps.bea.gov/api/data/'
        f'?UserID={BEA_API_KEY}'
        f'&method=GetData'
        f'&datasetname=Regional'
        f'&TableName=SAGDP1'
        f'&LineCode=1'
        f'&Year=2022'
        f'&GeoFips=STATE'
        f'&ResultFormat=JSON'
    )
    
    print(f'URL: {url}\n')
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'Data' in data['BEAAPI']['Results']:
                records = data['BEAAPI']['Results']['Data']
                print(f'✓ Found {len(records)} records')
                
                # Show first record
                if records:
                    print('\nFirst Record:')
                    print(json.dumps(records[0], indent=2))
                
                return True
            elif 'Error' in data['BEAAPI']['Results']:
                error = data['BEAAPI']['Results']['Error']
                print(f'❌ BEA API Error: {error}')
        
        return False
        
    except Exception as e:
        print(f'❌ Request failed: {e}')
        return False


def main():
    """Run all tests"""
    print('\n' + '='*60)
    print('BEA API TESTING SUITE')
    print('='*60)
    print(f'API Key: {BEA_API_KEY[:10]}...')
    
    results = []
    
    # Run tests
    results.append(('Test 1: SAGDP2N Table', test_1_regional_dataset()))
    results.append(('Test 2: List GDP Tables', test_2_get_parameter_values()))
    results.append(('Test 3: California Specific', test_3_specific_state()))
    results.append(('Test 4: SAGDP1 Alternative', test_4_alternative_table()))
    
    # Summary
    print('\n' + '='*60)
    print('TEST SUMMARY')
    print('='*60)
    for test_name, passed in results:
        status = '✓ PASS' if passed else '❌ FAIL'
        print(f'{status}: {test_name}')
    
    print('\n' + '='*60)


if __name__ == '__main__':
    main()