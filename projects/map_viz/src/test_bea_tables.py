"""
Test different SAGDP tables to find the right one for GDP per capita
"""

import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BEA_API_KEY = '13B14004-10BE-45AF-BC61-3B7A3F127435'

def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def test_table(table_name, linecode):
    """Test a specific table and linecode"""
    print(f'\n{"="*60}')
    print(f'Testing: {table_name} (LineCode={linecode})')
    print("="*60)
    
    session = create_session()
    
    url = (
        f'https://apps.bea.gov/api/data/'
        f'?UserID={BEA_API_KEY}'
        f'&method=GetData'
        f'&datasetname=Regional'
        f'&TableName={table_name}'
        f'&LineCode={linecode}'
        f'&Year=2022'
        f'&GeoFips=STATE'
        f'&ResultFormat=JSON'
    )
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'Data' in data['BEAAPI']['Results']:
                records = data['BEAAPI']['Results']['Data']
                print(f'✓ Success! Found {len(records)} records')
                
                # Show California example
                ca_record = next((r for r in records if r.get('GeoName') == 'California'), None)
                if ca_record:
                    print(f'\nCalifornia Data:')
                    print(json.dumps(ca_record, indent=2))
                else:
                    print('\nFirst Record:')
                    print(json.dumps(records[0], indent=2))
                
                return True
            elif 'Error' in data['BEAAPI']['Results']:
                error = data['BEAAPI']['Results']['Error']
                print(f'❌ Error: {error.get("APIErrorDescription")}')
                if 'ErrorDetail' in error:
                    print(f'   Detail: {error["ErrorDetail"]}')
                return False
        
        return False
        
    except Exception as e:
        print(f'❌ Request failed: {e}')
        return False

def get_linecodes_for_table(table_name):
    """Get available LineCodes for a table"""
    print(f'\n{"="*60}')
    print(f'Getting LineCodes for {table_name}')
    print("="*60)
    
    session = create_session()
    
    url = (
        f'https://apps.bea.gov/api/data/'
        f'?UserID={BEA_API_KEY}'
        f'&method=GetParameterValuesFiltered'
        f'&datasetname=Regional'
        f'&TargetParameter=LineCode'
        f'&TableName={table_name}'
        f'&ResultFormat=JSON'
    )
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'ParamValue' in data['BEAAPI']['Results']:
                lines = data['BEAAPI']['Results']['ParamValue']
                print(f'\nAvailable LineCodes ({len(lines)} total):')
                
                # Show first 10
                for line in lines[:10]:
                    print(f"  {line.get('Key')}: {line.get('Desc')}")
                
                if len(lines) > 10:
                    print(f'  ... and {len(lines) - 10} more')
                
                return True
        
        return False
        
    except Exception as e:
        print(f'❌ Request failed: {e}')
        return False

def main():
    print('\n' + '='*60)
    print('BEA TABLE TESTING - Find the Right Table for GDP')
    print('='*60)
    
    # First, get LineCodes for SAGDP1 (we know this works)
    get_linecodes_for_table('SAGDP1')
    
    # Test different tables and linecodes
    tests = [
        ('SAGDP1', '1'),   # GDP summary - LineCode 1
        ('SAGDP2', '1'),   # GDP by state - LineCode 1
        ('SAGDP9', '1'),   # Real GDP - LineCode 1
    ]
    
    print('\n' + '='*60)
    print('TESTING TABLES')
    print('='*60)
    
    for table, linecode in tests:
        test_table(table, linecode)
    
    print('\n' + '='*60)
    print('DONE')
    print('='*60)

if __name__ == '__main__':
    main()