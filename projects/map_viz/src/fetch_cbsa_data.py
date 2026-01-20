"""
Fetch CBSA-Level Economic Data
Run with: python fetch_cbsa_data.py

Fetches:
- Median Household Income (Census ACS)
- Employment Rate (Census ACS)

Saves to: projects/map_viz/data/cbsas_economic_data.json
"""

import requests
import json
from pathlib import Path
import time

# API Keys
CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'


def fetch_median_income():
    """Fetch Median Household Income for all CBSAs from Census ACS"""
    print('Fetching median household income for CBSAs from Census ACS...')
    
    # ACS 5-Year Estimates (2022) - Table B19013 (Median Household Income)
    # Using 'metropolitan statistical area/micropolitan statistical area' geography
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B19013_001E&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={CENSUS_API_KEY}'
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # First row is headers
    headers = data[0]
    rows = data[1:]
    
    result = {}
    for row in rows:
        cbsa_code = row[2]  # CBSA code is in the last column
        name = row[0]  # Full CBSA name (e.g., "New York-Newark-Jersey City, NY-NJ-PA Metro Area")
        
        # Clean up the name (remove " Metro Area" or " Micro Area" suffix)
        if name.endswith(' Metro Area'):
            name = name[:-11]
            cbsa_type = 'Metropolitan Statistical Area'
        elif name.endswith(' Micro Area'):
            name = name[:-11]
            cbsa_type = 'Micropolitan Statistical Area'
        else:
            cbsa_type = 'Statistical Area'
        
        income = int(row[1]) if row[1] and row[1] not in ['-666666666', 'null'] else None
        
        result[cbsa_code] = {
            'name': name,
            'cbsaCode': cbsa_code,
            'type': cbsa_type,
            'medianIncome': income
        }
    
    print(f'✓ Fetched median income for {len(result)} CBSAs')
    return result


def fetch_employment_rate():
    """Fetch Employment Rate for all CBSAs from Census ACS"""
    print('Fetching employment rate for CBSAs from Census ACS...')
    
    # ACS 5-Year Estimates (2022) - Table S2301
    url = f'https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S2301_C03_001E&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={CENSUS_API_KEY}'
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    headers = data[0]
    rows = data[1:]
    
    result = {}
    for row in rows:
        cbsa_code = row[2]
        employment_rate = float(row[1]) if row[1] and row[1] != 'null' else None
        
        result[cbsa_code] = {
            'employmentRate': employment_rate
        }
    
    print(f'✓ Fetched employment rate for {len(result)} CBSAs')
    return result


def merge_data(income_data, employment_data):
    """Merge all data sources"""
    print('Merging all data sources...')
    
    merged = {}
    
    # Use income_data as base
    for cbsa_code, cbsa_info in income_data.items():
        merged[cbsa_code] = {
            'cbsaCode': cbsa_code,
            'name': cbsa_info['name'],
            'type': cbsa_info['type'],
            'medianIncome': cbsa_info.get('medianIncome'),
            'employmentRate': employment_data.get(cbsa_code, {}).get('employmentRate')
        }
    
    print(f'✓ Merged data for {len(merged)} CBSAs')
    return merged


def save_to_file(data, filename):
    """Save to JSON file"""
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    filepath = data_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f'✓ Data saved to: {filepath}')


def main():
    """Main execution"""
    print('=== Starting CBSA Economic Data Fetch ===\n')
    
    try:
        # Fetch all data
        income_data = fetch_median_income()
        time.sleep(1)  # Be nice to the API
        employment_data = fetch_employment_rate()
        
        # Merge data
        merged_data = merge_data(income_data, employment_data)
        
        # Save to file
        save_to_file(merged_data, 'cbsas_economic_data.json')
        
        # Print sample data (if NYC exists - code 35620)
        print('\n=== Sample Data (New York-Newark-Jersey City) ===')
        if '35620' in merged_data:
            print(json.dumps(merged_data['35620'], indent=2))
        else:
            # Print first CBSA as sample
            first_key = list(merged_data.keys())[0]
            print(json.dumps(merged_data[first_key], indent=2))
        
        print('\n=== Fetch Complete ===')
        print(f'Total CBSAs processed: {len(merged_data)}')
        
    except Exception as error:
        print(f'ERROR: {error}')
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()