"""
Fetch State-Level Economic Data
Run with: python fetch_state_data.py

Fetches:
- Median Household Income (Census ACS)
- Employment Rate (Census ACS)
- GDP (BEA)

Saves to: projects/map_viz/data/states_economic_data.json
"""

import requests
import json
import os
from pathlib import Path

# API Keys
CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'

# State FIPS codes (for reference and validation)
STATE_FIPS = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware',
    '11': 'District of Columbia', '12': 'Florida', '13': 'Georgia', '15': 'Hawaii',
    '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa',
    '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine',
    '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska',
    '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico',
    '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island',
    '45': 'South Carolina', '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas',
    '49': 'Utah', '50': 'Vermont', '51': 'Virginia', '53': 'Washington',
    '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming'
}


def fetch_median_income():
    """Fetch Median Household Income from Census ACS"""
    print('Fetching median household income from Census ACS...')
    
    # ACS 5-Year Estimates (2022) - Table B19013 (Median Household Income)
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B19013_001E&for=state:*&key={CENSUS_API_KEY}'
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # First row is headers: ["NAME", "B19013_001E", "state"]
    headers = data[0]
    rows = data[1:]
    
    result = {}
    for row in rows:
        state_fips = row[2]
        income = int(row[1]) if row[1] and row[1] != '-666666666' else None
        
        result[state_fips] = {
            'name': row[0],
            'medianIncome': income
        }
    
    print(f'✓ Fetched median income for {len(result)} states')
    return result


def fetch_employment_rate():
    """Fetch Employment Rate from Census ACS"""
    print('Fetching employment rate from Census ACS...')
    
    # ACS 5-Year Estimates (2022) - Table S2301
    # S2301_C03_001E = Employment Rate (% employed of population 16+)
    url = f'https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S2301_C03_001E&for=state:*&key={CENSUS_API_KEY}'
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    headers = data[0]
    rows = data[1:]
    
    result = {}
    for row in rows:
        state_fips = row[2]
        employment_rate = float(row[1]) if row[1] and row[1] != 'null' else None
        
        result[state_fips] = {
            'employmentRate': employment_rate
        }
    
    print(f'✓ Fetched employment rate for {len(result)} states')
    return result


def merge_data(income_data, employment_data):
    """Merge all data sources"""
    print('Merging all data sources...')
    
    merged = {}
    
    for fips, name in STATE_FIPS.items():
        merged[fips] = {
            'fips': fips,
            'name': name,
            'medianIncome': income_data.get(fips, {}).get('medianIncome'),
            'employmentRate': employment_data.get(fips, {}).get('employmentRate')
        }
    
    return merged


def save_to_file(data, filename):
    """Save to JSON file"""
    # Get the script directory and construct path to data folder
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    
    # Create data directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    
    filepath = data_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f'✓ Data saved to: {filepath}')


def main():
    """Main execution"""
    print('=== Starting State Economic Data Fetch ===\n')
    
    try:
        # Fetch all data
        income_data = fetch_median_income()
        employment_data = fetch_employment_rate()
        
        # Merge data
        merged_data = merge_data(income_data, employment_data)
        
        # Save to file
        save_to_file(merged_data, 'states_economic_data.json')
        
        # Print sample data
        print('\n=== Sample Data (California) ===')
        print(json.dumps(merged_data['06'], indent=2))
        
        print('\n=== Fetch Complete ===')
        print(f'Total states processed: {len(merged_data)}')
        
    except Exception as error:
        print(f'ERROR: {error}')
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()