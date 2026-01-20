"""
Fetch County-Level Economic Data
Run with: python fetch_county_data.py

Fetches:
- Median Household Income (Census ACS)
- Employment Rate (Census ACS)

Saves to: projects/map_viz/data/counties_economic_data.json
"""

import requests
import json
from pathlib import Path
import time

# API Keys
CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'

# State FIPS to abbreviation (for county names)
STATE_FIPS_TO_ABBR = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO',
    '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI',
    '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY',
    '22': 'LA', '23': 'ME', '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN',
    '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH',
    '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD',
    '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA',
    '54': 'WV', '55': 'WI', '56': 'WY'
}


def fetch_median_income():
    """Fetch Median Household Income for all counties from Census ACS"""
    print('Fetching median household income for counties from Census ACS...')
    
    # ACS 5-Year Estimates (2022) - Table B19013 (Median Household Income)
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B19013_001E&for=county:*&key={CENSUS_API_KEY}'
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # First row is headers: ["NAME", "B19013_001E", "state", "county"]
    headers = data[0]
    rows = data[1:]
    
    result = {}
    for row in rows:
        state_fips = row[2]
        county_fips = row[3]
        full_fips = state_fips + county_fips  # 5-digit FIPS
        
        # Parse county name (e.g., "Los Angeles County, California")
        name_parts = row[0].split(',')
        county_name = name_parts[0].strip()
        state_abbr = STATE_FIPS_TO_ABBR.get(state_fips, '')
        
        income = int(row[1]) if row[1] and row[1] not in ['-666666666', 'null'] else None
        
        result[full_fips] = {
            'name': f"{county_name}, {state_abbr}" if state_abbr else county_name,
            'stateFips': state_fips,
            'countyFips': county_fips,
            'medianIncome': income
        }
    
    print(f'✓ Fetched median income for {len(result)} counties')
    return result


def fetch_employment_rate():
    """Fetch Employment Rate for all counties from Census ACS"""
    print('Fetching employment rate for counties from Census ACS...')
    
    # ACS 5-Year Estimates (2022) - Table S2301
    # S2301_C03_001E = Employment Rate (% employed of population 16+)
    url = f'https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S2301_C03_001E&for=county:*&key={CENSUS_API_KEY}'
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    headers = data[0]
    rows = data[1:]
    
    result = {}
    for row in rows:
        state_fips = row[2]
        county_fips = row[3]
        full_fips = state_fips + county_fips
        
        employment_rate = float(row[1]) if row[1] and row[1] != 'null' else None
        
        result[full_fips] = {
            'employmentRate': employment_rate
        }
    
    print(f'✓ Fetched employment rate for {len(result)} counties')
    return result


def merge_data(income_data, employment_data):
    """Merge all data sources"""
    print('Merging all data sources...')
    
    merged = {}
    
    # Use income_data as base since it has all counties
    for fips, county_info in income_data.items():
        merged[fips] = {
            'fips': fips,
            'name': county_info['name'],
            'stateFips': county_info['stateFips'],
            'countyFips': county_info['countyFips'],
            'medianIncome': county_info.get('medianIncome'),
            'employmentRate': employment_data.get(fips, {}).get('employmentRate')
        }
    
    print(f'✓ Merged data for {len(merged)} counties')
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
    print('=== Starting County Economic Data Fetch ===\n')
    
    try:
        # Fetch all data
        income_data = fetch_median_income()
        time.sleep(1)  # Be nice to the API
        employment_data = fetch_employment_rate()
        
        # Merge data
        merged_data = merge_data(income_data, employment_data)
        
        # Save to file
        save_to_file(merged_data, 'counties_economic_data.json')
        
        # Print sample data (Los Angeles County)
        print('\n=== Sample Data (Los Angeles County, CA) ===')
        if '06037' in merged_data:
            print(json.dumps(merged_data['06037'], indent=2))
        
        print('\n=== Fetch Complete ===')
        print(f'Total counties processed: {len(merged_data)}')
        
    except Exception as error:
        print(f'ERROR: {error}')
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    main()