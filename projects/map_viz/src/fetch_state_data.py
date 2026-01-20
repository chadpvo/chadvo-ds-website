"""
Fetch State-Level Economic Data - PHASE 1 (with retry logic)
Run with: python fetch_state_data.py

Fetches:
- Median Household Income (Census ACS)
- Employment Rate (Census ACS)
- Median Home Value (Census ACS)
- Median Gross Rent (Census ACS)
- Homeownership Rate (Census ACS)
- Vacancy Rate (Census ACS)
- GDP per Capita (BEA)

Saves to: projects/map_viz/data/states_economic_data.json
"""

import requests
import json
from pathlib import Path
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# API Keys
CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'
BEA_API_KEY = '13B14004-10BE-45AF-BC61-3B7A3F127435'

# State FIPS codes
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


def create_session_with_retries():
    """Create a requests session with automatic retry logic"""
    session = requests.Session()
    
    # Retry strategy: retry up to 5 times with exponential backoff
    retry_strategy = Retry(
        total=5,  # Total number of retries
        backoff_factor=2,  # Wait 2, 4, 8, 16, 32 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
        allowed_methods=["GET"]  # Only retry GET requests
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def fetch_median_income():
    """Fetch Median Household Income from Census ACS"""
    print('Fetching median household income from Census ACS...')
    
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B19013_001E&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
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
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching median income: {e}')
        print('  Returning empty result - will continue with other metrics')
        return {}


def fetch_employment_rate():
    """Fetch Employment Rate from Census ACS"""
    print('Fetching employment rate from Census ACS...')
    
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S2301_C03_001E&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=30)
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
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching employment rate: {e}')
        print('  Returning empty result - will continue with other metrics')
        return {}


def fetch_housing_metrics():
    """Fetch Housing Metrics from Census ACS"""
    print('Fetching housing metrics from Census ACS...')
    
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B25077_001E,B25064_001E,B25003_002E,B25003_001E,B25002_001E,B25002_003E&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        headers = data[0]
        rows = data[1:]
        
        result = {}
        for row in rows:
            state_fips = row[7]
            
            median_home_value = int(row[1]) if row[1] and row[1] not in ['-666666666', 'null'] else None
            median_rent = int(row[2]) if row[2] and row[2] not in ['-666666666', 'null'] else None
            
            owner_occupied = int(row[3]) if row[3] and row[3] != 'null' else None
            total_occupied = int(row[4]) if row[4] and row[4] != 'null' else None
            
            total_units = int(row[5]) if row[5] and row[5] != 'null' else None
            vacant_units = int(row[6]) if row[6] and row[6] != 'null' else None
            
            # Calculate homeownership rate
            homeownership_rate = None
            if owner_occupied and total_occupied and total_occupied > 0:
                homeownership_rate = round((owner_occupied / total_occupied) * 100, 1)
            
            # Calculate vacancy rate
            vacancy_rate = None
            if vacant_units and total_units and total_units > 0:
                vacancy_rate = round((vacant_units / total_units) * 100, 1)
            
            result[state_fips] = {
                'medianHomeValue': median_home_value,
                'medianRent': median_rent,
                'homeownershipRate': homeownership_rate,
                'vacancyRate': vacancy_rate
            }
        
        print(f'✓ Fetched housing metrics for {len(result)} states')
        return result
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching housing metrics: {e}')
        print('  Returning empty result - will continue with other metrics')
        return {}


def fetch_bea_gdp():
    """Fetch GDP per Capita from BEA"""
    print('Fetching GDP data from BEA...')
    
    if BEA_API_KEY == 'YOUR_BEA_API_KEY':
        print('⚠ BEA API key not set - skipping GDP data')
        print('  Get free key at: https://apps.bea.gov/api/signup/')
        return {}
    
    session = create_session_with_retries()
    
    try:
        # Using CAGDP2 (GDP by state) instead of SQGDP2 for better coverage
        # TableName=CAGDP2, LineCode=1 (GDP in current dollars, all industry total)
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP2&LineCode=1&Year=2022&GeoFips=STATE&ResultFormat=JSON'
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        result = {}
        
        # Debug: Print what BEA returns
        print('  DEBUG: Checking BEA response structure...')
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            if 'Data' in data['BEAAPI']['Results']:
                bea_data = data['BEAAPI']['Results']['Data']
                print(f'  DEBUG: BEA returned {len(bea_data)} records')
                
                # Print first few records to see structure
                if len(bea_data) > 0:
                    print(f'  DEBUG: Sample record: {bea_data[0]}')
                
                for item in bea_data:
                    geo_fips = item.get('GeoFips', '')
                    geo_name = item.get('GeoName', '')
                    data_value = item.get('DataValue')
                    
                    # BEA uses 2-digit FIPS codes with leading zeros
                    if geo_fips and len(geo_fips) >= 2:
                        state_fips = geo_fips.zfill(2)  # Pad with leading zero if needed
                        
                        # Only process state-level data (not US total)
                        if state_fips in STATE_FIPS and data_value:
                            try:
                                # GDP is in millions, we want total GDP
                                gdp_millions = float(data_value.replace(',', ''))
                                
                                # We'll calculate per capita in merge_data using population
                                # For now, just store the total GDP
                                result[state_fips] = {
                                    'gdpTotal': int(gdp_millions)
                                }
                            except Exception as e:
                                print(f'  DEBUG: Error parsing GDP for {geo_name}: {e}')
            else:
                print('  DEBUG: No "Data" field in BEA response')
                if 'Error' in data['BEAAPI']['Results']:
                    print(f'  BEA Error: {data["BEAAPI"]["Results"]["Error"]}')
        
        print(f'✓ Fetched GDP for {len(result)} states')
        
        # Print states without GDP data
        missing = [name for fips, name in STATE_FIPS.items() if fips not in result]
        if missing:
            print(f'  ⚠ Missing GDP data for: {", ".join(missing[:5])}{"..." if len(missing) > 5 else ""}')
        
        return result
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching BEA data: {e}')
        return {}


def merge_data(income_data, employment_data, housing_data, gdp_data):
    """Merge all data sources"""
    print('Merging all data sources...')
    
    merged = {}
    
    for fips, name in STATE_FIPS.items():
        merged[fips] = {
            'fips': fips,
            'name': name,
            'medianIncome': income_data.get(fips, {}).get('medianIncome'),
            'employmentRate': employment_data.get(fips, {}).get('employmentRate'),
            'medianHomeValue': housing_data.get(fips, {}).get('medianHomeValue'),
            'medianRent': housing_data.get(fips, {}).get('medianRent'),
            'homeownershipRate': housing_data.get(fips, {}).get('homeownershipRate'),
            'vacancyRate': housing_data.get(fips, {}).get('vacancyRate'),
            'gdpTotal': gdp_data.get(fips, {}).get('gdpTotal')  # Changed from gdpPerCapita
        }
    
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
    print('=== Starting State Economic Data Fetch - PHASE 1 ===\n')
    
    try:
        # Fetch all data with delays between requests
        income_data = fetch_median_income()
        print('  Waiting 3 seconds before next request...\n')
        time.sleep(3)
        
        employment_data = fetch_employment_rate()
        print('  Waiting 3 seconds before next request...\n')
        time.sleep(3)
        
        housing_data = fetch_housing_metrics()
        print('  Waiting 3 seconds before next request...\n')
        time.sleep(3)
        
        gdp_data = fetch_bea_gdp()
        
        # Merge data
        merged_data = merge_data(income_data, employment_data, housing_data, gdp_data)
        
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