"""
Fetch CBSA-Level Economic Data - PHASE 1 (with retry logic)
Run with: python fetch_cbsa_data.py

Fetches:
- Median Household Income (Census ACS)
- Employment Rate (Census ACS)
- Median Home Value (Census ACS)
- Median Gross Rent (Census ACS)
- Homeownership Rate (Census ACS)
- Vacancy Rate (Census ACS)

Saves to: projects/map_viz/data/cbsas_economic_data.json
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

def create_session_with_retries():
    """Create a requests session with automatic retry logic"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def fetch_median_income():
    """Fetch Median Household Income for all CBSAs"""
    print('Fetching median household income for CBSAs...')
    
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B19013_001E&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        headers = data[0]
        rows = data[1:]
        
        result = {}
        for row in rows:
            cbsa_code = row[2]
            name = row[0]
            
            # Clean up name
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
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching median income: {e}')
        return {}


def fetch_employment_rate():
    """Fetch Employment Rate for all CBSAs"""
    print('Fetching employment rate for CBSAs...')
    
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/2022/acs/acs5/subject?get=NAME,S2301_C03_001E&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
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
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching employment rate: {e}')
        return {}


def fetch_housing_metrics():
    """Fetch Housing Metrics for all CBSAs"""
    print('Fetching housing metrics for CBSAs...')
    
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/2022/acs/acs5?get=NAME,B25077_001E,B25064_001E,B25003_002E,B25003_001E,B25002_001E,B25002_003E&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        headers = data[0]
        rows = data[1:]
        
        result = {}
        for row in rows:
            cbsa_code = row[7]
            
            median_home_value = int(row[1]) if row[1] and row[1] not in ['-666666666', 'null'] else None
            median_rent = int(row[2]) if row[2] and row[2] not in ['-666666666', 'null'] else None
            
            owner_occupied = int(row[3]) if row[3] and row[3] != 'null' else None
            total_occupied = int(row[4]) if row[4] and row[4] != 'null' else None
            
            total_units = int(row[5]) if row[5] and row[5] != 'null' else None
            vacant_units = int(row[6]) if row[6] and row[6] != 'null' else None
            
            # Calculate rates
            homeownership_rate = None
            if owner_occupied and total_occupied and total_occupied > 0:
                homeownership_rate = round((owner_occupied / total_occupied) * 100, 1)
            
            vacancy_rate = None
            if vacant_units and total_units and total_units > 0:
                vacancy_rate = round((vacant_units / total_units) * 100, 1)
            
            result[cbsa_code] = {
                'medianHomeValue': median_home_value,
                'medianRent': median_rent,
                'homeownershipRate': homeownership_rate,
                'vacancyRate': vacancy_rate
            }
        
        print(f'✓ Fetched housing metrics for {len(result)} CBSAs')
        return result
    
    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching housing metrics: {e}')
        return {}
    
def fetch_bea_gdp_cbsa():
    """Fetch CBSA GDP data from BEA Regional dataset"""
    print('Fetching GDP data for CBSAs from BEA...')

    session = create_session_with_retries()

    try:
        url = (
            f'https://apps.bea.gov/api/data/'
            f'?UserID={BEA_API_KEY}'
            f'&method=GetData'
            f'&datasetname=Regional'
            f'&TableName=CAGDP2'
            f'&LineCode=1'
            f'&Year=2022'
            f'&GeoFips=MSA'
            f'&ResultFormat=JSON'
        )

        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        result = {}

        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            for item in data['BEAAPI']['Results'].get('Data', []):
                cbsa_code = item.get('GeoFips')
                value = item.get('DataValue')

                if cbsa_code and value:
                    try:
                        result[cbsa_code] = {
                            'gdpTotal': int(float(value.replace(',', '')))
                        }
                    except Exception:
                        pass

        print(f'✓ Fetched GDP for {len(result)} CBSAs')
        return result

    except requests.exceptions.RequestException as e:
        print(f'⚠ Error fetching CBSA GDP: {e}')
        return {}


def merge_data(income_data, employment_data, housing_data, gdp_data):
    """Merge all data sources"""
    print('Merging all data sources...')
    
    merged = {}
    
    for cbsa_code, cbsa_info in income_data.items():
        merged[cbsa_code] = {
            'cbsaCode': cbsa_code,
            'name': cbsa_info['name'],
            'type': cbsa_info['type'],
            'medianIncome': cbsa_info.get('medianIncome'),
            'employmentRate': employment_data.get(cbsa_code, {}).get('employmentRate'),
            'medianHomeValue': housing_data.get(cbsa_code, {}).get('medianHomeValue'),
            'medianRent': housing_data.get(cbsa_code, {}).get('medianRent'),
            'homeownershipRate': housing_data.get(cbsa_code, {}).get('homeownershipRate'),
            'vacancyRate': housing_data.get(cbsa_code, {}).get('vacancyRate'),
            'gdpTotal': gdp_data.get(cbsa_code, {}).get('gdpTotal')
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
    print('=== Starting CBSA Economic Data Fetch - PHASE 1 ===\n')
    
    try:
        income_data = fetch_median_income()
        print('  Waiting 5 seconds before next request...\n')
        time.sleep(5)
        
        employment_data = fetch_employment_rate()
        print('  Waiting 5 seconds before next request...\n')
        time.sleep(5)
        
        housing_data = fetch_housing_metrics()
        
        gdp_data = fetch_bea_gdp_cbsa()
        merged_data = merge_data(income_data, employment_data, housing_data, gdp_data)
        
        save_to_file(merged_data, 'cbsas_economic_data.json')
        
        print('\n=== Sample Data ===')
        # Try to find a well-known CBSA
        sample_cbsas = ['35620', '31080', '16980']  # NYC, LA, Chicago
        for code in sample_cbsas:
            if code in merged_data:
                print(json.dumps(merged_data[code], indent=2))
                break
        else:
            # If none found, show first
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