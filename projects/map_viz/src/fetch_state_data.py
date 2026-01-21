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

# --- NEW HELPER FUNCTION ---
def fetch_with_year_fallback(fetch_func, start_year, max_retries=3):
    """
    Tries to fetch data for a specific year. If it fails (empty result),
    it tries the previous year, up to max_retries.
    """
    current_year = start_year
    for i in range(max_retries + 1):
        data = fetch_func(current_year)
        
        # If we got data (dictionary is not empty), return it
        if data:
            if current_year != start_year:
                print(f'   ↳ SUCCESS: Fallback to {current_year} worked!')
            return data, current_year
            
        # If data is empty, decrement year and try again
        if i < max_retries:
            print(f'   ⚠ No data for {current_year}, trying {current_year - 1}...')
            current_year -= 1
        
    print(f'   ❌ Failed to fetch data after checking back to {current_year}')
    return {}, start_year

def detect_latest_census_year():
    print('Detecting latest available Census ACS dataset...')
    session = create_session_with_retries()
    current_year = 2025
    for year in range(current_year, 2010, -1):
        test_url = f'https://api.census.gov/data/{year}/acs/acs5'
        try:
            response = session.get(test_url, timeout=10)
            if response.status_code == 200:
                print(f'✓ Latest available ACS dataset: {year}')
                return year
        except:
            continue
    print('⚠ Could not detect latest year, defaulting to 2023')
    return 2023

def detect_latest_bea_year():
    print('Detecting latest available BEA dataset...')
    if BEA_API_KEY == 'YOUR_BEA_API_KEY': return 2023
    
    session = create_session_with_retries()
    try:
        url = (f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetParameterValues'
               f'&datasetname=Regional&ParameterName=Year&TableName=SAGDP2&ResultFormat=JSON')
        response = session.get(url, timeout=30)
        data = response.json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'ParamValue' in data['BEAAPI']['Results']:
            years = [int(item['Key']) for item in data['BEAAPI']['Results']['ParamValue'] if item['Key'].isdigit()]
            if years:
                latest = max(years)
                print(f'✓ Latest available BEA dataset: {latest}')
                return latest
    except Exception as e:
        print(f'⚠ Error detecting BEA year: {e}')
    return 2023

def fetch_median_income(year):
    print(f'Fetching median household income from Census ACS {year}...')
    session = create_session_with_retries() # <--- FIXED MISSING SESSION
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        result = {}
        for row in data[1:]:
            state_fips = row[2]
            income = int(row[1]) if row[1] and row[1] != '-666666666' else None
            result[state_fips] = {'name': row[0], 'medianIncome': income}
        print(f'✓ Fetched median income for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error fetching median income: {e}')
        return {}

def fetch_employment_rate(year):
    print(f'Fetching employment rate from Census ACS {year}...')
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S2301_C03_001E&for=state:*&key={CENSUS_API_KEY}'    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        result = {}
        for row in data[1:]:
            state_fips = row[2]
            emp_rate = float(row[1]) if row[1] and row[1] != 'null' else None
            result[state_fips] = {'employmentRate': emp_rate}
        print(f'✓ Fetched employment rate for {len(result)} states')
        return result
    except Exception as e:
        # Note: We print error but return empty dict so fallback can trigger
        print(f'⚠ Error fetching employment rate: {e}') 
        return {}

def fetch_housing_metrics(year):
    print(f'Fetching housing metrics from Census ACS {year}...')
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B25077_001E,B25064_001E,B25003_002E,B25003_001E,B25002_001E,B25002_003E&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        result = {}
        for row in data[1:]:
            state_fips = row[7]
            med_home = int(row[1]) if row[1] and row[1] not in ['-666666666', 'null'] else None
            med_rent = int(row[2]) if row[2] and row[2] not in ['-666666666', 'null'] else None
            owner_occ = int(row[3]) if row[3] and row[3] != 'null' else None
            total_occ = int(row[4]) if row[4] and row[4] != 'null' else None
            total_units = int(row[5]) if row[5] and row[5] != 'null' else None
            vacant = int(row[6]) if row[6] and row[6] != 'null' else None
            
            home_rate = round((owner_occ / total_occ) * 100, 1) if owner_occ and total_occ else None
            vac_rate = round((vacant / total_units) * 100, 1) if vacant and total_units else None
            
            result[state_fips] = {
                'medianHomeValue': med_home, 'medianRent': med_rent,
                'homeownershipRate': home_rate, 'vacancyRate': vac_rate
            }
        print(f'✓ Fetched housing metrics for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error fetching housing metrics: {e}')
        return {}

def fetch_bea_gdp(year):
    print(f'Fetching GDP data from BEA Regional dataset {year}...')
    if BEA_API_KEY == 'YOUR_BEA_API_KEY': return {}
    session = create_session_with_retries()
    
    try:
        url = (f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional'
               f'&TableName=SAGDP2&LineCode=1&Year={year}&GeoFips=STATE&ResultFormat=JSON')
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        result = {}
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
            bea_data = data['BEAAPI']['Results']['Data']
            print(f'  ✓ BEA returned {len(bea_data)} records')
            for item in bea_data:
                geo_fips = item.get('GeoFips', '')
                val = item.get('DataValue')
                if geo_fips and len(geo_fips) >= 2:
                    state_fips = geo_fips[:2]
                    if state_fips in STATE_FIPS and state_fips != '00' and val:
                        try:
                            result[state_fips] = {'gdpTotal': int(float(val.replace(',', '')))}
                        except: pass
        else:
            # Capture specific BEA error logic here so we trigger fallback
            print("  ⚠ BEA API returned no 'Data' field (likely year not ready)")
            return {} # Return empty to trigger fallback

        print(f'✓ Fetched GDP for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error fetching BEA data: {e}')
        return {}

def merge_data(income, employment, housing, gdp, years_meta):
    print('Merging all data sources...')
    merged = {}
    for fips, name in STATE_FIPS.items():
        merged[fips] = {
            'fips': fips,
            'name': name,
            'medianIncome': income.get(fips, {}).get('medianIncome'),
            'employmentRate': employment.get(fips, {}).get('employmentRate'),
            'medianHomeValue': housing.get(fips, {}).get('medianHomeValue'),
            'medianRent': housing.get(fips, {}).get('medianRent'),
            'homeownershipRate': housing.get(fips, {}).get('homeownershipRate'),
            'vacancyRate': housing.get(fips, {}).get('vacancyRate'),
            'gdpTotal': gdp.get(fips, {}).get('gdpTotal'),
            # Adding metadata per state so you know which year was used
            'years': years_meta
        }
    return merged

def save_to_file(data, filename, base_year, years_meta):
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename
    
    from datetime import datetime
    output = {
        'metadata': {
            'baseYear': base_year,
            'actualYearsUsed': years_meta,
            'source': 'US Census Bureau ACS & BEA',
            'fetchDate': datetime.now().isoformat(),
            'recordCount': len(data)
        },
        'data': data
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f'✓ Data saved to: {filepath}')

def main():
    print('=== Starting State Economic Data Fetch (With Fallback) ===\n')
    try:
        census_base = detect_latest_census_year()
        bea_base = detect_latest_bea_year()
        print(f'\n📅 Target Base Years: Census {census_base}, BEA {bea_base}\n')
        
        # Fetch with fallback logic
        income_data, inc_year = fetch_with_year_fallback(fetch_median_income, census_base)
        time.sleep(1)
        
        employment_data, emp_year = fetch_with_year_fallback(fetch_employment_rate, census_base)
        time.sleep(1)
        
        housing_data, house_year = fetch_with_year_fallback(fetch_housing_metrics, census_base)
        time.sleep(1)
        
        gdp_data, gdp_year = fetch_with_year_fallback(fetch_bea_gdp, bea_base)
        
        # Compile year metadata
        years_meta = {
            'incomeYear': inc_year,
            'employmentYear': emp_year,
            'housingYear': house_year,
            'gdpYear': gdp_year
        }
        
        merged_data = merge_data(income_data, employment_data, housing_data, gdp_data, years_meta)
        save_to_file(merged_data, 'states_economic_data.json', census_base, years_meta)
        
        print('\n=== Sample Data (California) ===')
        if '06' in merged_data:
            print(json.dumps(merged_data['06'], indent=2))
        
        print(f'\n=== Fetch Complete ===')
        print(f"Years used: {years_meta}")
        
    except Exception as error:
        print(f'ERROR: {error}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()