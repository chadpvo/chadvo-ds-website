"""
Fetch County-Level Economic Data (Robust Version)
Run with: python fetch_county_data.py
"""

import requests
import json
from pathlib import Path
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# API Keys
CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'
BEA_API_KEY = '13B14004-10BE-45AF-BC61-3B7A3F127435'

# State FIPS to abbreviation mapping
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

def create_session_with_retries():
    session = requests.Session()
    retry_strategy = Retry(
        total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_with_year_fallback(fetch_func, start_year, max_retries=3):
    current_year = start_year
    for i in range(max_retries + 1):
        data = fetch_func(current_year)
        if data:
            if current_year != start_year:
                print(f'   ↳ SUCCESS: Fallback to {current_year} worked!')
            return data, current_year
        if i < max_retries:
            print(f'   ⚠ No data for {current_year}, trying {current_year - 1}...')
            current_year -= 1
    print(f'   ❌ Failed to fetch data after checking back to {current_year}')
    return {}, start_year

def detect_latest_census_year():
    print('Detecting latest available Census ACS dataset...')
    session = create_session_with_retries()
    for year in range(2025, 2015, -1):
        try:
            if session.get(f'https://api.census.gov/data/{year}/acs/acs5', timeout=10).status_code == 200:
                print(f'✓ Latest available ACS dataset: {year}')
                return year
        except: continue
    return 2022

def detect_latest_bea_year():
    print('Detecting latest available BEA dataset...')
    if BEA_API_KEY == 'YOUR_BEA_API_KEY': return 2022
    session = create_session_with_retries()
    try:
        url = (f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetParameterValues'
               f'&datasetname=Regional&ParameterName=Year&TableName=CAGDP2&ResultFormat=JSON')
        data = session.get(url, timeout=30).json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'ParamValue' in data['BEAAPI']['Results']:
            years = [int(i['Key']) for i in data['BEAAPI']['Results']['ParamValue'] if i['Key'].isdigit()]
            if years: return max(years)
    except: pass
    return 2022

def fetch_median_income(year):
    print(f'Fetching median household income (Census ACS {year})...')
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        result = {}
        for row in response.json()[1:]:
            state, county = row[2], row[3]
            full_fips = state + county
            name_parts = row[0].split(',')
            county_name = name_parts[0].strip()
            state_abbr = STATE_FIPS_TO_ABBR.get(state, '')
            
            val = int(row[1]) if row[1] and row[1] not in ['-666666666', 'null'] else None
            result[full_fips] = {
                'name': f"{county_name}, {state_abbr}" if state_abbr else county_name,
                'stateFips': state, 'countyFips': county, 'medianIncome': val
            }
        print(f'✓ Fetched median income for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_employment_rate(year):
    print(f'Fetching employment rate (Census ACS {year})...')
    session = create_session_with_retries()
    # Note: Uses /subject endpoint
    url = f'https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S2301_C03_001E&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        result = {}
        for row in response.json()[1:]:
            full_fips = row[2] + row[3]
            val = float(row[1]) if row[1] and row[1] != 'null' else None
            result[full_fips] = {'employmentRate': val}
        print(f'✓ Fetched employment rate for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_metrics(year):
    print(f'Fetching housing metrics (Census ACS {year})...')
    session = create_session_with_retries()
    url = (f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B25077_001E,B25064_001E,'
           f'B25003_002E,B25003_001E,B25002_001E,B25002_003E&for=county:*&in=state:*&key={CENSUS_API_KEY}')
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        result = {}
        for row in response.json()[1:]:
            full_fips = row[7] + row[8]
            med_home = int(row[1]) if row[1] not in ['-666666666', 'null'] else None
            med_rent = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            owner, total_occ = (int(row[3]) if row[3] != 'null' else 0), (int(row[4]) if row[4] != 'null' else 0)
            total_units, vacant = (int(row[5]) if row[5] != 'null' else 0), (int(row[6]) if row[6] != 'null' else 0)
            
            home_rate = round((owner / total_occ) * 100, 1) if owner and total_occ else None
            vac_rate = round((vacant / total_units) * 100, 1) if vacant and total_units else None
            
            result[full_fips] = {
                'medianHomeValue': med_home, 'medianRent': med_rent,
                'homeownershipRate': home_rate, 'vacancyRate': vac_rate
            }
        print(f'✓ Fetched housing metrics for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_bea_gdp_county(year):
    print(f'Fetching GDP data from BEA (CAGDP2) {year}...')
    if BEA_API_KEY == 'YOUR_BEA_API_KEY': return {}
    session = create_session_with_retries()
    
    try:
        # CAGDP2 = County GDP Table
        url = (f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional'
               f'&TableName=CAGDP2&LineCode=1&Year={year}&GeoFips=COUNTY&ResultFormat=JSON')
        
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        result = {}
        
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
            for item in data['BEAAPI']['Results']['Data']:
                geo_fips = item.get('GeoFips', '')
                val = item.get('DataValue')
                if geo_fips and len(geo_fips) == 5 and val:
                    try:
                        result[geo_fips] = {'gdpTotal': int(float(val.replace(',', '')))}
                    except: pass
        else:
             print("  ⚠ BEA API returned no 'Data' field (likely year not ready)")
             return {}

        print(f'✓ Fetched GDP for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def merge_data(income, employment, housing, gdp, years_meta):
    print('Merging all data sources...')
    merged = {}
    for fips, info in income.items():
        merged[fips] = {
            'fips': fips, 'name': info['name'],
            'stateFips': info['stateFips'], 'countyFips': info['countyFips'],
            'medianIncome': info.get('medianIncome'),
            'employmentRate': employment.get(fips, {}).get('employmentRate'),
            'medianHomeValue': housing.get(fips, {}).get('medianHomeValue'),
            'medianRent': housing.get(fips, {}).get('medianRent'),
            'homeownershipRate': housing.get(fips, {}).get('homeownershipRate'),
            'vacancyRate': housing.get(fips, {}).get('vacancyRate'),
            'gdpTotal': gdp.get(fips, {}).get('gdpTotal'),
            'years': years_meta
        }
    return merged

def save_to_file(data, filename, base_year, years_meta):
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename
    
    output = {
        'metadata': {
            'baseYear': base_year, 'actualYearsUsed': years_meta,
            'source': 'Census ACS & BEA Regional', 'fetchDate': datetime.now().isoformat(),
            'recordCount': len(data)
        },
        'data': data
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f'✓ Data saved to: {filepath}')

def main():
    print('=== Starting County Economic Data Fetch (Robust) ===\n')
    try:
        census_base = detect_latest_census_year()
        bea_base = detect_latest_bea_year()
        print(f'\n📅 Target Base Years: Census {census_base}, BEA {bea_base}\n')

        income_data, inc_year = fetch_with_year_fallback(fetch_median_income, census_base)
        time.sleep(1)
        employment_data, emp_year = fetch_with_year_fallback(fetch_employment_rate, census_base)
        time.sleep(1)
        housing_data, house_year = fetch_with_year_fallback(fetch_housing_metrics, census_base)
        time.sleep(1)
        gdp_data, gdp_year = fetch_with_year_fallback(fetch_bea_gdp_county, bea_base)

        years_meta = {'incomeYear': inc_year, 'employmentYear': emp_year, 'housingYear': house_year, 'gdpYear': gdp_year}
        merged_data = merge_data(income_data, employment_data, housing_data, gdp_data, years_meta)
        save_to_file(merged_data, 'counties_economic_data.json', census_base, years_meta)
        
        print('\n=== Sample Data (LA County) ===')
        if '06037' in merged_data: print(json.dumps(merged_data['06037'], indent=2))
        
        print(f'\n=== Fetch Complete ===\nYears used: {years_meta}')
        
    except Exception as error:
        print(f'ERROR: {error}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()