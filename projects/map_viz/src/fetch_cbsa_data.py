"""
Fetch CBSA-Level Economic Data (Robust Version)
Run with: python fetch_cbsa_data.py

Fetches:
- Median Household Income (Census ACS)
- Employment Rate (Census ACS /subject)
- Housing Metrics (Census ACS)
- GDP (BEA Regional - Table CAGDP2)
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

# Census Geography constant for CBSA
CBSA_GEO = 'metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'

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

def fetch_with_year_fallback(fetch_func, start_year, max_retries=3):
    """Tries to fetch data for a specific year, falling back to previous years on failure."""
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
    print('⚠ Could not detect latest year, defaulting to 2022')
    return 2022

def detect_latest_bea_year():
    print('Detecting latest available BEA dataset...')
    if BEA_API_KEY == 'YOUR_BEA_API_KEY': return 2022
    session = create_session_with_retries()
    try:
        # We use CAGDP2 (County/MSA) to check years, matching your working logic
        url = (f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetParameterValues'
               f'&datasetname=Regional&ParameterName=Year&TableName=CAGDP2&ResultFormat=JSON')
        response = session.get(url, timeout=30)
        data = response.json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'ParamValue' in data['BEAAPI']['Results']:
            years = [int(i['Key']) for i in data['BEAAPI']['Results']['ParamValue'] if i['Key'].isdigit()]
            if years:
                latest = max(years)
                print(f'✓ Latest available BEA dataset: {latest}')
                return latest
    except: pass
    return 2022

def fetch_median_income(year):
    print(f'Fetching median household income (Census ACS {year})...')
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E&for={CBSA_GEO}&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        rows = data[1:]
        result = {}
        for row in rows:
            cbsa_code = row[2]
            name = row[0]
            
            # --- CUSTOM NAME PARSING ---
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
                'name': name, 'cbsaCode': cbsa_code, 'type': cbsa_type, 'medianIncome': income
            }
        print(f'✓ Fetched median income for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_employment_rate(year):
    print(f'Fetching employment rate (Census ACS {year})...')
    session = create_session_with_retries()
    # Uses /subject endpoint
    url = f'https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S2301_C03_001E&for={CBSA_GEO}&key={CENSUS_API_KEY}'
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        result = {}
        for row in data[1:]:
            cbsa_code = row[2]
            val = float(row[1]) if row[1] and row[1] != 'null' else None
            result[cbsa_code] = {'employmentRate': val}
        print(f'✓ Fetched employment rate for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_metrics(year):
    print(f'Fetching housing metrics (Census ACS {year})...')
    session = create_session_with_retries()
    url = (f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B25077_001E,B25064_001E,'
           f'B25003_002E,B25003_001E,B25002_001E,B25002_003E&for={CBSA_GEO}&key={CENSUS_API_KEY}')
    
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        result = {}
        for row in data[1:]:
            cbsa_code = row[7]
            med_home = int(row[1]) if row[1] not in ['-666666666', 'null'] else None
            med_rent = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            owner, total_occ = (int(row[3]) if row[3] != 'null' else 0), (int(row[4]) if row[4] != 'null' else 0)
            total_units, vacant = (int(row[5]) if row[5] != 'null' else 0), (int(row[6]) if row[6] != 'null' else 0)
            
            home_rate = round((owner / total_occ) * 100, 1) if owner and total_occ else None
            vac_rate = round((vacant / total_units) * 100, 1) if vacant and total_units else None
            
            result[cbsa_code] = {
                'medianHomeValue': med_home, 'medianRent': med_rent,
                'homeownershipRate': home_rate, 'vacancyRate': vac_rate
            }
        print(f'✓ Fetched housing metrics for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_bea_gdp_cbsa(year):
    """Fetch CBSA GDP data from BEA Regional dataset"""
    print(f'Fetching GDP data from BEA (CAGDP2) {year}...')
    
    if BEA_API_KEY == 'YOUR_BEA_API_KEY': return {}
    session = create_session_with_retries()
    result = {}
    
    # We fetch 'MSA' (Metropolitan) AND 'MIC' (Micropolitan) using CAGDP2 (Table used in your working script)
    geo_types = ['MSA', 'MIC']
    
    for geo_type in geo_types:
        try:
            # Using CAGDP2 as requested
            url = (f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional'
                   f'&TableName=CAGDP2&LineCode=1&Year={year}&GeoFips={geo_type}&ResultFormat=JSON')
            
            response = session.get(url, timeout=60)
            if response.status_code != 200: continue
            
            data = response.json()
            
            if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
                # Error Logging
                if 'Error' in data['BEAAPI']['Results']:
                    err = data['BEAAPI']['Results']['Error']
                    if year <= 2023: # Only print errors for likely valid years
                         print(f"    ⚠ BEA Error ({geo_type}): {err.get('APIErrorDescription', 'Unknown')}")
                    continue
                
                # Data Parsing
                if 'Data' in data['BEAAPI']['Results']:
                    items = data['BEAAPI']['Results']['Data']
                    count = 0
                    for item in items:
                        cbsa_code = item.get('GeoFips')
                        val = item.get('DataValue')
                        if cbsa_code and val:
                            try:
                                result[cbsa_code] = {'gdpTotal': int(float(val.replace(',', '')))}
                                count += 1
                            except: pass
                    if count > 0:
                        print(f"    ✓ Found {count} records for {geo_type}")

        except Exception as e:
            print(f"    ⚠ Request failed for {geo_type}: {e}")

    if not result:
        print(f"    ⚠ No data found for {year}")
        return {}

    print(f'✓ Fetched GDP for {len(result)} CBSAs')
    return result

def merge_data(income, employment, housing, gdp, years_meta):
    print('Merging all data sources...')
    merged = {}
    for code, info in income.items():
        merged[code] = {
            'cbsaCode': code, 'name': info['name'], 'type': info['type'],
            'medianIncome': info.get('medianIncome'),
            'employmentRate': employment.get(code, {}).get('employmentRate'),
            'medianHomeValue': housing.get(code, {}).get('medianHomeValue'),
            'medianRent': housing.get(code, {}).get('medianRent'),
            'homeownershipRate': housing.get(code, {}).get('homeownershipRate'),
            'vacancyRate': housing.get(code, {}).get('vacancyRate'),
            'gdpTotal': gdp.get(code, {}).get('gdpTotal'),
            'years': years_meta
        }
    print(f'✓ Merged data for {len(merged)} CBSAs')
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
    print('=== Starting CBSA Economic Data Fetch (Robust) ===\n')
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
        gdp_data, gdp_year = fetch_with_year_fallback(fetch_bea_gdp_cbsa, bea_base)

        years_meta = {'incomeYear': inc_year, 'employmentYear': emp_year, 'housingYear': house_year, 'gdpYear': gdp_year}
        merged_data = merge_data(income_data, employment_data, housing_data, gdp_data, years_meta)
        save_to_file(merged_data, 'cbsas_economic_data.json', census_base, years_meta)
        
        print('\n=== Sample Data (NYC Metro) ===')
        if '35620' in merged_data: print(json.dumps(merged_data['35620'], indent=2))
        elif len(merged_data) > 0: print(json.dumps(merged_data[list(merged_data.keys())[0]], indent=2))
            
        print(f'\n=== Fetch Complete ===\nYears used: {years_meta}')
        
    except Exception as error:
        print(f'ERROR: {error}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()