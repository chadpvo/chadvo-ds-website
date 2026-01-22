"""
Complete Zip Code (ZCTA) Data Fetcher
Fetches Census ACS data for all US Zip Code Tabulation Areas.
Iterates by State to handle the large volume of Zip Codes (33k+) without timeouts.
"""

import requests
import json
from pathlib import Path
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# ============================================================
# API KEYS
# ============================================================
CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'

# State FIPS Mapping (Used to iterate Zips by State)
STATE_FIPS_TO_CODE = {
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
        total=5, backoff_factor=2, 
        status_forcelist=[429, 500, 502, 503, 504], 
        allowed_methods=["GET"]
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
                print(f'   ↳ Fallback to {current_year} worked!')
            return data, current_year
        if i < max_retries:
            print(f'   ⚠ No data for {current_year}, trying {current_year - 1}...')
            current_year -= 1
    print(f'   ❌ Failed after checking back to {current_year}')
    return {}, start_year

def detect_latest_census_year():
    print('Detecting latest Census ACS...')
    session = create_session_with_retries()
    for year in range(2025, 2015, -1):
        try:
            if session.get(f'https://api.census.gov/data/{year}/acs/acs5', timeout=10).status_code == 200:
                print(f'✓ Latest ACS: {year}')
                return year
        except: continue
    return 2023

# ==================== HELPER: BATCH FETCH BY STATE ====================

def fetch_census_by_state_loop(year, variables, label):
    """
    Fetches ZCTA data by iterating through every state.
    Necessary because fetching all 33k Zips at once often times out or hits limits.
    """
    print(f'Fetching {label} (ACS {year})...')
    session = create_session_with_retries()
    combined_result = {}
    
    # Counter for progress
    count = 0
    total_states = len(STATE_FIPS_TO_CODE)
    
    for fips, abbr in STATE_FIPS_TO_CODE.items():
        # URL for ZCTAs within a specific state
        url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=zip%20code%20tabulation%20area:*&in=state:{fips}&key={CENSUS_API_KEY}'
        
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Skip header row
                for row in data[1:]:
                    # The last column is the ZCTA5 code
                    zip_code = row[-1]
                    
                    # Logic to parse the row is handled by the callback or processed here generically?
                    # To keep it simple, we return the raw row data mapped by Zip
                    combined_result[zip_code] = row
            
            count += 1
            if count % 10 == 0:
                print(f'  ...processed {count}/{total_states} states')
                
        except Exception as e:
            # Some states might not strictly have ZCTAs nested perfectly in older APIs, but 2020+ works well.
            # We continue silently to ensure we get as much as possible.
            continue
            
    print(f'✓ Fetched {label} for {len(combined_result)} Zips')
    return combined_result

# ==================== CENSUS FETCHERS (ZIP LEVEL) ====================

def fetch_household_economics(year):
    # Variables: Median Income, Poverty Universe, Poverty Count
    variables = ['NAME', 'B19013_001E', 'B17001_001E', 'B17001_002E']
    
    raw_data = fetch_census_by_state_loop(year, variables, "household economics")
    
    result = {}
    for zip_code, row in raw_data.items():
        try:
            total_pop = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            poverty = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            
            result[zip_code] = {
                'name': row[0],
                'zipCode': zip_code,
                'medianHouseholdIncome': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'povertyRate': round((poverty / total_pop) * 100, 1) if total_pop and poverty else None
            }
        except: continue
        
    return result

def fetch_housing_characteristics(year):
    # Units, Occupancy, Vacancy, Tenure, Year Built
    variables = [
        'NAME', 'B25001_001E', 'B25002_001E', 'B25002_002E', 'B25002_003E',
        'B25003_001E', 'B25003_002E', 'B25003_003E', 'B25035_001E'
    ]
    
    raw_data = fetch_census_by_state_loop(year, variables, "housing characteristics")
    
    result = {}
    for zip_code, row in raw_data.items():
        try:
            result[zip_code] = {
                'totalHousingUnits': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'occupiedUnits': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'vacantUnits': int(row[4]) if row[4] not in ['-666666666', 'null'] else None,
                'ownerOccupied': int(row[6]) if row[6] not in ['-666666666', 'null'] else None,
                'renterOccupied': int(row[7]) if row[7] not in ['-666666666', 'null'] else None,
                'medianYearBuilt': int(row[8]) if row[8] not in ['-666666666', 'null'] else None
            }
        except: continue
    return result

def fetch_housing_values_costs(year):
    # Value, Rent, Owner Costs
    variables = ['NAME', 'B25077_001E', 'B25064_001E', 'B25088_002E', 'B25088_003E']
    
    raw_data = fetch_census_by_state_loop(year, variables, "housing values")
    
    result = {}
    for zip_code, row in raw_data.items():
        try:
            result[zip_code] = {
                'medianHomeValue': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianGrossRent': int(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsWithMortgage': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsNoMortgage': int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            }
        except: continue
    return result

def fetch_demographics(year):
    # Pop, Age, Employment (B23025)
    variables = [
        'NAME', 'B01003_001E', 'B01002_001E',
        'B23025_003E', 'B23025_004E', 'B23025_005E'
    ]
    
    raw_data = fetch_census_by_state_loop(year, variables, "demographics")
    
    result = {}
    for zip_code, row in raw_data.items():
        try:
            civ_labor_force = int(row[3]) if row[3] not in ['-666666666', 'null', None] else 0
            employed = int(row[4]) if row[4] not in ['-666666666', 'null', None] else 0
            unemployed = int(row[5]) if row[5] not in ['-666666666', 'null', None] else 0
            
            result[zip_code] = {
                'totalPopulation': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianAge': float(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'employmentRate': round((employed / civ_labor_force) * 100, 1) if civ_labor_force > 0 else None,
                'unemploymentRate': round((unemployed / civ_labor_force) * 100, 1) if civ_labor_force > 0 else None
            }
        except: continue
    return result

# ==================== MERGE AND SAVE ====================

def merge_all_data(household_econ, housing_chars, housing_vals, demographics, years_meta):
    print('Merging all data...')
    merged = {}
    
    # Master list of Zips found in any dataset
    all_zips = set(household_econ.keys()) | set(housing_chars.keys()) | set(housing_vals.keys())
    
    for zip_code in all_zips:
        base_info = household_econ.get(zip_code, {})
        
        merged[zip_code] = {
            'zipCode': zip_code,
            'name': base_info.get('name', f'ZCTA5 {zip_code}'),
            'medianHouseholdIncome': base_info.get('medianHouseholdIncome'),
            'povertyRate': base_info.get('povertyRate'),
            **housing_chars.get(zip_code, {}),
            **housing_vals.get(zip_code, {}),
            **demographics.get(zip_code, {}),
            # Note: BEA GDP and HUD FMR not available for ZCTAs in this standard format
            'years': years_meta
        }
    
    return merged

def save_to_file(data, filename, years_meta):
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename
    
    output = {
        'metadata': {
            'source': 'Census ACS (5-Year Estimates)',
            'note': 'BEA GDP and HUD FMR are not available at standard ZCTA level.',
            'fetchDate': datetime.now().isoformat(),
            'recordCount': len(data),
            'yearsUsed': years_meta
        },
        'data': data
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f'✓ Saved to: {filepath}')

def main():
    print('='*70)
    print('COMPLETE ZIP CODE (ZCTA) DATA FETCH (Census ACS)')
    print('='*70 + '\n')
    
    try:
        census_year = detect_latest_census_year()
        print(f'\n📅 Using: Census={census_year}\n')
        
        # Fetch Census data (Batched by State for robustness)
        household_econ, he_year = fetch_with_year_fallback(fetch_household_economics, census_year)
        time.sleep(1)
        
        housing_chars, hc_year = fetch_with_year_fallback(fetch_housing_characteristics, census_year)
        time.sleep(1)
        
        housing_vals, hv_year = fetch_with_year_fallback(fetch_housing_values_costs, census_year)
        time.sleep(1)
        
        demographics, d_year = fetch_with_year_fallback(fetch_demographics, census_year)
        time.sleep(1)
        
        years_meta = {
            'householdEconomics': he_year,
            'housingCharacteristics': hc_year,
            'housingValues': hv_year,
            'demographics': d_year
        }
        
        merged = merge_all_data(household_econ, housing_chars, housing_vals, demographics, years_meta)
        save_to_file(merged, 'zips_economic_data.json', years_meta)
        
        print('\n=== Sample (90210) ===')
        if '90210' in merged:
            print(json.dumps(merged['90210'], indent=2))
        elif len(merged) > 0:
             print(json.dumps(merged[list(merged.keys())[0]], indent=2))
        
        print(f'\n✅ Complete! Years: {years_meta}')
        
    except Exception as e:
        print(f'\n❌ ERROR: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()