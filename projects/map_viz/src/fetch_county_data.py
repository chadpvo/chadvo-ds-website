"""
Complete County-Level Data Fetcher (Robust & Scalable)
Fetches Census ACS, BEA GDP, and HUD (FMR + Income Limits) for all US counties.
Optimized to fetch HUD data by State batches to prevent API timeouts.
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
BEA_API_KEY = '13B14004-10BE-45AF-BC61-3B7A3F127435'
HUD_API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI2IiwianRpIjoiYTgzZjE4NWRlODZhOWYwODYwZDZiOGU5ZDRmMGMxZTZlZmU1OWZkZjlkMmI2YmQzNGQ1YjUyODQwZTg2Y2Y1YjlhZTZhOWU1YmMzYWVmMjUiLCJpYXQiOjE3NjkwMjYwNjkuMjExNTI3LCJuYmYiOjE3NjkwMjYwNjkuMjExNTMsImV4cCI6MjA4NDU1ODg2OS4xOTc3NzksInN1YiI6IjExNzg3MiIsInNjb3BlcyI6W119.I3mmAjvo-3Tgm9Y42YAfc86TZO-nPbmEx_JXvJaMwZqMKVzbsg81TnBM5At-NX_xVeWCRv8ddrezx3C2ox8p7w"

# API Base URLs
HUD_FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"
HUD_IL_BASE = "https://www.huduser.gov/hudapi/public/il"

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
    # Increased retries and backoff for reliability at scale
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
    for i, year in enumerate(range(start_year, start_year - max_retries - 1, -1)):
        data = fetch_func(year)
        if data:
            if year != start_year:
                print(f'   ↳ Fallback to {year} worked!')
            return data, year
        if i < max_retries:
            print(f'   ⚠ No data for {year}, trying {year - 1}...')
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

def detect_latest_bea_year():
    print('Detecting latest BEA...')
    session = create_session_with_retries()
    try:
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetParameterValues&datasetname=Regional&ParameterName=Year&TableName=CAGDP2&ResultFormat=JSON'
        years = [int(i['Key']) for i in session.get(url, timeout=30).json().get('BEAAPI', {}).get('Results', {}).get('ParamValue', []) if i['Key'].isdigit()]
        if years:
            print(f'✓ Latest BEA: {max(years)}')
            return max(years)
    except: pass
    return 2023

def detect_latest_hud_years():
    print('Detecting latest HUD...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    
    fmr_year, il_year = None, None
    for year in range(2026, 2015, -1):
        try:
            if not fmr_year and session.get(f"{HUD_FMR_BASE}/statedata/CA", headers=headers, params={'year': year}, timeout=10).status_code == 200:
                fmr_year = year
            if not il_year and session.get(f"{HUD_IL_BASE}/statedata/CA", headers=headers, params={'year': year}, timeout=10).status_code == 200:
                il_year = year
            if fmr_year and il_year:
                break
        except: continue
    
    print(f'✓ Latest HUD FMR: {fmr_year}, IL: {il_year}')
    return fmr_year or 2024, il_year or 2024

# ==================== CENSUS FETCHERS (MATCHING STATE VARIABLES) ====================

def fetch_household_economics(year):
    print(f'Fetching household economics (ACS {year})...')
    session = create_session_with_retries()
    # Variables: Median Income, Poverty Universe, Poverty Count
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E,B17001_001E,B17001_002E&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        result = {}
        for row in session.get(url, timeout=60).json()[1:]:
            fips = row[4] + row[5]
            county_name = row[0].split(',')[0].strip()
            state_abbr = STATE_FIPS_TO_CODE.get(row[4], '')
            
            total_pop = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            poverty = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            
            result[fips] = {
                'name': f"{county_name}, {state_abbr}",
                'stateFips': row[4],
                'countyFips': row[5],
                'medianHouseholdIncome': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'povertyRate': round((poverty / total_pop) * 100, 1) if total_pop and poverty else None
            }
        print(f'✓ Fetched for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_characteristics(year):
    """Fetches Housing Units, Occupancy, Vacancy, Tenure, and Age of Structure"""
    print(f'Fetching housing characteristics (ACS {year})...')
    session = create_session_with_retries()
    # Added B25035_001E (Year Built) to match State script
    variables = [
        'NAME', 'B25001_001E', 'B25002_001E', 'B25002_002E', 'B25002_003E',
        'B25003_001E', 'B25003_002E', 'B25003_003E', 'B25035_001E'
    ]
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        result = {}
        for row in session.get(url, timeout=60).json()[1:]:
            fips = row[-2] + row[-1] # State + County
            
            total = int(row[1]) if row[1] not in ['-666666666', 'null'] else None
            occupied = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            vacant = int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            owner = int(row[6]) if row[6] not in ['-666666666', 'null'] else None
            renter = int(row[7]) if row[7] not in ['-666666666', 'null'] else None
            year_built = int(row[8]) if row[8] not in ['-666666666', 'null'] else None
            
            result[fips] = {
                'totalHousingUnits': total,
                'occupiedUnits': occupied,
                'vacantUnits': vacant,
                'ownerOccupied': owner,
                'renterOccupied': renter,
                'medianYearBuilt': year_built
            }
        print(f'✓ Fetched characteristics for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_values_costs(year):
    """Fetches Home Values, Rents, and Monthly Owner Costs"""
    print(f'Fetching housing values & costs (ACS {year})...')
    session = create_session_with_retries()
    # Added B25088 (Owner Costs) to match State script
    variables = ['NAME', 'B25077_001E', 'B25064_001E', 'B25088_002E', 'B25088_003E']
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        result = {}
        for row in session.get(url, timeout=60).json()[1:]:
            fips = row[-2] + row[-1]
            
            result[fips] = {
                'medianHomeValue': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianGrossRent': int(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsWithMortgage': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsNoMortgage': int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            }
        print(f'✓ Fetched values/costs for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_demographics(year):
    print(f'Fetching demographics (ACS {year})...')
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B01003_001E,B01002_001E,B23025_003E,B23025_004E&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        result = {}
        for row in session.get(url, timeout=60).json()[1:]:
            fips = row[5] + row[6]
            employed = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            unemployed = int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            labor = (employed + unemployed) if employed and unemployed else None
            
            result[fips] = {
                'totalPopulation': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianAge': float(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'employmentRate': round((employed / labor) * 100, 1) if labor and employed else None,
                'unemploymentRate': round((unemployed / labor) * 100, 1) if labor and unemployed else None
            }
        print(f'✓ Fetched demographics for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_bea_gdp(year):
    print(f'Fetching GDP (BEA {year})...')
    session = create_session_with_retries()
    
    try:
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP2&LineCode=1&Year={year}&GeoFips=COUNTY&ResultFormat=JSON'
        data = session.get(url, timeout=60).json()
        
        result = {}
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
            for item in data['BEAAPI']['Results']['Data']:
                fips = item.get('GeoFips', '')
                val = item.get('DataValue')
                if fips and len(fips) == 5 and val:
                    try:
                        result[fips] = {'gdpTotal': int(float(val.replace(',', '')))}
                    except: pass
        
        print(f'✓ Fetched GDP for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

# ==================== HUD FETCHERS (OPTIMIZED SCALABILITY) ====================

def fetch_hud_county_data_optimized(year_fmr, year_il):
    """
    OPTIMIZED HUD FETCHER
    Fetches data by STATE instead of by COUNTY.
    Reduces API calls from ~3000 to ~50.
    """
    print(f'Fetching HUD county data (FMR={year_fmr}, IL={year_il})...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    
    result = {}
    
    # Iterate through STATES, not Counties
    for state_fips, state_code in STATE_FIPS_TO_CODE.items():
        # 1. Fetch FMR for the whole state (includes county breakdown)
        try:
            url = f"{HUD_FMR_BASE}/statedata/{state_code}"
            resp = session.get(url, headers=headers, params={'year': year_fmr}, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json().get('data', {})
                # 'counties' key usually contains the list of all counties in the state
                counties = data.get('counties', [])
                
                for county in counties:
                    fips = county.get('fips_code', '')[:5] # Ensure 5 digits
                    if not fips: continue
                    
                    if fips not in result: result[fips] = {}
                    
                    result[fips].update({
                        'fmr0Bedroom': int(county.get('Efficiency', 0)) if county.get('Efficiency') else None,
                        'fmr1Bedroom': int(county.get('One-Bedroom', 0)) if county.get('One-Bedroom') else None,
                        'fmr2Bedroom': int(county.get('Two-Bedroom', 0)) if county.get('Two-Bedroom') else None,
                        'fmr3Bedroom': int(county.get('Three-Bedroom', 0)) if county.get('Three-Bedroom') else None,
                        'fmr4Bedroom': int(county.get('Four-Bedroom', 0)) if county.get('Four-Bedroom') else None
                    })
        except Exception as e:
            print(f'  ⚠ FMR Error for {state_code}: {e}')

        # 2. Fetch Income Limits for the whole state
        try:
            url = f"{HUD_IL_BASE}/statedata/{state_code}"
            resp = session.get(url, headers=headers, params={'year': year_il}, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                # The IL endpoint returns a LIST of counties/areas directly
                if isinstance(data, list):
                    for area in data:
                        # Sometimes IL data uses 10-digit FIPS or 5-digit depending on year
                        # We check if it has a 'fips_code' or 'county_code'
                        # Note: HUD IL API structure varies slightly, but usually has fips_code
                        fips = str(area.get('fips_code', ''))[:5] # sometimes it is an int
                        
                        # Fallback: check if we can match by name if fips missing (rare)
                        if not fips or len(fips) < 5: continue
                        
                        if fips not in result: result[fips] = {}
                        
                        result[fips].update({
                            'medianFamilyIncome': area.get('median_income'),
                            'incomeLimitLow80_4person': area.get('low', {}).get('il80_p4')
                        })
        except Exception as e:
            print(f'  ⚠ IL Error for {state_code}: {e}')

        print(f'  ✓ Processed HUD data for {state_code}')
        time.sleep(0.1) # Brief pause to be nice to API

    print(f'✓ Fetched HUD data for {len(result)} counties')
    return result

# ==================== MERGE AND SAVE ====================

def merge_all_data(household_econ, housing_chars, housing_vals, demographics, gdp, hud_data, years_meta):
    print('Merging all data...')
    merged = {}
    
    for fips, info in household_econ.items():
        merged[fips] = {
            'fips': fips,
            'name': info['name'],
            'stateFips': info['stateFips'],
            'countyFips': info['countyFips'],
            'medianHouseholdIncome': info.get('medianHouseholdIncome'),
            'povertyRate': info.get('povertyRate'),
            # Now includes all extensive housing data
            **housing_chars.get(fips, {}),
            **housing_vals.get(fips, {}),
            **demographics.get(fips, {}),
            'gdpTotal': gdp.get(fips, {}).get('gdpTotal'),
            **hud_data.get(fips, {}),
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
            'source': 'Census ACS, BEA, HUD FMR & Income Limits',
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
    print('COMPLETE COUNTY DATA FETCH (Optimized)')
    print('='*70 + '\n')
    
    try:
        census_year = detect_latest_census_year()
        bea_year = detect_latest_bea_year()
        hud_fmr_year, hud_il_year = detect_latest_hud_years()
        
        print(f'\n📅 Using: Census={census_year}, BEA={bea_year}, FMR={hud_fmr_year}, IL={hud_il_year}\n')
        
        household_econ, he_year = fetch_with_year_fallback(fetch_household_economics, census_year)
        time.sleep(1)
        
        # Split housing into two calls to match State variables
        housing_chars, hc_year = fetch_with_year_fallback(fetch_housing_characteristics, census_year)
        time.sleep(1)
        housing_vals, hv_year = fetch_with_year_fallback(fetch_housing_values_costs, census_year)
        time.sleep(1)
        
        demographics, d_year = fetch_with_year_fallback(fetch_demographics, census_year)
        time.sleep(1)
        gdp, g_year = fetch_with_year_fallback(fetch_bea_gdp, bea_year)
        time.sleep(1)
        
        # Optimized HUD Fetcher
        hud_data = fetch_hud_county_data_optimized(hud_fmr_year, hud_il_year)
        
        years_meta = {
            'householdEconomics': he_year,
            'housingCharacteristics': hc_year,
            'housingValues': hv_year,
            'demographics': d_year,
            'gdp': g_year,
            'hudFMR': hud_fmr_year,
            'hudIncomeLimits': hud_il_year
        }
        
        merged = merge_all_data(household_econ, housing_chars, housing_vals, demographics, gdp, hud_data, years_meta)
        save_to_file(merged, 'counties_economic_data.json', years_meta)
        
        print('\n=== Sample (LA County) ===')
        if '06037' in merged:
            print(json.dumps(merged['06037'], indent=2))
        elif len(merged) > 0:
             print(json.dumps(merged[list(merged.keys())[0]], indent=2))
        
        print(f'\n✅ Complete! Years: {years_meta}')
        
    except Exception as e:
        print(f'\n❌ ERROR: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()