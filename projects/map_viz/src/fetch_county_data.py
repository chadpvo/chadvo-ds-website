"""
Complete County-Level Data Fetcher
Fetches Census ACS, BEA GDP, and HUD (FMR + Income Limits) for all US counties.
Includes optimization for HUD API to prevent timeouts.
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

# State Code Mapping (Needed for HUD batch fetching)
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
    print('Detecting latest Census ACS dataset...')
    session = create_session_with_retries()
    for year in range(2025, 2015, -1):
        try:
            if session.get(f'https://api.census.gov/data/{year}/acs/acs5', timeout=10).status_code == 200:
                print(f'✓ Latest ACS: {year}')
                return year
        except: continue
    return 2023

def detect_latest_bea_year():
    print('Detecting latest BEA dataset...')
    session = create_session_with_retries()
    try:
        # Changed TableName to CAGDP2 (County GDP)
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetParameterValues&datasetname=Regional&ParameterName=Year&TableName=CAGDP2&ResultFormat=JSON'
        data = session.get(url, timeout=30).json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            years = [int(i['Key']) for i in data['BEAAPI']['Results']['ParamValue'] if i['Key'].isdigit()]
            if years:
                print(f'✓ Latest BEA: {max(years)}')
                return max(years)
    except: pass
    return 2023

def detect_latest_hud_years():
    print('Detecting latest HUD datasets...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    
    fmr_year, il_year = None, None
    
    # Test FMR
    for year in range(2026, 2015, -1):
        try:
            resp = session.get(f"{HUD_FMR_BASE}/statedata/CA", headers=headers, params={'year': year}, timeout=10)
            if resp.status_code == 200 and resp.json().get('data'):
                fmr_year = year
                break
        except: continue
    
    # Test Income Limits
    for year in range(2025, 2015, -1):
        try:
            resp = session.get(f"{HUD_IL_BASE}/statedata/CA", headers=headers, params={'year': year}, timeout=10)
            if resp.status_code == 200 and resp.json().get('data'):
                il_year = year
                break
        except: continue
    
    print(f'✓ Latest HUD FMR: {fmr_year}, Income Limits: {il_year}')
    return fmr_year or 2024, il_year or 2024

# ==================== CENSUS DATA FETCHERS (COUNTY LEVEL) ====================

def fetch_housing_characteristics(year):
    print(f'Fetching housing characteristics (ACS {year})...')
    session = create_session_with_retries()
    
    variables = [
        'NAME', 'B25001_001E', 'B25002_001E', 'B25002_002E', 'B25002_003E',
        'B25003_001E', 'B25003_002E', 'B25003_003E', 'B25024_002E',
        'B25024_003E', 'B25035_001E'
    ]
    
    # Added &in=state:* to request
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-2] + row[-1] # Combine State+County FIPS
            
            total = int(row[1]) if row[1] not in ['-666666666', 'null'] else None
            occupied = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            vacant = int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            owner = int(row[6]) if row[6] not in ['-666666666', 'null'] else None
            renter = int(row[7]) if row[7] not in ['-666666666', 'null'] else None
            
            result[fips] = {
                'totalHousingUnits': total,
                'occupiedUnits': occupied,
                'vacantUnits': vacant,
                'ownerOccupied': owner,
                'renterOccupied': renter,
                'medianYearBuilt': int(row[10]) if row[10] not in ['-666666666', 'null'] else None
            }
        
        print(f'✓ Fetched housing characteristics for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_values_costs(year):
    print(f'Fetching housing values & costs (ACS {year})...')
    session = create_session_with_retries()
    
    variables = ['NAME', 'B25077_001E', 'B25064_001E', 'B25088_002E', 'B25088_003E']
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-2] + row[-1]
            result[fips] = {
                'medianHomeValue': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianGrossRent': int(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsWithMortgage': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsNoMortgage': int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            }
        
        print(f'✓ Fetched housing values for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_household_economics(year):
    print(f'Fetching household economics (ACS {year})...')
    session = create_session_with_retries()
    
    variables = ['NAME', 'B19013_001E', 'B17001_001E', 'B17001_002E']
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-2] + row[-1]
            total_pop = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            poverty = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            
            result[fips] = {
                'medianHouseholdIncome': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'povertyRate': round((poverty / total_pop) * 100, 1) if total_pop and poverty else None
            }
        
        print(f'✓ Fetched household economics for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_demographics(year):
    print(f'Fetching demographics (ACS {year})...')
    session = create_session_with_retries()
    
    # Base vars: Total Pop, Median Age
    # Employment vars (B23025): 
    #   003E = Civilian Labor Force
    #   004E = Employed
    #   005E = Unemployed
    variables = [
        'NAME', 'B01003_001E', 'B01002_001E',
        'B23025_003E', 'B23025_004E', 'B23025_005E'
    ]
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=county:*&in=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-2] + row[-1]
            
            # Basic Demographics
            total_pop = int(row[1]) if row[1] not in ['-666666666', 'null', None] else None
            median_age = float(row[2]) if row[2] not in ['-666666666', 'null', None] else None
            
            # Employment Stats
            civ_labor_force = int(row[3]) if row[3] not in ['-666666666', 'null', None] else 0
            employed = int(row[4]) if row[4] not in ['-666666666', 'null', None] else 0
            unemployed = int(row[5]) if row[5] not in ['-666666666', 'null', None] else 0
            
            emp_rate = None
            unemp_rate = None
            
            # Calculate Rates
            if civ_labor_force > 0:
                # Unemployment Rate = Unemployed / Civilian Labor Force
                unemp_rate = round((unemployed / civ_labor_force) * 100, 1)
                
                # Employment Rate = Employed / Civilian Labor Force 
                # (Note: Some definitions use Total Pop 16+, but this keeps it consistent with labor force participation)
                emp_rate = round((employed / civ_labor_force) * 100, 1)
            
            result[fips] = {
                'totalPopulation': total_pop,
                'medianAge': median_age,
                'employmentRate': emp_rate,
                'unemploymentRate': unemp_rate
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
        # CAGDP2 = County GDP Table
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP2&LineCode=1&Year={year}&GeoFips=COUNTY&ResultFormat=JSON'
        data = session.get(url, timeout=60).json()
        
        result = {}
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
            for item in data['BEAAPI']['Results']['Data']:
                geo_fips = item.get('GeoFips', '')
                val = item.get('DataValue')
                
                # Filter for valid 5-digit county FIPS (exclude state summaries)
                if geo_fips and len(geo_fips) == 5 and val and geo_fips != '00000':
                    try:
                        result[geo_fips] = {'gdpTotal': int(float(val.replace(',', '')))}
                    except: pass
        else:
            return {}
        
        print(f'✓ Fetched GDP for {len(result)} counties')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

# ==================== HUD DATA FETCHERS (OPTIMIZED BATCH) ====================

def fetch_hud_county_data_optimized(year_fmr, year_il):
    """
    Fetches HUD data by iterating through STATES instead of Counties.
    Drastically reduces API calls from ~3000+ to ~50.
    """
    print(f'Fetching HUD data (FMR {year_fmr}, IL {year_il})...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    result = {}
    
    for fips_state, state_code in STATE_FIPS_TO_CODE.items():
        # 1. Fetch FMR (Fair Market Rent)
        try:
            url = f"{HUD_FMR_BASE}/statedata/{state_code}"
            resp = session.get(url, headers=headers, params={'year': year_fmr}, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json().get('data', {})
                counties = data.get('counties', [])
                
                for county in counties:
                    fips = county.get('fips_code', '')[:5]
                    if not fips: continue
                    
                    if fips not in result: result[fips] = {}
                    
                    result[fips].update({
                        'fmr0Bedroom': int(county.get('Efficiency', 0)) if county.get('Efficiency') else None,
                        'fmr1Bedroom': int(county.get('One-Bedroom', 0)) if county.get('One-Bedroom') else None,
                        'fmr2Bedroom': int(county.get('Two-Bedroom', 0)) if county.get('Two-Bedroom') else None,
                        'fmr3Bedroom': int(county.get('Three-Bedroom', 0)) if county.get('Three-Bedroom') else None,
                        'fmr4Bedroom': int(county.get('Four-Bedroom', 0)) if county.get('Four-Bedroom') else None
                    })
        except: pass

        # 2. Fetch Income Limits
        try:
            url = f"{HUD_IL_BASE}/statedata/{state_code}"
            resp = session.get(url, headers=headers, params={'year': year_il}, timeout=30)
            
            if resp.status_code == 200:
                data_list = resp.json().get('data', [])
                if isinstance(data_list, list):
                    for area in data_list:
                        fips = str(area.get('fips_code', ''))[:5]
                        if not fips or len(fips) < 5: continue
                        
                        if fips not in result: result[fips] = {}
                        
                        result[fips].update({
                            'medianFamilyIncome': int(area['median_income']) if area.get('median_income') else None,
                            'incomeLimitLow80_4person': int(area['low']['il80_p4']) if area.get('low', {}).get('il80_p4') else None
                        })
        except: pass
        
        # Rate limit kindness
        time.sleep(0.1)
    
    print(f'✓ Fetched HUD data for {len(result)} counties')
    return result

# ==================== MERGE AND SAVE ====================

def merge_all_data(housing_chars, housing_vals, household_econ, demographics, gdp, hud_data, years_meta):
    print('Merging all data sources...')
    merged = {}
    
    # We use housing_chars keys as the base list of counties
    all_fips = set(housing_chars.keys()) | set(household_econ.keys())
    
    for fips in all_fips:
        state_code = STATE_FIPS_TO_CODE.get(fips[:2], '')
        
        merged[fips] = {
            'fips': fips,
            'stateCode': state_code,
            **housing_chars.get(fips, {}),
            **housing_vals.get(fips, {}),
            **household_econ.get(fips, {}),
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
    print('COMPLETE COUNTY DATA FETCH (Census + BEA + HUD)')
    print('='*70 + '\n')
    
    try:
        # Detect latest years
        census_year = detect_latest_census_year()
        bea_year = detect_latest_bea_year()
        hud_fmr_year, hud_il_year = detect_latest_hud_years()
        
        print(f'\n📅 Using: Census={census_year}, BEA={bea_year}, FMR={hud_fmr_year}, IL={hud_il_year}\n')
        
        # Fetch Census/BEA data
        housing_chars, hc_year = fetch_with_year_fallback(fetch_housing_characteristics, census_year)
        time.sleep(1)
        housing_vals, hv_year = fetch_with_year_fallback(fetch_housing_values_costs, census_year)
        time.sleep(1)
        household_econ, he_year = fetch_with_year_fallback(fetch_household_economics, census_year)
        time.sleep(1)
        demographics, demo_year = fetch_with_year_fallback(fetch_demographics, census_year)
        time.sleep(1)
        gdp, gdp_year = fetch_with_year_fallback(fetch_bea_gdp, bea_year)
        
        # Fetch HUD data (Optimized)
        hud_data = fetch_hud_county_data_optimized(hud_fmr_year, hud_il_year)
        
        # Compile metadata
        years_meta = {
            'housingCharacteristics': hc_year,
            'housingValues': hv_year,
            'householdEconomics': he_year,
            'demographics': demo_year,
            'gdp': gdp_year,
            'hudFMR': hud_fmr_year,
            'hudIncomeLimits': hud_il_year
        }
        
        # Merge and save
        merged = merge_all_data(housing_chars, housing_vals, household_econ, demographics, gdp, hud_data, years_meta)
        save_to_file(merged, 'counties_economic_data.json', years_meta)
        
        print('\n=== Sample (Los Angeles County) ===')
        if '06037' in merged:
            print(json.dumps(merged['06037'], indent=2))
        
        print(f'\n✅ Complete! Years used: {years_meta}')
        
    except Exception as e:
        print(f'\n❌ ERROR: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()