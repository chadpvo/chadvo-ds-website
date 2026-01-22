"""
Complete CBSA-Level Data Fetcher
Fetches Census ACS, BEA GDP, and HUD (FMR + Income Limits) for all US CBSAs.
(Metropolitan and Micropolitan Statistical Areas)
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

HUD_FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"
HUD_IL_BASE = "https://www.huduser.gov/hudapi/public/il"

# Census Geo for "Metropolitan/Micropolitan Statistical Area"
CBSA_GEO = 'metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'

def create_session_with_retries():
    session = requests.Session()
    retry = Retry(
        total=5, backoff_factor=2, 
        status_forcelist=[429, 500, 502, 503, 504], 
        allowed_methods=["GET"]
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
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

def detect_latest_bea_year():
    print('Detecting latest BEA...')
    session = create_session_with_retries()
    try:
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
    print('Detecting latest HUD...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    
    fmr_year, il_year = None, None
    for year in range(2026, 2015, -1):
        try:
            if not fmr_year and session.get(f"{HUD_FMR_BASE}/listMetroAreas", headers=headers, params={'year': year}, timeout=10).status_code == 200:
                fmr_year = year
            if not il_year and session.get(f"{HUD_IL_BASE}/statedata/CA", headers=headers, params={'year': year}, timeout=10).status_code == 200:
                il_year = year
            if fmr_year and il_year:
                break
        except: continue
    
    print(f'✓ Latest HUD FMR: {fmr_year}, IL: {il_year}')
    return fmr_year or 2024, il_year or 2024

# ==================== CENSUS FETCHERS ====================

def fetch_housing_characteristics(year):
    """Fetches Occupancy, Vacancy, Tenure, Year Built"""
    print(f'Fetching housing characteristics (ACS {year})...')
    session = create_session_with_retries()
    
    variables = [
        'NAME', 'B25001_001E', 'B25002_001E', 'B25002_002E', 'B25002_003E',
        'B25003_001E', 'B25003_002E', 'B25003_003E', 'B25035_001E'
    ]
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for={CBSA_GEO}&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        
        for row in data[1:]:
            cbsa = row[-1]
            
            result[cbsa] = {
                'totalHousingUnits': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'occupiedUnits': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'vacantUnits': int(row[4]) if row[4] not in ['-666666666', 'null'] else None,
                'ownerOccupied': int(row[6]) if row[6] not in ['-666666666', 'null'] else None,
                'renterOccupied': int(row[7]) if row[7] not in ['-666666666', 'null'] else None,
                'medianYearBuilt': int(row[8]) if row[8] not in ['-666666666', 'null'] else None
            }
        
        print(f'✓ Fetched characteristics for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_values_costs(year):
    """Fetches Home Values, Rents, Owner Costs"""
    print(f'Fetching housing values & costs (ACS {year})...')
    session = create_session_with_retries()
    
    variables = ['NAME', 'B25077_001E', 'B25064_001E', 'B25088_002E', 'B25088_003E']
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for={CBSA_GEO}&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        
        for row in data[1:]:
            cbsa = row[-1]
            result[cbsa] = {
                'medianHomeValue': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianGrossRent': int(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsWithMortgage': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsNoMortgage': int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            }
        
        print(f'✓ Fetched values/costs for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_household_economics(year):
    print(f'Fetching household economics (ACS {year})...')
    session = create_session_with_retries()
    url = f'https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E,B17001_001E,B17001_002E&for={CBSA_GEO}&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        for row in data[1:]:
            cbsa = row[4]
            name = row[0]
            
            # Clean Name
            cbsa_type = 'Statistical Area'
            if 'Metro Area' in name: cbsa_type = 'Metropolitan'
            elif 'Micro Area' in name: cbsa_type = 'Micropolitan'
            
            total_pop = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            poverty = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            
            result[cbsa] = {
                'name': name,
                'cbsaCode': cbsa,
                'type': cbsa_type,
                'medianHouseholdIncome': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'povertyRate': round((poverty / total_pop) * 100, 1) if total_pop and poverty else None
            }
        print(f'✓ Fetched for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_demographics(year):
    print(f'Fetching demographics (ACS {year})...')
    session = create_session_with_retries()
    
    # Using Detailed Tables (B23025) for accurate Employment/Unemployment
    variables = [
        'NAME', 'B01003_001E', 'B01002_001E', 
        'B23025_003E', 'B23025_004E', 'B23025_005E'
    ]
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for={CBSA_GEO}&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=60).json()
        result = {}
        for row in data[1:]:
            cbsa = row[-1]
            
            total_pop = int(row[1]) if row[1] not in ['-666666666', 'null'] else None
            median_age = float(row[2]) if row[2] not in ['-666666666', 'null'] else None
            
            civ_labor_force = int(row[3]) if row[3] not in ['-666666666', 'null', None] else 0
            employed = int(row[4]) if row[4] not in ['-666666666', 'null', None] else 0
            unemployed = int(row[5]) if row[5] not in ['-666666666', 'null', None] else 0
            
            emp_rate = round((employed / civ_labor_force) * 100, 1) if civ_labor_force > 0 else None
            unemp_rate = round((unemployed / civ_labor_force) * 100, 1) if civ_labor_force > 0 else None
            
            result[cbsa] = {
                'totalPopulation': total_pop,
                'medianAge': median_age,
                'employmentRate': emp_rate,
                'unemploymentRate': unemp_rate
            }
        print(f'✓ Fetched for {len(result)} CBSAs')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_bea_gdp(year):
    print(f'Fetching GDP (BEA {year})...')
    session = create_session_with_retries()
    result = {}
    
    # BEA splits CBSAs into MSA and MIC (Micropolitan)
    for geo_type in ['MSA', 'MIC']:
        try:
            url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional&TableName=CAGDP2&LineCode=1&Year={year}&GeoFips={geo_type}&ResultFormat=JSON'
            data = session.get(url, timeout=60).json()
            
            if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
                for item in data['BEAAPI']['Results']['Data']:
                    cbsa = item.get('GeoFips')
                    val = item.get('DataValue')
                    if cbsa and val:
                        try:
                            result[cbsa] = {'gdpTotal': int(float(val.replace(',', '')))}
                        except: pass
        except: pass
    
    print(f'✓ Fetched GDP for {len(result)} CBSAs')
    return result

# ==================== HUD FETCHERS ====================

def fetch_hud_cbsa_data(year_fmr, year_il):
    """Fetch HUD FMR and Income Limits for all CBSAs"""
    print(f'Fetching HUD CBSA data (FMR={year_fmr}, IL={year_il})...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    result = {}
    
    # 1. Get list of all HUD Metro Areas
    try:
        resp = session.get(f"{HUD_FMR_BASE}/listMetroAreas", headers=headers, params={'year': year_fmr}, timeout=30)
        if resp.status_code != 200:
            print('⚠ Failed to get HUD metro list')
            return {}
        
        metro_data = resp.json()
        # Handle inconsistent API wrapper
        metro_list = metro_data.get('data', []) if isinstance(metro_data, dict) else metro_data
        
        print(f'  Found {len(metro_list)} metro areas, fetching details...')
        
        # 2. Iterate through Metro Areas
        for idx, metro in enumerate(metro_list):
            cbsa_code = metro.get('cbsa_code')
            if not cbsa_code: continue
            
            if cbsa_code not in result: result[cbsa_code] = {}

            # Fetch FMR
            try:
                fmr_resp = session.get(f"{HUD_FMR_BASE}/data/{cbsa_code}", headers=headers, params={'year': year_fmr}, timeout=10)
                if fmr_resp.status_code == 200:
                    fmr_data = fmr_resp.json().get('data', {}).get('basicdata', {})
                    if fmr_data:
                        result[cbsa_code].update({
                            'fmr0Bedroom': int(fmr_data.get('Efficiency', 0)) if fmr_data.get('Efficiency') else None,
                            'fmr1Bedroom': int(fmr_data.get('One-Bedroom', 0)) if fmr_data.get('One-Bedroom') else None,
                            'fmr2Bedroom': int(fmr_data.get('Two-Bedroom', 0)) if fmr_data.get('Two-Bedroom') else None,
                            'fmr3Bedroom': int(fmr_data.get('Three-Bedroom', 0)) if fmr_data.get('Three-Bedroom') else None,
                            'fmr4Bedroom': int(fmr_data.get('Four-Bedroom', 0)) if fmr_data.get('Four-Bedroom') else None
                        })
            except: pass
            
            # Fetch Income Limits
            try:
                il_resp = session.get(f"{HUD_IL_BASE}/data/{cbsa_code}", headers=headers, params={'year': year_il}, timeout=10)
                if il_resp.status_code == 200:
                    il_data = il_resp.json().get('data', {})
                    if il_data:
                        result[cbsa_code].update({
                            'medianFamilyIncome': int(il_data['median_income']) if il_data.get('median_income') else None,
                            'incomeLimitLow80_4person': int(il_data['low']['il80_p4']) if il_data.get('low', {}).get('il80_p4') else None
                        })
            except: pass
            
            # Progress Log
            if (idx + 1) % 100 == 0:
                print(f'  Processed {idx + 1}/{len(metro_list)}...')
            
            # Rate limit kindness
            time.sleep(0.05)
            
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}
    
    print(f'✓ Fetched HUD data for {len(result)} CBSAs')
    return result

# ==================== MERGE AND SAVE ====================

def merge_all_data(household_econ, housing_chars, housing_vals, demographics, gdp, hud_data, years_meta):
    print('Merging all data...')
    merged = {}
    
    # Use Census keys as base
    all_cbsas = set(household_econ.keys()) | set(gdp.keys())
    
    for cbsa in all_cbsas:
        base_info = household_econ.get(cbsa, {})
        
        merged[cbsa] = {
            'cbsaCode': cbsa,
            'name': base_info.get('name'),
            'type': base_info.get('type'),
            'medianHouseholdIncome': base_info.get('medianHouseholdIncome'),
            'povertyRate': base_info.get('povertyRate'),
            **housing_chars.get(cbsa, {}),
            **housing_vals.get(cbsa, {}),
            **demographics.get(cbsa, {}),
            'gdpTotal': gdp.get(cbsa, {}).get('gdpTotal'),
            **hud_data.get(cbsa, {}),
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
    print('COMPLETE CBSA DATA FETCH (Census + BEA + HUD)')
    print('='*70 + '\n')
    
    try:
        census_year = detect_latest_census_year()
        bea_year = detect_latest_bea_year()
        hud_fmr_year, hud_il_year = detect_latest_hud_years()
        
        print(f'\n📅 Using: Census={census_year}, BEA={bea_year}, FMR={hud_fmr_year}, IL={hud_il_year}\n')
        
        # Census
        household_econ, he_year = fetch_with_year_fallback(fetch_household_economics, census_year)
        time.sleep(1)
        housing_chars, hc_year = fetch_with_year_fallback(fetch_housing_characteristics, census_year)
        time.sleep(1)
        housing_vals, hv_year = fetch_with_year_fallback(fetch_housing_values_costs, census_year)
        time.sleep(1)
        demographics, d_year = fetch_with_year_fallback(fetch_demographics, census_year)
        time.sleep(1)
        
        # BEA
        gdp, g_year = fetch_with_year_fallback(fetch_bea_gdp, bea_year)
        
        # HUD
        hud_data = fetch_hud_cbsa_data(hud_fmr_year, hud_il_year)
        
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
        save_to_file(merged, 'cbsas_economic_data.json', years_meta)
        
        print('\n=== Sample (NYC Metro - 35620) ===')
        if '35620' in merged:
            print(json.dumps(merged['35620'], indent=2))
        elif merged:
            print(json.dumps(merged[list(merged.keys())[0]], indent=2))
        
        print(f'\n✅ Complete! Years: {years_meta}')
        
    except Exception as e:
        print(f'\n❌ ERROR: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()