"""
Complete State-Level Data Fetcher
Fetches Census ACS, BEA GDP, and HUD (FMR + Income Limits) data for all US states
Automatically uses the latest available year for each dataset
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

# State FIPS to Code Mapping
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
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetParameterValues&datasetname=Regional&ParameterName=Year&TableName=SAGDP2&ResultFormat=JSON'
        data = session.get(url, timeout=30).json()
        if 'BEAAPI' in data and 'Results' in data['BEAAPI']:
            years = [int(i['Key']) for i in data['BEAAPI']['Results']['ParamValue'] if i['Key'].isdigit()]
            if years:
                print(f'✓ Latest BEA: {max(years)}')
                return max(years)
    except: pass
    return 2023

def detect_latest_hud_years():
    """Detect latest available HUD FMR and Income Limits years"""
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

# ==================== CENSUS DATA FETCHERS ====================

def fetch_housing_characteristics(year):
    print(f'Fetching housing characteristics (ACS {year})...')
    session = create_session_with_retries()
    
    variables = [
        'NAME', 'B25001_001E', 'B25002_001E', 'B25002_002E', 'B25002_003E',
        'B25003_001E', 'B25003_002E', 'B25003_003E', 'B25024_002E',
        'B25024_003E', 'B25035_001E'
    ]
    
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=30).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-1]
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
        
        print(f'✓ Fetched housing characteristics for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_housing_values_costs(year):
    print(f'Fetching housing values & costs (ACS {year})...')
    session = create_session_with_retries()
    
    variables = ['NAME', 'B25077_001E', 'B25064_001E', 'B25088_002E', 'B25088_003E']
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=30).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-1]
            result[fips] = {
                'medianHomeValue': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'medianGrossRent': int(row[2]) if row[2] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsWithMortgage': int(row[3]) if row[3] not in ['-666666666', 'null'] else None,
                'medianOwnerCostsNoMortgage': int(row[4]) if row[4] not in ['-666666666', 'null'] else None
            }
        
        print(f'✓ Fetched housing values for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_household_economics(year):
    print(f'Fetching household economics (ACS {year})...')
    session = create_session_with_retries()
    
    variables = ['NAME', 'B19013_001E', 'B17001_001E', 'B17001_002E']
    url = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables)}&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        data = session.get(url, timeout=30).json()
        result = {}
        
        for row in data[1:]:
            fips = row[-1]
            total_pop = int(row[2]) if row[2] not in ['-666666666', 'null'] else None
            poverty = int(row[3]) if row[3] not in ['-666666666', 'null'] else None
            
            result[fips] = {
                'medianHouseholdIncome': int(row[1]) if row[1] not in ['-666666666', 'null'] else None,
                'povertyRate': round((poverty / total_pop) * 100, 1) if total_pop and poverty else None
            }
        
        print(f'✓ Fetched household economics for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_demographics(year):
    print(f'Fetching demographics (ACS {year})...')
    session = create_session_with_retries()
    
    # Get total population and median age from base tables
    variables_base = ['NAME', 'B01003_001E', 'B01002_001E']
    url_base = f'https://api.census.gov/data/{year}/acs/acs5?get={",".join(variables_base)}&for=state:*&key={CENSUS_API_KEY}'
    
    # Get employment/unemployment rates from Subject Table S2301 (pre-calculated by Census)
    # S2301_C03_001E = Employment Rate (% of population 16+ that is employed)
    # S2301_C04_001E = Unemployment Rate (% of labor force that is unemployed)
    variables_subject = ['NAME', 'S2301_C03_001E', 'S2301_C04_001E']
    url_subject = f'https://api.census.gov/data/{year}/acs/acs5/subject?get={",".join(variables_subject)}&for=state:*&key={CENSUS_API_KEY}'
    
    try:
        result = {}
        
        # Fetch base demographics
        base_data = session.get(url_base, timeout=30).json()
        for row in base_data[1:]:
            fips = row[-1]
            result[fips] = {
                'totalPopulation': int(row[1]) if row[1] not in ['-666666666', 'null', None] else None,
                'medianAge': float(row[2]) if row[2] not in ['-666666666', 'null', None] else None
            }
        
        # Fetch employment rates (pre-calculated by Census)
        subject_data = session.get(url_subject, timeout=30).json()
        for row in subject_data[1:]:
            fips = row[-1]
            if fips in result:
                result[fips]['employmentRate'] = float(row[1]) if row[1] not in ['-666666666', 'null', None, ''] else None
                result[fips]['unemploymentRate'] = float(row[2]) if row[2] not in ['-666666666', 'null', None, ''] else None
        
        print(f'✓ Fetched demographics for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

def fetch_bea_gdp(year):
    print(f'Fetching GDP (BEA {year})...')
    session = create_session_with_retries()
    
    try:
        url = f'https://apps.bea.gov/api/data/?UserID={BEA_API_KEY}&method=GetData&datasetname=Regional&TableName=SAGDP2&LineCode=1&Year={year}&GeoFips=STATE&ResultFormat=JSON'
        data = session.get(url, timeout=30).json()
        
        result = {}
        if 'BEAAPI' in data and 'Results' in data['BEAAPI'] and 'Data' in data['BEAAPI']['Results']:
            for item in data['BEAAPI']['Results']['Data']:
                geo_fips = item.get('GeoFips', '')[:2]
                val = item.get('DataValue')
                if geo_fips in STATE_FIPS and val:
                    try:
                        result[geo_fips] = {'gdpTotal': int(float(val.replace(',', '')))}
                    except: pass
        else:
            return {}
        
        print(f'✓ Fetched GDP for {len(result)} states')
        return result
    except Exception as e:
        print(f'⚠ Error: {e}')
        return {}

# ==================== HUD DATA FETCHERS ====================

def fetch_hud_fmr_state(year):
    """Fetch Fair Market Rents for all states"""
    print(f'Fetching HUD FMR (year {year})...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    result = {}
    
    for fips, state_code in STATE_FIPS_TO_CODE.items():
        try:
            url = f"{HUD_FMR_BASE}/statedata/{state_code}"
            resp = session.get(url, headers=headers, params={'year': year}, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json().get('data', {})
                if data and 'metroareas' in data and data['metroareas']:
                    # Calculate state-wide averages from metro areas
                    metros = data['metroareas']
                    avg_0br = sum(int(m.get('Efficiency', 0)) for m in metros if m.get('Efficiency')) / len([m for m in metros if m.get('Efficiency')])
                    avg_1br = sum(int(m.get('One-Bedroom', 0)) for m in metros if m.get('One-Bedroom')) / len([m for m in metros if m.get('One-Bedroom')])
                    avg_2br = sum(int(m.get('Two-Bedroom', 0)) for m in metros if m.get('Two-Bedroom')) / len([m for m in metros if m.get('Two-Bedroom')])
                    avg_3br = sum(int(m.get('Three-Bedroom', 0)) for m in metros if m.get('Three-Bedroom')) / len([m for m in metros if m.get('Three-Bedroom')])
                    avg_4br = sum(int(m.get('Four-Bedroom', 0)) for m in metros if m.get('Four-Bedroom')) / len([m for m in metros if m.get('Four-Bedroom')])
                    
                    result[fips] = {
                        'fmr0Bedroom': round(avg_0br),
                        'fmr1Bedroom': round(avg_1br),
                        'fmr2Bedroom': round(avg_2br),
                        'fmr3Bedroom': round(avg_3br),
                        'fmr4Bedroom': round(avg_4br)
                    }
            time.sleep(0.1)
        except: continue
    
    print(f'✓ Fetched FMR for {len(result)} states')
    return result

def fetch_hud_income_limits_state(year):
    """Fetch Income Limits for all states"""
    print(f'Fetching HUD Income Limits (year {year})...')
    session = create_session_with_retries()
    headers = {"Authorization": f"Bearer {HUD_API_TOKEN}"}
    result = {}
    
    for fips, state_code in STATE_FIPS_TO_CODE.items():
        try:
            url = f"{HUD_IL_BASE}/statedata/{state_code}"
            resp = session.get(url, headers=headers, params={'year': year}, timeout=30)
            
            if resp.status_code == 200:
                data_list = resp.json().get('data', [])
                if data_list:
                    # Calculate state-wide averages
                    medians = [d.get('median_income') for d in data_list if d.get('median_income')]
                    il80_p4s = [d.get('low', {}).get('il80_p4') for d in data_list if d.get('low', {}).get('il80_p4')]
                    
                    if medians:
                        result[fips] = {
                            'medianFamilyIncome': round(sum(medians) / len(medians)),
                            'incomeLimitLow80_4person': round(sum(il80_p4s) / len(il80_p4s)) if il80_p4s else None
                        }
            time.sleep(0.1)
        except: continue
    
    print(f'✓ Fetched Income Limits for {len(result)} states')
    return result

# ==================== MERGE AND SAVE ====================

def merge_all_data(housing_chars, housing_vals, household_econ, demographics, gdp, hud_fmr, hud_il, years_meta):
    print('Merging all data sources...')
    merged = {}
    
    for fips, name in STATE_FIPS.items():
        merged[fips] = {
            'fips': fips,
            'name': name,
            'stateCode': STATE_FIPS_TO_CODE.get(fips, ''),
            **housing_chars.get(fips, {}),
            **housing_vals.get(fips, {}),
            **household_econ.get(fips, {}),
            **demographics.get(fips, {}),
            'gdpTotal': gdp.get(fips, {}).get('gdpTotal'),
            **hud_fmr.get(fips, {}),
            **hud_il.get(fips, {}),
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
    print('COMPLETE STATE DATA FETCH (Census + BEA + HUD)')
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
        
        # Fetch HUD data
        hud_fmr, fmr_year = fetch_with_year_fallback(fetch_hud_fmr_state, hud_fmr_year)
        time.sleep(1)
        hud_il, il_year = fetch_with_year_fallback(fetch_hud_income_limits_state, hud_il_year)
        
        # Compile metadata
        years_meta = {
            'housingCharacteristics': hc_year,
            'housingValues': hv_year,
            'householdEconomics': he_year,
            'demographics': demo_year,
            'gdp': gdp_year,
            'hudFMR': fmr_year,
            'hudIncomeLimits': il_year
        }
        
        # Merge and save
        merged = merge_all_data(housing_chars, housing_vals, household_econ, demographics, gdp, hud_fmr, hud_il, years_meta)
        save_to_file(merged, 'states_economic_data.json', years_meta)
        
        print('\n=== Sample (California) ===')
        if '06' in merged:
            print(json.dumps(merged['06'], indent=2))
        
        print(f'\n✅ Complete! Years used: {years_meta}')
        
    except Exception as e:
        print(f'\n❌ ERROR: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()