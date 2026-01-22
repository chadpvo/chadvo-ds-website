"""
Test Script: Verify Unemployment Rate Data from Census
Tests State, County, and CBSA levels for California only
"""

import requests
import json

CENSUS_API_KEY = '7e9febefb3835ac0c2796d2e00df516e60c3e406'
YEAR = 2023

print("="*70)
print("UNEMPLOYMENT RATE DATA TESTER - CALIFORNIA ONLY")
print("="*70)

# ==================== TEST 1: STATE LEVEL ====================
print("\n" + "="*70)
print("TEST 1: STATE LEVEL (California)")
print("="*70)

print("\nAttempt 1: Using Subject Table S2301 (Employment Status)")
url = f'https://api.census.gov/data/{YEAR}/acs/acs5/subject?get=NAME,S2301_C03_001E,S2301_C04_001E&for=state:06&key={CENSUS_API_KEY}'
try:
    response = requests.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Response: {json.dumps(data, indent=2)}")
    
    if len(data) > 1:
        emp_rate = data[1][1]
        unemp_rate = data[1][2]
        print(f"\n✅ SUCCESS!")
        print(f"   Employment Rate: {emp_rate}%")
        print(f"   Unemployment Rate: {unemp_rate}%")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "-"*70)
print("Attempt 2: Using Base Table B23025 (Employment Status)")
url = f'https://api.census.gov/data/{YEAR}/acs/acs5?get=NAME,B23025_001E,B23025_002E,B23025_003E,B23025_004E,B23025_005E&for=state:06&key={CENSUS_API_KEY}'
try:
    response = requests.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Response: {json.dumps(data, indent=2)}")
    
    if len(data) > 1:
        total_16plus = int(data[1][1]) if data[1][1] != 'null' else None
        in_labor_force = int(data[1][2]) if data[1][2] != 'null' else None
        civilian_labor = int(data[1][3]) if data[1][3] != 'null' else None
        employed = int(data[1][4]) if data[1][4] != 'null' else None
        unemployed = int(data[1][5]) if data[1][5] != 'null' else None
        
        print(f"\n✅ RAW DATA:")
        print(f"   Total 16+: {total_16plus:,}")
        print(f"   In Labor Force: {in_labor_force:,}")
        print(f"   Employed: {employed:,}")
        print(f"   Unemployed: {unemployed:,}")
        
        if in_labor_force:
            emp_rate = round((employed / in_labor_force) * 100, 1)
            unemp_rate = round((unemployed / in_labor_force) * 100, 1)
            print(f"\n   CALCULATED:")
            print(f"   Employment Rate: {emp_rate}%")
            print(f"   Unemployment Rate: {unemp_rate}%")
except Exception as e:
    print(f"❌ Error: {e}")

# ==================== TEST 2: COUNTY LEVEL ====================
print("\n\n" + "="*70)
print("TEST 2: COUNTY LEVEL (California Counties)")
print("="*70)

print("\nAttempt 1: Subject Table S2301 for Counties in CA")
url = f'https://api.census.gov/data/{YEAR}/acs/acs5/subject?get=NAME,S2301_C03_001E,S2301_C04_001E&for=county:*&in=state:06&key={CENSUS_API_KEY}'
try:
    response = requests.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if len(data) > 1:
        print(f"\n✅ Got {len(data) - 1} counties")
        print(f"\nSample (first 5 counties):")
        for row in data[1:6]:
            name = row[0]
            emp = row[1]
            unemp = row[2]
            print(f"   {name}: Emp={emp}%, Unemp={unemp}%")
    else:
        print(f"❌ No data returned")
        print(f"Response: {json.dumps(data, indent=2)}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "-"*70)
print("Attempt 2: Base Table B23025 for Counties")
url = f'https://api.census.gov/data/{YEAR}/acs/acs5?get=NAME,B23025_003E,B23025_004E,B23025_005E&for=county:*&in=state:06&key={CENSUS_API_KEY}'
try:
    response = requests.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if len(data) > 1:
        print(f"\n✅ Got {len(data) - 1} counties")
        print(f"\nSample calculations (first 5 counties):")
        for row in data[1:6]:
            name = row[0]
            labor = int(row[1]) if row[1] != 'null' else None
            employed = int(row[2]) if row[2] != 'null' else None
            unemployed = int(row[3]) if row[3] != 'null' else None
            
            if labor:
                unemp_rate = round((unemployed / labor) * 100, 1) if unemployed else None
                print(f"   {name}: LaborForce={labor:,}, Unemp={unemployed:,}, Rate={unemp_rate}%")
except Exception as e:
    print(f"❌ Error: {e}")

# ==================== TEST 3: CBSA LEVEL ====================
print("\n\n" + "="*70)
print("TEST 3: CBSA/METRO LEVEL (California CBSAs)")
print("="*70)

CBSA_GEO = 'metropolitan%20statistical%20area/micropolitan%20statistical%20area:*'

print("\nAttempt 1: Subject Table S2301 for All CBSAs")
url = f'https://api.census.gov/data/{YEAR}/acs/acs5/subject?get=NAME,S2301_C03_001E,S2301_C04_001E&for={CBSA_GEO}&key={CENSUS_API_KEY}'
try:
    response = requests.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if len(data) > 1:
        # Filter for CA CBSAs
        ca_cbsas = [row for row in data[1:] if 'CA' in row[0]]
        print(f"\n✅ Total CBSAs: {len(data) - 1}")
        print(f"✅ CA-related CBSAs: {len(ca_cbsas)}")
        print(f"\nSample CA CBSAs (first 5):")
        for row in ca_cbsas[:5]:
            name = row[0]
            emp = row[1]
            unemp = row[2]
            print(f"   {name}")
            print(f"      Employment: {emp}%, Unemployment: {unemp}%")
    else:
        print(f"❌ No data returned")
        print(f"Response: {json.dumps(data, indent=2)}")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "-"*70)
print("Attempt 2: Base Table B23025 for CBSAs")
url = f'https://api.census.gov/data/{YEAR}/acs/acs5?get=NAME,B23025_003E,B23025_004E,B23025_005E&for={CBSA_GEO}&key={CENSUS_API_KEY}'
try:
    response = requests.get(url, timeout=30)
    print(f"Status: {response.status_code}")
    data = response.json()
    
    if len(data) > 1:
        ca_cbsas = [row for row in data[1:] if 'CA' in row[0]]
        print(f"\n✅ Total CBSAs: {len(data) - 1}")
        print(f"✅ CA-related CBSAs: {len(ca_cbsas)}")
        print(f"\nSample calculations (first 5 CA CBSAs):")
        for row in ca_cbsas[:5]:
            name = row[0]
            labor = int(row[1]) if row[1] != 'null' else None
            employed = int(row[2]) if row[2] != 'null' else None
            unemployed = int(row[3]) if row[3] != 'null' else None
            
            if labor:
                unemp_rate = round((unemployed / labor) * 100, 1) if unemployed else None
                print(f"   {name}")
                print(f"      Unemployed: {unemployed:,}, Rate: {unemp_rate}%")
except Exception as e:
    print(f"❌ Error: {e}")

# ==================== SUMMARY ====================
print("\n\n" + "="*70)
print("TESTING COMPLETE - SUMMARY")
print("="*70)
print("\nKey Findings:")
print("1. Check which methods returned unemployment rate successfully")
print("2. Compare Subject Table (S2301) vs Base Table (B23025)")
print("3. Identify if the issue is with:")
print("   - API availability")
print("   - Data structure")
print("   - Variable codes")
print("   - Geographic level support")
print("\nNext Steps:")
print("- Use the working method in production scripts")
print("- Document which variables work at which geographic levels")
print("="*70)