"""
Complete Redfin Data Pipeline
==============================
1. Downloads raw Redfin data for State, County, and CBSA
2. Processes and adds FIPS codes to county data
3. Ready for final aggregation step

Usage:
    python download_and_process_redfin.py
"""

import requests
import gzip
import io
import csv
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Base directory (adjust to your project path)
BASE_DIR = Path(r"C:\personal_projects\chadvo-ds-website\projects\map_viz")
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "redfin" / "raw"
PROCESSED_DIR = DATA_DIR / "redfin" / "processed"
REFERENCE_DIR = DATA_DIR / "reference"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# STEP 1: DOWNLOAD REDFIN DATA
# ============================================================================

def download_redfin_regions():
    """
    Downloads Redfin data for State, County, and CBSA.
    Filters for 3-month rolling average (90 days).
    """
    print("\n" + "="*80)
    print("STEP 1: DOWNLOADING REDFIN DATA")
    print("="*80)
    
    base_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker'
    
    targets = {
        'state':  f'{base_url}/state_market_tracker.tsv000.gz',
        'county': f'{base_url}/county_market_tracker.tsv000.gz',
        'cbsa':   'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz'
    }
    
    print(f"📂 Output Directory: {RAW_DIR}")

    for region_type, url in targets.items():
        print("\n" + "-"*70)
        print(f"PROCESSING: {region_type.upper()}")
        print("-"*70)
        
        try:
            print(f"📥 Downloading from {url}...")
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            # Decompress
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                content = f.read().decode('utf-8')
            
            lines = content.strip().split('\n')
            reader = csv.reader(lines, delimiter='\t', quotechar='"')
            headers = next(reader)
            
            # Find period_duration column
            try:
                duration_idx = headers.index('period_duration')
            except ValueError:
                duration_idx = -1

            filtered_data = []
            
            print(f"🔍 Filtering for 3-Month Rolling Average (90 days)...")
            
            for row in reader:
                if duration_idx != -1:
                    # Filter for '90' days
                    if '90' not in row[duration_idx]: 
                        continue
                
                filtered_data.append(dict(zip(headers, row)))

            # Save as JSON
            output_file = RAW_DIR / f'redfin_{region_type}_3mo.json'
            
            print(f"💾 Saving {len(filtered_data):,} records...")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'metadata': {
                        'region': region_type,
                        'filter': '3-month rolling (90 days)',
                        'count': len(filtered_data),
                        'downloaded': datetime.now().isoformat()
                    },
                    'data': filtered_data
                }, f, indent=2)
                
            file_size = output_file.stat().st_size / 1024 / 1024
            print(f"✅ Saved: {output_file} ({file_size:.1f} MB)")

        except Exception as e:
            print(f"❌ Error processing {region_type}: {e}")
            return False
    
    return True


# ============================================================================
# STEP 2: ADD FIPS CODES TO COUNTY DATA
# ============================================================================

def add_fips_to_counties():
    """
    Adds county FIPS codes to the county data by matching county names
    from the uszips.csv reference file.
    """
    print("\n" + "="*80)
    print("STEP 2: ADDING FIPS CODES TO COUNTY DATA")
    print("="*80)
    
    # Input files
    county_file = RAW_DIR / 'redfin_county_3mo.json'
    zip_reference = REFERENCE_DIR / 'uszips.csv'
    
    # Check if files exist
    if not county_file.exists():
        print(f"❌ County file not found: {county_file}")
        return False
    
    if not zip_reference.exists():
        print(f"❌ Reference file not found: {zip_reference}")
        return False
    
    # Load county data
    print("\n1️⃣  Loading Redfin county data...")
    with open(county_file, 'r') as f:
        redfin_data = json.load(f)
    
    records = redfin_data['data']
    print(f"✓ Loaded {len(records):,} county records")
    
    # Load FIPS reference
    print("\n2️⃣  Loading county FIPS reference...")
    zip_ref = pd.read_csv(zip_reference, dtype={'county_fips': str, 'state_id': str})
    
    # Create county name lookup
    county_lookup = {}
    for _, row in zip_ref.iterrows():
        if pd.notna(row['county_name']) and pd.notna(row['state_id']) and pd.notna(row['county_fips']):
            county_name = row['county_name']
            state_id = row['state_id']
            fips = str(row['county_fips']).zfill(5)
            
            # Create multiple lookup keys for better matching
            if 'County' not in county_name and 'Parish' not in county_name and 'Borough' not in county_name:
                key1 = f"{county_name} County, {state_id}"
                county_lookup[key1] = fips
            
            key2 = f"{county_name}, {state_id}"
            county_lookup[key2] = fips
    
    print(f"✓ Created lookup for {len(county_lookup):,} unique county keys")
    
    # Match and add FIPS codes
    print("\n3️⃣  Matching Redfin counties to FIPS codes...")
    
    matched = 0
    unmatched = []
    
    for record in records:
        region = record.get('REGION', '')
        
        if region in county_lookup:
            record['COUNTY_FIPS'] = county_lookup[region]
            matched += 1
        else:
            # Try with "County" suffix if missing
            if ', ' in region:
                county_part, state_part = region.rsplit(', ', 1)
                if 'County' not in county_part:
                    alt_region = f"{county_part} County, {state_part}"
                    if alt_region in county_lookup:
                        record['COUNTY_FIPS'] = county_lookup[alt_region]
                        matched += 1
                        continue
            
            unmatched.append(region)
    
    print(f"✓ Matched: {matched:,} counties ({matched / len(records) * 100:.1f}%)")
    print(f"⚠ Unmatched: {len(unmatched):,} counties")
    
    if len(unmatched) > 0 and len(unmatched) <= 10:
        print("\nSample unmatched counties:")
        for county in unmatched[:10]:
            print(f"  - {county}")
    
    # Save updated data back to raw folder
    print(f"\n4️⃣  Saving updated county data...")
    
    redfin_data['metadata']['fips_codes_added'] = datetime.now().isoformat()
    redfin_data['metadata']['counties_with_fips'] = matched
    redfin_data['metadata']['counties_without_fips'] = len(unmatched)
    
    with open(county_file, 'w') as f:
        json.dump(redfin_data, f, indent=2)
    
    file_size = county_file.stat().st_size / 1024 / 1024
    print(f"✓ Updated: {county_file} ({file_size:.1f} MB)")
    
    return True


# ============================================================================
# STEP 3: ADD CBSA CODES
# ============================================================================

def add_cbsa_codes():
    """
    Adds CBSA_CODE field to CBSA data using PARENT_METRO_REGION_METRO_CODE.
    This field already matches our economic CBSA codes at 96.7% rate.
    """
    print("\n" + "="*80)
    print("STEP 3: ADDING CBSA CODES")
    print("="*80)
    
    cbsa_file = RAW_DIR / 'redfin_cbsa_3mo.json'
    
    if not cbsa_file.exists():
        print(f"❌ CBSA file not found: {cbsa_file}")
        return False
    
    print("\n1️⃣  Loading Redfin CBSA data...")
    with open(cbsa_file, 'r') as f:
        redfin_data = json.load(f)
    
    records = redfin_data['data']
    print(f"✓ Loaded {len(records):,} CBSA records")
    
    print("\n2️⃣  Adding CBSA_CODE field...")
    
    added = 0
    for record in records:
        metro_code = record.get('PARENT_METRO_REGION_METRO_CODE')
        if metro_code and metro_code != 'NA':
            # Use the metro code directly as CBSA code
            record['CBSA_CODE'] = str(metro_code).zfill(5)  # Zero-pad to 5 digits
            added += 1
    
    print(f"✓ Added CBSA_CODE to {added:,} records ({added / len(records) * 100:.1f}%)")
    
    print(f"\n3️⃣  Saving updated CBSA data...")
    
    redfin_data['metadata']['cbsa_codes_added'] = datetime.now().isoformat()
    redfin_data['metadata']['records_with_cbsa_code'] = added
    
    with open(cbsa_file, 'w') as f:
        json.dump(redfin_data, f, indent=2)
    
    file_size = cbsa_file.stat().st_size / 1024 / 1024
    print(f"✓ Updated: {cbsa_file} ({file_size:.1f} MB)")
    
    return True


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def main():
    """
    Complete pipeline: Download → Add FIPS codes
    """
    print("\n" + "="*80)
    print("REDFIN DATA DOWNLOAD & PROCESSING PIPELINE")
    print("="*80)
    print(f"Base Directory: {BASE_DIR}")
    print(f"Data Directory: {DATA_DIR}")
    print("="*80)
    
    # Step 1: Download
    success = download_redfin_regions()
    if not success:
        print("\n❌ Download failed. Aborting.")
        return
    
    # Step 2: Add FIPS codes to counties
    success = add_fips_to_counties()
    if not success:
        print("\n❌ FIPS code addition failed.")
        return
    
    # Step 3: Add CBSA codes
    success = add_cbsa_codes()
    if not success:
        print("\n⚠️ CBSA code addition had issues (but continuing).")
    
    # Summary
    print("\n" + "="*80)
    print("✅ PIPELINE COMPLETE!")
    print("="*80)
    print("\nFiles created:")
    print(f"  • {RAW_DIR / 'redfin_state_3mo.json'}")
    print(f"  • {RAW_DIR / 'redfin_county_3mo.json'} (with FIPS codes)")
    print(f"  • {RAW_DIR / 'redfin_cbsa_3mo.json'} (with CBSA codes)")
    print("\nNext step:")
    print("  Run: python process_all_redfin_levels.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()