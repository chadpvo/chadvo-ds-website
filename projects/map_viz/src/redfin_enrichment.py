"""
Redfin ZIP Code Data Enrichment Script
--------------------------------------
Adds geocoding (lat/lon, city names) to Redfin housing market data.
Run this script whenever you download new Redfin data.
"""

import pandas as pd
import requests
from pathlib import Path
import json
from datetime import datetime
import zipfile
import io

# ============================================================================
# CONFIGURATION
# ============================================================================

# Paths (relative to script location)
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

# Input
REDFIN_TSV = DATA_DIR / "redfin" / "raw" / "redfin_zip_all_states.tsv"

# Reference data
ZIP_DATABASE = DATA_DIR / "reference" / "uszips.csv"

# Output
OUTPUT_DIR = DATA_DIR / "redfin" / "processed"
OUTPUT_CSV = OUTPUT_DIR / "redfin_with_geocoding.csv"
OUTPUT_JSON_FULL = OUTPUT_DIR / "redfin_with_geocoding.json"
OUTPUT_JSON_LATEST = OUTPUT_DIR / "redfin_latest_only.json"

# ============================================================================
# STEP 1: DOWNLOAD ZIP CODE DATABASE
# ============================================================================

def download_zip_database():
    """
    Download free ZIP code database with lat/lon and city names.
    Source: SimpleMaps (free basic version)
    """
    print("\n" + "="*80)
    print("DOWNLOADING ZIP CODE DATABASE")
    print("="*80)
    
    url = "https://simplemaps.com/static/data/us-zips/1.82/basic/simplemaps_uszips_basicv1.82.zip"
    
    try:
        print(f"Downloading from SimpleMaps...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        print("Extracting ZIP file...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
            with z.open(csv_name) as csv_file:
                zip_df = pd.read_csv(csv_file)
        
        # Save to reference folder
        ZIP_DATABASE.parent.mkdir(parents=True, exist_ok=True)
        zip_df.to_csv(ZIP_DATABASE, index=False)
        
        print(f"✓ Downloaded {len(zip_df):,} ZIP codes")
        print(f"✓ Saved to: {ZIP_DATABASE}")
        return zip_df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nFallback: Download manually from https://simplemaps.com/data/us-zips")
        print(f"Save as: {ZIP_DATABASE}")
        return None


def load_zip_database():
    """Load existing ZIP database or download if missing."""
    
    if ZIP_DATABASE.exists():
        print(f"\n✓ Loading existing ZIP database: {ZIP_DATABASE}")
        zip_df = pd.read_csv(ZIP_DATABASE)
        print(f"  {len(zip_df):,} ZIP codes loaded")
        return zip_df
    else:
        print(f"\n⚠ ZIP database not found at: {ZIP_DATABASE}")
        return download_zip_database()


# ============================================================================
# STEP 2: EXTRACT ZIP CODES FROM REDFIN DATA
# ============================================================================

def extract_zip_code(region_string):
    """Extract 5-digit ZIP from REGION field (e.g., 'Zip Code: 77622' -> '77622')"""
    if pd.isna(region_string):
        return None
    
    import re
    match = re.search(r'(\d{5})', str(region_string))
    return match.group(1) if match else None


# ============================================================================
# STEP 3: MERGE AND ENRICH
# ============================================================================

def enrich_redfin_data():
    """Main enrichment function."""
    
    print("\n" + "="*80)
    print("REDFIN DATA ENRICHMENT PIPELINE")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if input file exists
    if not REDFIN_TSV.exists():
        print(f"\n❌ ERROR: Redfin TSV file not found!")
        print(f"Expected location: {REDFIN_TSV}")
        print("\nPlease ensure the file is in the correct location.")
        return
    
    # Load Redfin data
    print(f"\n1️⃣ Loading Redfin data...")
    print(f"   Source: {REDFIN_TSV}")
    redfin_df = pd.read_csv(REDFIN_TSV, sep='\t', low_memory=False)
    print(f"   ✓ Loaded {len(redfin_df):,} records")
    
    # Extract ZIP codes
    print(f"\n2️⃣ Extracting ZIP codes from REGION field...")
    redfin_df['ZIP'] = redfin_df['REGION'].apply(extract_zip_code)
    valid_zips = redfin_df['ZIP'].notna().sum()
    print(f"   ✓ Extracted {valid_zips:,} valid ZIP codes ({valid_zips/len(redfin_df)*100:.1f}%)")
    
    # Load ZIP database
    print(f"\n3️⃣ Loading ZIP code reference database...")
    zip_df = load_zip_database()
    
    if zip_df is None:
        print("\n❌ Cannot proceed without ZIP database. Exiting.")
        return
    
    # Prepare ZIP database columns
    print(f"\n4️⃣ Preparing ZIP database for merge...")
    zip_cols_mapping = {
        'zip': 'ZIP',
        'lat': 'LATITUDE',
        'lng': 'LONGITUDE',
        'city': 'CITY_NAME',
        'state_id': 'STATE_ABBREV',
        'state_name': 'STATE_FULL_NAME',
        'county_name': 'COUNTY_NAME',
        'population': 'ZIP_POPULATION',
        'density': 'POPULATION_DENSITY'
    }
    
    # Keep only columns that exist
    available_cols = {k: v for k, v in zip_cols_mapping.items() if k in zip_df.columns}
    zip_df_clean = zip_df[list(available_cols.keys())].copy()
    zip_df_clean = zip_df_clean.rename(columns=available_cols)
    
    # Ensure ZIP is 5-digit string with leading zeros
    zip_df_clean['ZIP'] = zip_df_clean['ZIP'].astype(str).str.zfill(5)
    redfin_df['ZIP'] = redfin_df['ZIP'].astype(str).str.zfill(5)
    
    print(f"   ✓ Prepared {len(zip_df_clean):,} ZIP codes")
    print(f"   ✓ Columns to merge: {', '.join(available_cols.values())}")
    
    # Merge datasets
    print(f"\n5️⃣ Merging Redfin data with geocoding...")
    merged_df = redfin_df.merge(zip_df_clean, on='ZIP', how='left')
    
    # Calculate match statistics
    matched = merged_df['LATITUDE'].notna().sum()
    total = len(merged_df)
    match_rate = (matched / total) * 100
    
    print(f"   ✓ Merge complete!")
    print(f"   ✓ Matched: {matched:,} / {total:,} records ({match_rate:.1f}%)")
    
    if matched < total:
        unmatched = total - matched
        print(f"   ⚠ Unmatched: {unmatched:,} records ({100-match_rate:.1f}%)")
    
    # Convert dates to string for JSON compatibility
    merged_df['PERIOD_BEGIN'] = pd.to_datetime(merged_df['PERIOD_BEGIN']).dt.strftime('%Y-%m-%d')
    merged_df['PERIOD_END'] = pd.to_datetime(merged_df['PERIOD_END']).dt.strftime('%Y-%m-%d')
    
    # Handle null values for JSON
    merged_df = merged_df.replace({pd.NA: None, pd.NaT: None})
    merged_df = merged_df.where(pd.notnull(merged_df), None)
    
    return merged_df


# ============================================================================
# STEP 4: SAVE OUTPUTS
# ============================================================================

def save_outputs(merged_df):
    """Save enriched data in multiple formats."""
    
    print(f"\n6️⃣ Saving outputs...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Save full CSV
    print(f"\n   📄 Saving CSV (full dataset)...")
    merged_df.to_csv(OUTPUT_CSV, index=False)
    csv_size = OUTPUT_CSV.stat().st_size / 1024 / 1024
    print(f"      ✓ {OUTPUT_CSV}")
    print(f"      ✓ Size: {csv_size:.1f} MB")
    
    # Save full JSON
    print(f"\n   📄 Saving JSON (full dataset)...")
    full_json = {
        "metadata": {
            "source": "Redfin Market Tracker",
            "enrichment_date": datetime.now().isoformat(),
            "total_records": len(merged_df),
            "date_range": {
                "start": merged_df['PERIOD_BEGIN'].min(),
                "end": merged_df['PERIOD_END'].max()
            },
            "columns": list(merged_df.columns)
        },
        "data": merged_df.to_dict('records')
    }
    
    with open(OUTPUT_JSON_FULL, 'w') as f:
        json.dump(full_json, f, indent=2)
    
    json_full_size = OUTPUT_JSON_FULL.stat().st_size / 1024 / 1024
    print(f"      ✓ {OUTPUT_JSON_FULL}")
    print(f"      ✓ Size: {json_full_size:.1f} MB")
    
    # Save latest period only (smaller for web use)
    print(f"\n   📄 Saving JSON (latest period only)...")
    merged_df['PERIOD_END_DT'] = pd.to_datetime(merged_df['PERIOD_END'])
    latest_df = merged_df.sort_values('PERIOD_END_DT').groupby('ZIP').tail(1)
    latest_df = latest_df.drop('PERIOD_END_DT', axis=1)
    
    latest_json = {
        "metadata": {
            "source": "Redfin Market Tracker",
            "enrichment_date": datetime.now().isoformat(),
            "total_records": len(latest_df),
            "latest_period": latest_df['PERIOD_END'].max(),
            "note": "Contains only the most recent period for each ZIP code",
            "columns": list(latest_df.columns)
        },
        "data": latest_df.to_dict('records')
    }
    
    with open(OUTPUT_JSON_LATEST, 'w') as f:
        json.dump(latest_json, f, indent=2)
    
    json_latest_size = OUTPUT_JSON_LATEST.stat().st_size / 1024 / 1024
    print(f"      ✓ {OUTPUT_JSON_LATEST}")
    print(f"      ✓ Size: {json_latest_size:.1f} MB")
    
    # Summary
    print(f"\n" + "="*80)
    print("ENRICHMENT COMPLETE! ✨")
    print("="*80)
    print(f"\nOutput files:")
    print(f"  1. CSV (full):         {OUTPUT_CSV.name} ({csv_size:.1f} MB)")
    print(f"  2. JSON (full):        {OUTPUT_JSON_FULL.name} ({json_full_size:.1f} MB)")
    print(f"  3. JSON (latest only): {OUTPUT_JSON_LATEST.name} ({json_latest_size:.1f} MB)")
    
    print(f"\n📊 Records:")
    print(f"  - Full dataset:   {len(merged_df):,} records")
    print(f"  - Latest period:  {len(latest_df):,} unique ZIP codes")
    
    print(f"\n💡 Next steps:")
    print(f"  - Use '{OUTPUT_JSON_LATEST.name}' for your Mapbox visualization")
    print(f"  - Commit these files to GitHub (check sizes first!)")
    print(f"  - Re-run this script when Redfin releases new data")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "🏠 "*20)
    print("REDFIN DATA GEOCODING ENRICHMENT SCRIPT")
    print("🏠 "*20)
    
    # Run enrichment
    merged_df = enrich_redfin_data()
    
    if merged_df is not None:
        save_outputs(merged_df)
    else:
        print("\n❌ Enrichment failed. Please check errors above.")
    
    print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "="*80)