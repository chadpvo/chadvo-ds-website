"""
Redfin Data Processor - ALL GEOGRAPHIC LEVELS
==============================================
Consolidated script for processing Redfin data at all geographic levels:
- ZIP Code Level
- State Level
- County Level
- CBSA Level

This script:
1. Preserves ALL original data values (no capping/clipping)
2. Applies log transformation for normalization
3. Generates percentile ranks (0.0 to 1.0) for smooth color gradients
4. Applies domain rules to filter out obvious errors
5. Drops unnecessary columns (single-value, null, or redundant)

Why Percentile Ranks?
- Perfect for map visualization: evenly distributed colors
- Preserves original values: show real prices in tooltips
- No artificial spikes: unlike capping, every value keeps its position

Usage:
    python process_all_redfin_levels.py              # Process all levels
    python process_all_redfin_levels.py --zip        # Process only ZIP level
    python process_all_redfin_levels.py --state      # Process only State level
    python process_all_redfin_levels.py --county     # Process only County level
    python process_all_redfin_levels.py --cbsa       # Process only CBSA level
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime, timedelta
import argparse

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

# ============================================================================
# ZIP CODE LEVEL CONFIGURATION
# ============================================================================

ZIP_INPUT_CSV = DATA_DIR / "redfin" / "processed" / "redfin_with_geocoding.csv"
ZIP_OUTPUT_JSON = DATA_DIR / "redfin" / "processed" / "redfin_latest_optimized.json"

ZIP_MONTHS_CUTOFF = 24
ZIP_KEEP_COLUMNS = [
    'ZIP', 'PERIOD_END', 
    'LATITUDE', 'LONGITUDE', 'CITY_NAME', 'STATE_ABBREV',
    'MEDIAN_SALE_PRICE', 'MEDIAN_SALE_PRICE_YOY',
    'INVENTORY', 'INVENTORY_YOY',
    'MEDIAN_DOM', 
    'HOMES_SOLD',
    'SOLD_ABOVE_LIST' 
]

# ============================================================================
# GEOGRAPHIC AGGREGATION LEVEL CONFIGURATION
# ============================================================================

# Input files (raw data from Redfin)
INPUT_DIR = DATA_DIR / "redfin" / "raw"
STATE_INPUT = INPUT_DIR / "redfin_state_3mo.json"
COUNTY_INPUT = INPUT_DIR / "redfin_county_3mo.json"
CBSA_INPUT = INPUT_DIR / "redfin_cbsa_3mo.json"

# Output files (processed with log + ranks)
OUTPUT_DIR = DATA_DIR / "redfin" / "processed"
STATE_OUTPUT = OUTPUT_DIR / "redfin_state_aggregated.json"
COUNTY_OUTPUT = OUTPUT_DIR / "redfin_county_aggregated.json"
CBSA_OUTPUT = OUTPUT_DIR / "redfin_cbsa_aggregated.json"

# Columns to drop (single-value, null, or redundant columns)
COLS_TO_DROP = [
    # Single-value columns (not useful)
    'PERIOD_DURATION',           # All 90
    'REGION_TYPE',               # All "zip code" or similar
    'REGION_TYPE_ID',            # All 2
    'IS_SEASONALLY_ADJUSTED',    # All False
    'LAST_UPDATED',              # All same date
    'PROPERTY_TYPE_ID',          # We have PROPERTY_TYPE (text version)
    'PARENT_METRO_REGION_METRO_CODE',  # We have PARENT_METRO_REGION (text version)
    
    # Columns with 100% nulls
    'CITY',
    'MONTHS_OF_SUPPLY',
    'MONTHS_OF_SUPPLY_MOM',
    'MONTHS_OF_SUPPLY_YOY',
    'PRICE_DROPS',
    'PRICE_DROPS_MOM',
    'PRICE_DROPS_YOY',
]

# Metrics to transform and rank (for State/County/CBSA)
REDFIN_METRICS = [
    'MEDIAN_SALE_PRICE',
    'MEDIAN_SALE_PRICE_YOY',
    'MEDIAN_LIST_PRICE',
    'MEDIAN_LIST_PRICE_YOY',
    'MEDIAN_PPSF',
    'MEDIAN_PPSF_YOY',
    'MEDIAN_LIST_PPSF',
    'MEDIAN_LIST_PPSF_YOY',
    'HOMES_SOLD',
    'HOMES_SOLD_YOY',
    'PENDING_SALES',
    'PENDING_SALES_YOY',
    'NEW_LISTINGS',
    'NEW_LISTINGS_YOY',
    'INVENTORY',
    'INVENTORY_YOY',
    'MEDIAN_DOM',
    'MEDIAN_DOM_YOY',
    'AVG_SALE_TO_LIST',
    'AVG_SALE_TO_LIST_YOY',
    'SOLD_ABOVE_LIST',
    'SOLD_ABOVE_LIST_YOY',
    'OFF_MARKET_IN_TWO_WEEKS',
    'OFF_MARKET_IN_TWO_WEEKS_YOY'
]

# Metrics for log transformation (typically skewed distributions)
LOG_TRANSFORM_METRICS = [
    'MEDIAN_SALE_PRICE',
    'MEDIAN_LIST_PRICE',
    'MEDIAN_PPSF',
    'MEDIAN_LIST_PPSF',
    'HOMES_SOLD',
    'PENDING_SALES',
    'NEW_LISTINGS',
    'INVENTORY',
    'MEDIAN_DOM'
]

# Metrics for percentile ranking (all metrics that will be visualized)
RANK_METRICS = [
    'MEDIAN_SALE_PRICE',
    'MEDIAN_SALE_PRICE_YOY',
    'MEDIAN_LIST_PRICE',
    'MEDIAN_LIST_PRICE_YOY',
    'MEDIAN_PPSF',
    'MEDIAN_PPSF_YOY',
    'MEDIAN_LIST_PPSF',
    'MEDIAN_LIST_PPSF_YOY',
    'HOMES_SOLD',
    'HOMES_SOLD_YOY',
    'PENDING_SALES',
    'PENDING_SALES_YOY',
    'NEW_LISTINGS',
    'NEW_LISTINGS_YOY',
    'INVENTORY',
    'INVENTORY_YOY',
    'MEDIAN_DOM',
    'MEDIAN_DOM_YOY',
    'AVG_SALE_TO_LIST',
    'AVG_SALE_TO_LIST_YOY',
    'SOLD_ABOVE_LIST',
    'SOLD_ABOVE_LIST_YOY',
    'OFF_MARKET_IN_TWO_WEEKS',
    'OFF_MARKET_IN_TWO_WEEKS_YOY'
]

# Domain rules (filter out obvious errors)
MIN_VALID_PRICE = 10000
MAX_VALID_DOM = 730  # 2 years in days

# ============================================================================
# SHARED PROCESSING FUNCTIONS
# ============================================================================

def step1_log_transform(df, metrics, level_name=""):
    """Step 1: Apply log transformation to normalize distribution."""
    prefix = f"{level_name} - " if level_name else ""
    print(f"\n{'='*80}")
    print(f"{prefix}STEP 1: LOG TRANSFORMATION")
    print('='*80)
    
    df_transformed = df.copy()
    transform_stats = {
        'transformed_metrics': [],
        'skewness': {}
    }
    
    for metric in metrics:
        if metric not in df_transformed.columns:
            continue
        
        valid_values = df_transformed[metric].dropna()
        if len(valid_values) == 0:
            continue
        
        # Skewness before
        skew_before = valid_values.skew()
        
        # Create log-transformed column
        log_col_name = f"{metric}_LOG"
        df_transformed[log_col_name] = np.log1p(df_transformed[metric])
        
        # Skewness after
        skew_after = df_transformed[log_col_name].dropna().skew()
        
        transform_stats['transformed_metrics'].append(metric)
        transform_stats['skewness'][metric] = {
            'before': float(skew_before),
            'after': float(skew_after),
            'improvement': float(abs(skew_before) - abs(skew_after))
        }
        
        print(f"  {metric}:")
        print(f"    Skewness: {skew_before:.2f} → {skew_after:.2f}")
        print(f"    Created column: {log_col_name}")
    
    print(f"\n✓ Transformed {len(transform_stats['transformed_metrics'])} metrics")
    
    return df_transformed, transform_stats


def step2_percentile_ranks(df, metrics, level_name=""):
    """Step 2: Generate percentile ranks (0.0 to 1.0) for smooth color gradients."""
    prefix = f"{level_name} - " if level_name else ""
    print(f"\n{'='*80}")
    print(f"{prefix}STEP 2: PERCENTILE RANKING")
    print('='*80)
    
    df_ranked = df.copy()
    rank_stats = {
        'ranked_metrics': [],
        'distribution': {}
    }
    
    for metric in metrics:
        if metric not in df_ranked.columns:
            continue
        
        valid_values = df_ranked[metric].dropna()
        if len(valid_values) == 0:
            continue
        
        # Create percentile rank column (0.0 to 1.0)
        rank_col_name = f"{metric}_RANK"
        
        # Use rank(method='average', pct=True) to get percentile ranks
        df_ranked[rank_col_name] = df_ranked[metric].rank(method='average', pct=True)
        
        # Get distribution stats
        rank_values = df_ranked[rank_col_name].dropna()
        rank_stats['ranked_metrics'].append(metric)
        rank_stats['distribution'][metric] = {
            'min_rank': float(rank_values.min()),
            'max_rank': float(rank_values.max()),
            'median_rank': float(rank_values.median()),
            'unique_ranks': int(rank_values.nunique())
        }
        
        print(f"  {metric}:")
        print(f"    Created column: {rank_col_name}")
        print(f"    Unique ranks: {rank_values.nunique():,}")
        print(f"    Range: {rank_values.min():.3f} to {rank_values.max():.3f}")
    
    print(f"\n✓ Generated ranks for {len(rank_stats['ranked_metrics'])} metrics")
    
    return df_ranked, rank_stats


def step3_domain_rules(df, level_name=""):
    """Step 3: Apply domain-specific filtering (remove obvious errors)."""
    prefix = f"{level_name} - " if level_name else ""
    print(f"\n{'='*80}")
    print(f"{prefix}STEP 3: DOMAIN RULES")
    print('='*80)
    
    df_clean = df.copy()
    stats = {'removed': 0, 'nullified': 0}
    
    initial_count = len(df_clean)
    
    # Remove invalid prices (< $10k are likely data errors)
    if 'MEDIAN_SALE_PRICE' in df_clean.columns:
        before = len(df_clean)
        df_clean = df_clean[
            (df_clean['MEDIAN_SALE_PRICE'].isna()) | 
            (df_clean['MEDIAN_SALE_PRICE'] >= MIN_VALID_PRICE)
        ]
        removed_price = before - len(df_clean)
        if removed_price > 0:
            print(f"  ✗ Removed {removed_price} records with price < ${MIN_VALID_PRICE:,}")
            stats['removed'] += removed_price
    
    # Nullify suspicious DOM (> 2 years is suspicious)
    if 'MEDIAN_DOM' in df_clean.columns:
        suspicious = (df_clean['MEDIAN_DOM'] > MAX_VALID_DOM).sum()
        if suspicious > 0:
            df_clean.loc[df_clean['MEDIAN_DOM'] > MAX_VALID_DOM, 'MEDIAN_DOM'] = None
            if 'MEDIAN_DOM_LOG' in df_clean.columns:
                df_clean.loc[df_clean['MEDIAN_DOM'].isna(), 'MEDIAN_DOM_LOG'] = None
            if 'MEDIAN_DOM_RANK' in df_clean.columns:
                df_clean.loc[df_clean['MEDIAN_DOM'].isna(), 'MEDIAN_DOM_RANK'] = None
            print(f"  ⚠ Nullified {suspicious} DOM values > {MAX_VALID_DOM} days")
            stats['nullified'] = suspicious
    
    print(f"\n✓ Filtered from {initial_count:,} to {len(df_clean):,} records")
    
    return df_clean, stats


# ============================================================================
# ZIP CODE LEVEL PROCESSING
# ============================================================================

def process_zip_level():
    """Process ZIP code level data from CSV."""
    print("\n" + "#"*80)
    print("# PROCESSING ZIP CODE LEVEL")
    print("#"*80)

    if not ZIP_INPUT_CSV.exists():
        print(f"❌ Error: Input CSV not found at: {ZIP_INPUT_CSV}")
        return False

    # 1. Load CSV
    print(f"\n1️⃣  Reading CSV...")
    load_cols = list(set(ZIP_KEEP_COLUMNS + ['PERIOD_END', 'ZIP']))
    df = pd.read_csv(ZIP_INPUT_CSV, usecols=lambda c: c in load_cols, low_memory=False)
    print(f"    ✓ Loaded {len(df):,} records")

    # 2. Process Dates
    print(f"\n2️⃣  Processing dates...")
    df['dt'] = pd.to_datetime(df['PERIOD_END'])
    max_date = df['dt'].max()
    cutoff_date = max_date - timedelta(days=30 * ZIP_MONTHS_CUTOFF)
    print(f"    Latest Date: {max_date.date()}")
    print(f"    Cutoff Date: {cutoff_date.date()}")

    # 3. Get Latest per ZIP
    print(f"\n3️⃣  Filtering latest records per ZIP...")
    latest_df = df.sort_values('dt').groupby('ZIP').tail(1)
    total_zips = len(latest_df)
    
    # Check for duplicates
    print(f"\n   🔍 Checking for duplicate ZIPs...")
    duplicate_zips = latest_df['ZIP'].duplicated().sum()
    if duplicate_zips > 0:
        print(f"   ⚠️  WARNING: Found {duplicate_zips} duplicate ZIP codes!")
        print(f"   Keeping only the first occurrence of each ZIP...")
        latest_df = latest_df.drop_duplicates(subset=['ZIP'], keep='first')
        print(f"   ✓ Deduplicated: {len(latest_df)} unique ZIPs")
    else:
        print(f"   ✓ No duplicate ZIPs found")

    # 4. Remove Stale Data
    print(f"\n4️⃣  Removing stale data...")
    latest_df = latest_df[latest_df['dt'] >= cutoff_date]
    active_zips = len(latest_df)
    dropped = total_zips - active_zips
    print(f"    ✓ Total ZIPs: {total_zips:,}")
    print(f"    ✓ Active ZIPs: {active_zips:,}")
    print(f"    🗑️  Dropped {dropped:,} stale ZIPs")

    # Use ZIP-specific metrics for transformation
    zip_log_metrics = [m for m in LOG_TRANSFORM_METRICS if m in latest_df.columns]
    zip_rank_metrics = [m for m in RANK_METRICS if m in latest_df.columns]

    # 5. STEP 1: Log Transformation
    latest_df, transform_stats = step1_log_transform(latest_df, zip_log_metrics, "ZIP")
    
    # 6. STEP 2: Percentile Ranks
    latest_df, rank_stats = step2_percentile_ranks(latest_df, zip_rank_metrics, "ZIP")
    
    # 7. STEP 3: Domain Rules
    latest_df, domain_stats = step3_domain_rules(latest_df, "ZIP")

    # 8. Sort and clean
    print(f"\n8️⃣  Finalizing data...")
    latest_df = latest_df.sort_values('dt', ascending=False)
    latest_df = latest_df.drop('dt', axis=1)
    latest_df = latest_df.replace({pd.NA: None, float('nan'): None, np.inf: None, -np.inf: None})
    latest_df = latest_df.where(pd.notnull(latest_df), None)

    # 9. Save JSON
    print(f"\n9️⃣  Saving to {ZIP_OUTPUT_JSON.name}...")
    
    output = {
        "metadata": {
            "source": "Redfin Market Tracker",
            "geography_level": "ZIP Code",
            "processing_method": "Log Transformation + Percentile Ranks (No Capping)",
            "generated_at": datetime.now().isoformat(),
            "record_count": int(len(latest_df)),
            "latest_data_point": str(max_date.date()),
            "processing_steps": {
                "1_log_transformation": {
                    "method": "natural_log",
                    "formula": "log(x + 1)",
                    "transformed_metrics": transform_stats['transformed_metrics'],
                    "skewness_improvements": transform_stats['skewness']
                },
                "2_percentile_ranks": {
                    "method": "rank(pct=True)",
                    "description": "Percentile ranks from 0.0 (lowest) to 1.0 (highest)",
                    "ranked_metrics": rank_stats['ranked_metrics'],
                    "distribution": rank_stats['distribution']
                },
                "3_domain_rules": {
                    "min_valid_price": int(MIN_VALID_PRICE),
                    "max_valid_dom": int(MAX_VALID_DOM),
                    "records_removed": int(domain_stats['removed']),
                    "values_nullified": int(domain_stats['nullified'])
                }
            },
            "usage_instructions": {
                "for_visualization": "Use *_RANK columns for perfectly smooth color gradients",
                "for_display": "Use original columns in tooltips/labels (real values)",
                "alternative_viz": "Use *_LOG columns for log-scale visualization",
                "example": "Map color from MEDIAN_SALE_PRICE_RANK (0.0-1.0), show $value from MEDIAN_SALE_PRICE"
            }
        },
        "data": latest_df.to_dict('records')
    }

    ZIP_OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(ZIP_OUTPUT_JSON, 'w') as f:
        json.dump(output, f, indent=2)

    size_mb = ZIP_OUTPUT_JSON.stat().st_size / 1024 / 1024
    
    print(f"\n✅ ZIP CODE LEVEL PROCESSING COMPLETE!")
    print(f"    File: {ZIP_OUTPUT_JSON.name}")
    print(f"    Size: {size_mb:.2f} MB")
    print(f"    Records: {len(latest_df):,}")
    
    return True


# ============================================================================
# GEOGRAPHIC AGGREGATION LEVEL PROCESSING
# ============================================================================

def load_redfin_json(input_path, geo_level):
    """Load Redfin JSON data for a specific geographic level."""
    print(f"\n{'='*80}")
    print(f"LOADING {geo_level.upper()} DATA")
    print('='*80)
    
    if not input_path.exists():
        print(f"❌ ERROR: File not found at {input_path}")
        return None
    
    with open(input_path, 'r') as f:
        data = json.load(f)
    
    # Handle both formats: {"data": [...]} or just [...]
    if isinstance(data, dict) and 'data' in data:
        df = pd.DataFrame(data['data'])
        metadata = data.get('metadata', {})
    else:
        df = pd.DataFrame(data)
        metadata = {}
    
    print(f"✓ Loaded {len(df):,} records")
    print(f"✓ Columns: {len(df.columns)} total")
    
    # Check for period information
    period_col = None
    for col in ['period_end', 'PERIOD_END', 'period', 'date']:
        if col in df.columns:
            period_col = col
            break
    
    if period_col:
        print(f"✓ Latest period: {df[period_col].max()}")
    
    return df, metadata


def standardize_column_names(df):
    """Standardize column names to uppercase with underscores."""
    df.columns = df.columns.str.upper()
    return df


def convert_numeric_columns(df):
    """Convert metric columns to numeric type (they may be loaded as strings from JSON)."""
    print(f"\n{'='*80}")
    print("CONVERTING NUMERIC COLUMNS")
    print('='*80)
    
    # All potential numeric columns to convert
    numeric_columns = [
        'MEDIAN_SALE_PRICE', 'MEDIAN_SALE_PRICE_MOM', 'MEDIAN_SALE_PRICE_YOY',
        'MEDIAN_LIST_PRICE', 'MEDIAN_LIST_PRICE_MOM', 'MEDIAN_LIST_PRICE_YOY',
        'MEDIAN_PPSF', 'MEDIAN_PPSF_MOM', 'MEDIAN_PPSF_YOY',
        'MEDIAN_LIST_PPSF', 'MEDIAN_LIST_PPSF_MOM', 'MEDIAN_LIST_PPSF_YOY',
        'HOMES_SOLD', 'HOMES_SOLD_MOM', 'HOMES_SOLD_YOY',
        'PENDING_SALES', 'PENDING_SALES_MOM', 'PENDING_SALES_YOY',
        'NEW_LISTINGS', 'NEW_LISTINGS_MOM', 'NEW_LISTINGS_YOY',
        'INVENTORY', 'INVENTORY_MOM', 'INVENTORY_YOY',
        'MEDIAN_DOM', 'MEDIAN_DOM_MOM', 'MEDIAN_DOM_YOY',
        'AVG_SALE_TO_LIST', 'AVG_SALE_TO_LIST_MOM', 'AVG_SALE_TO_LIST_YOY',
        'SOLD_ABOVE_LIST', 'SOLD_ABOVE_LIST_MOM', 'SOLD_ABOVE_LIST_YOY',
        'OFF_MARKET_IN_TWO_WEEKS', 'OFF_MARKET_IN_TWO_WEEKS_MOM', 'OFF_MARKET_IN_TWO_WEEKS_YOY'
    ]
    
    converted_count = 0
    for col in numeric_columns:
        if col in df.columns:
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            converted_count += 1
    
    print(f"✓ Converted {converted_count} columns to numeric type")
    
    return df


def filter_latest_period(df, geo_level):
    """Filter data to only include the latest period for each geographic entity."""
    print(f"\n{'='*80}")
    print(f"FILTERING LATEST PERIOD - {geo_level.upper()}")
    print('='*80)
    
    initial_count = len(df)
    
    # Find the period column
    period_col = None
    for col in ['PERIOD_END', 'PERIOD_BEGIN', 'DATE']:
        if col in df.columns:
            period_col = col
            break
    
    if not period_col:
        print("⚠ No period column found - keeping all records")
        return df
    
    # Convert period to datetime
    df['_PERIOD_DT'] = pd.to_datetime(df[period_col])
    
    # Find the latest period
    latest_period = df['_PERIOD_DT'].max()
    
    # Filter to latest period only
    df_latest = df[df['_PERIOD_DT'] == latest_period].copy()
    df_latest = df_latest.drop('_PERIOD_DT', axis=1)
    
    removed = initial_count - len(df_latest)
    
    print(f"  Latest period: {latest_period.date()}")
    print(f"  Records before: {initial_count:,}")
    print(f"  Records after: {len(df_latest):,}")
    print(f"  🗑️  Removed {removed:,} historical records")
    
    return df_latest


def drop_unnecessary_columns(df):
    """Remove single-value, null, and redundant columns."""
    print(f"\n{'='*80}")
    print("DROPPING UNNECESSARY COLUMNS")
    print('='*80)
    
    initial_cols = len(df.columns)
    cols_dropped = []
    
    for col in COLS_TO_DROP:
        if col in df.columns:
            df = df.drop(columns=[col])
            cols_dropped.append(col)
    
    if cols_dropped:
        print(f"✓ Dropped {len(cols_dropped)} columns:")
        for col in cols_dropped:
            print(f"  - {col}")
    else:
        print("✓ No columns to drop")
    
    print(f"✓ Columns: {initial_cols} → {len(df.columns)}")
    
    return df


def save_json(df, output_path, geo_level, metadata, transform_stats, rank_stats, domain_stats):
    """Save processed data as JSON with metadata."""
    print(f"\n{'='*80}")
    print(f"SAVING {geo_level.upper()} DATA")
    print('='*80)
    
    # Clean data for JSON serialization
    df_clean = df.replace({pd.NA: None, float('nan'): None, np.inf: None, -np.inf: None})
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    # Create output structure
    output = {
        "metadata": {
            "source": "Redfin Market Tracker",
            "geography_level": geo_level,
            "processing_method": "Log Transformation + Percentile Ranks (No Capping)",
            "generated_at": datetime.now().isoformat(),
            "record_count": int(len(df_clean)),
            "original_metadata": metadata,
            "processing_steps": {
                "1_log_transformation": {
                    "method": "natural_log",
                    "formula": "log(x + 1)",
                    "transformed_metrics": transform_stats['transformed_metrics'],
                    "skewness_improvements": transform_stats['skewness']
                },
                "2_percentile_ranks": {
                    "method": "rank(pct=True)",
                    "description": "Percentile ranks from 0.0 (lowest) to 1.0 (highest)",
                    "ranked_metrics": rank_stats['ranked_metrics'],
                    "distribution": rank_stats['distribution']
                },
                "3_domain_rules": {
                    "min_valid_price": int(MIN_VALID_PRICE),
                    "max_valid_dom": int(MAX_VALID_DOM),
                    "records_removed": int(domain_stats['removed']),
                    "values_nullified": int(domain_stats['nullified'])
                }
            },
            "usage_instructions": {
                "for_visualization": "Use *_RANK columns for perfectly smooth color gradients",
                "for_display": "Use original columns in tooltips/labels (real values)",
                "alternative_viz": "Use *_LOG columns for log-scale visualization",
                "example": f"Map color from MEDIAN_SALE_PRICE_RANK (0.0-1.0), show $value from MEDIAN_SALE_PRICE"
            }
        },
        "data": df_clean.to_dict('records')
    }
    
    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    
    file_size = output_path.stat().st_size / 1024
    print(f"✓ Saved to: {output_path}")
    print(f"✓ File size: {file_size:.1f} KB")
    print(f"✓ Records: {len(df_clean):,}")


def process_geographic_level(input_path, output_path, geo_level):
    """Process a single geographic level through the entire pipeline."""
    print(f"\n\n{'#'*80}")
    print(f"# PROCESSING {geo_level.upper()} LEVEL")
    print(f"{'#'*80}")
    
    # Load data
    result = load_redfin_json(input_path, geo_level)
    if result is None:
        print(f"❌ Skipping {geo_level} - file not found")
        return False
    
    df, metadata = result
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Convert numeric columns (important - JSON may load as strings)
    df = convert_numeric_columns(df)
    
    # Filter to latest period only (removes historical time series)
    df = filter_latest_period(df, geo_level)
    
    # Drop unnecessary columns
    df = drop_unnecessary_columns(df)
    
    # Filter metrics to only those present in data
    log_metrics = [m for m in LOG_TRANSFORM_METRICS if m in df.columns]
    rank_metrics = [m for m in RANK_METRICS if m in df.columns]
    
    # Step 1: Log transformation
    df, transform_stats = step1_log_transform(df, log_metrics, geo_level)
    
    # Step 2: Percentile ranks
    df, rank_stats = step2_percentile_ranks(df, rank_metrics, geo_level)
    
    # Step 3: Domain rules
    df, domain_stats = step3_domain_rules(df, geo_level)
    
    # Save
    save_json(df, output_path, geo_level, metadata, transform_stats, rank_stats, domain_stats)
    
    print(f"\n✅ {geo_level.upper()} PROCESSING COMPLETE!")
    return True


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Process Redfin data at all geographic levels',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_all_redfin_levels.py              # Process all levels
  python process_all_redfin_levels.py --zip        # Process only ZIP level
  python process_all_redfin_levels.py --state      # Process only State level
  python process_all_redfin_levels.py --county     # Process only County level
  python process_all_redfin_levels.py --cbsa       # Process only CBSA level
  python process_all_redfin_levels.py --zip --state  # Process ZIP and State levels
        """
    )
    
    parser.add_argument('--zip', action='store_true', help='Process ZIP code level')
    parser.add_argument('--state', action='store_true', help='Process State level')
    parser.add_argument('--county', action='store_true', help='Process County level')
    parser.add_argument('--cbsa', action='store_true', help='Process CBSA level')
    
    args = parser.parse_args()
    
    # If no specific level is requested, process all
    process_all = not (args.zip or args.state or args.county or args.cbsa)
    
    print("\n" + "="*80)
    print("REDFIN DATA PROCESSOR - ALL GEOGRAPHIC LEVELS")
    print("="*80)
    print("Processing: Log Transform → Percentile Ranks → Filter")
    print("✓ No data capping/clipping - all original values preserved!")
    print("="*80)
    
    success_count = 0
    total_count = 0
    
    # Process each level as requested
    levels = []
    
    if process_all or args.zip:
        levels.append(('ZIP', process_zip_level, None, None))
        total_count += 1
    
    if process_all or args.state:
        levels.append(('State', process_geographic_level, STATE_INPUT, STATE_OUTPUT))
        total_count += 1
    
    if process_all or args.county:
        levels.append(('County', process_geographic_level, COUNTY_INPUT, COUNTY_OUTPUT))
        total_count += 1
    
    if process_all or args.cbsa:
        levels.append(('CBSA', process_geographic_level, CBSA_INPUT, CBSA_OUTPUT))
        total_count += 1
    
    # Execute processing
    for level_info in levels:
        level_name = level_info[0]
        process_func = level_info[1]
        
        try:
            if level_name == 'ZIP':
                if process_func():
                    success_count += 1
            else:
                input_path = level_info[2]
                output_path = level_info[3]
                if process_func(input_path, output_path, level_name):
                    success_count += 1
        except Exception as e:
            print(f"\n❌ ERROR processing {level_name} level: {e}")
            import traceback
            traceback.print_exc()
    
    # Final summary
    print(f"\n\n{'='*80}")
    print("PROCESSING SUMMARY")
    print("="*80)
    print(f"Successfully processed: {success_count}/{total_count} geographic levels")
    
    if success_count > 0:
        print("\n📊 Output files:")
        if (process_all or args.zip) and ZIP_OUTPUT_JSON.exists():
            print(f"  ✓ {ZIP_OUTPUT_JSON}")
        if (process_all or args.state) and STATE_OUTPUT.exists():
            print(f"  ✓ {STATE_OUTPUT}")
        if (process_all or args.county) and COUNTY_OUTPUT.exists():
            print(f"  ✓ {COUNTY_OUTPUT}")
        if (process_all or args.cbsa) and CBSA_OUTPUT.exists():
            print(f"  ✓ {CBSA_OUTPUT}")
        
        print("\n💡 Each file contains:")
        print("  • Original values (for display/tooltips)")
        print("  • Log-transformed values (*_LOG columns)")
        print("  • Percentile ranks (*_RANK columns for color mapping)")
        
        print("\n✓ Best Practice for Maps:")
        print("  - Use *_RANK columns for color (smooth 0.0-1.0 gradient)")
        print("  - Display original values in tooltips (real prices)")
        print("  - No artificial spikes or capped values!")
    
    print("\n" + "="*80)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()