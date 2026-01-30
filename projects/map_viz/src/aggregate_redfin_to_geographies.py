"""
Aggregate Redfin ZIP-Level Data to State/County/CBSA Levels
============================================================
Takes the ZIP-level Redfin data and aggregates it to higher geographic levels
using weighted medians and percentile ranking for smooth gradients.

Input:  data/redfin/processed/redfin_latest_optimized.json (ZIP level)
        data/reference/uszips.csv (ZIP to County/State mapping)

Output: data/redfin/processed/redfin_state_aggregated.json
        data/redfin/processed/redfin_county_aggregated.json
        data/redfin/processed/redfin_cbsa_aggregated.json
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"

# Input files
REDFIN_ZIP_JSON = DATA_DIR / "redfin" / "processed" / "redfin_latest_optimized.json"
ZIP_REFERENCE = DATA_DIR / "reference" / "uszips.csv"

# Output files
OUTPUT_DIR = DATA_DIR / "redfin" / "processed"
STATE_OUTPUT = OUTPUT_DIR / "redfin_state_aggregated.json"
COUNTY_OUTPUT = OUTPUT_DIR / "redfin_county_aggregated.json"
CBSA_OUTPUT = OUTPUT_DIR / "redfin_cbsa_aggregated.json"

# Metrics to aggregate
REDFIN_METRICS = [
    'MEDIAN_SALE_PRICE',
    'MEDIAN_SALE_PRICE_YOY',
    'HOMES_SOLD',
    'INVENTORY',
    'MEDIAN_DOM'
]

# Metrics for log transformation
LOG_METRICS = [
    'MEDIAN_SALE_PRICE',
    'HOMES_SOLD',
    'INVENTORY',
    'MEDIAN_DOM'
]

# Metrics for percentile ranking
RANK_METRICS = [
    'MEDIAN_SALE_PRICE',
    'MEDIAN_SALE_PRICE_YOY',
    'HOMES_SOLD',
    'INVENTORY',
    'MEDIAN_DOM'
]


def load_redfin_zip_data():
    """Load Redfin ZIP-level data"""
    print("="*80)
    print("LOADING REDFIN ZIP-LEVEL DATA")
    print("="*80)

    with open(REDFIN_ZIP_JSON, 'r') as f:
        data = json.load(f)

    df = pd.DataFrame(data['data'])

    # Ensure ZIP is 5-digit string
    df['ZIP'] = df['ZIP'].astype(str).str.zfill(5)

    print(f"OK Loaded {len(df):,} ZIP code records")
    print(f"OK Period: {df['PERIOD_END'].iloc[0]}")
    print(f"OK Metrics: {', '.join(REDFIN_METRICS)}")

    return df


def load_zip_reference():
    """Load ZIP to County/State mapping"""
    print("\n" + "="*80)
    print("LOADING ZIP CODE REFERENCE DATA")
    print("="*80)

    df = pd.read_csv(ZIP_REFERENCE, dtype={'zip': str, 'county_fips': str, 'state_id': str})
    df = df.rename(columns={'zip': 'ZIP', 'state_id': 'STATE', 'county_fips': 'COUNTY_FIPS'})

    # Ensure ZIP is 5-digit string
    df['ZIP'] = df['ZIP'].str.zfill(5)

    # Ensure County FIPS is 5-digit (state + county)
    df['COUNTY_FIPS'] = df['COUNTY_FIPS'].str.zfill(5)

    # Extract State FIPS (first 2 digits of county FIPS)
    df['STATE_FIPS'] = df['COUNTY_FIPS'].str[:2]

    print(f"OK Loaded {len(df):,} ZIP codes")
    print(f"OK Unique states: {df['STATE_FIPS'].nunique()}")
    print(f"OK Unique counties: {df['COUNTY_FIPS'].nunique()}")

    return df[['ZIP', 'STATE_FIPS', 'COUNTY_FIPS', 'STATE', 'county_name', 'state_name', 'population']]


def weighted_median(values, weights):
    """Calculate weighted median"""
    if len(values) == 0:
        return None

    # Remove NaN values
    mask = ~np.isnan(values) & ~np.isnan(weights)
    values = values[mask]
    weights = weights[mask]

    if len(values) == 0:
        return None

    # Sort by values
    sorted_indices = np.argsort(values)
    sorted_values = values[sorted_indices]
    sorted_weights = weights[sorted_indices]

    # Calculate cumulative weight
    cumsum = np.cumsum(sorted_weights)
    cutoff = 0.5 * np.sum(sorted_weights)

    # Find median
    return float(sorted_values[cumsum >= cutoff][0])


def aggregate_to_geography(redfin_df, zip_ref_df, geo_column, geo_name):
    """
    Aggregate Redfin data to a specific geography level

    Parameters:
    - redfin_df: DataFrame with ZIP-level Redfin data
    - zip_ref_df: DataFrame with ZIP to geography mapping
    - geo_column: Column name for geography (e.g., 'STATE_FIPS', 'COUNTY_FIPS')
    - geo_name: Name for display (e.g., 'State', 'County')
    """
    print(f"\n{'='*80}")
    print(f"AGGREGATING TO {geo_name.upper()} LEVEL")
    print('='*80)

    # Merge Redfin data with ZIP reference
    merged = redfin_df.merge(zip_ref_df, on='ZIP', how='left')

    # Remove records without geography mapping
    merged = merged[merged[geo_column].notna()]

    print(f"OK Matched {len(merged):,} / {len(redfin_df):,} records ({len(merged)/len(redfin_df)*100:.1f}%)")

    # Use population as weight for aggregation
    merged['weight'] = merged['population'].fillna(1)

    # Group by geography
    aggregated = []

    for geo_id, group in merged.groupby(geo_column):
        record = {
            geo_column: geo_id,
            'ZIP_COUNT': len(group),
            'TOTAL_POPULATION': int(group['weight'].sum())
        }

        # Aggregate each metric
        for metric in REDFIN_METRICS:
            if metric in group.columns:
                values = group[metric].values
                weights = group['weight'].values

                # Calculate weighted median
                result = weighted_median(values, weights)

                if result is not None:
                    record[metric] = result

        # Add metadata
        if 'PERIOD_END' in group.columns:
            record['PERIOD_END'] = group['PERIOD_END'].iloc[0]

        if 'state_name' in group.columns:
            record['STATE_NAME'] = group['state_name'].iloc[0]

        if 'county_name' in group.columns and geo_name == 'County':
            record['COUNTY_NAME'] = group['county_name'].iloc[0]

        aggregated.append(record)

    result_df = pd.DataFrame(aggregated)

    print(f"OK Aggregated to {len(result_df):,} {geo_name.lower()}s")
    print(f"OK Avg ZIPs per {geo_name.lower()}: {result_df['ZIP_COUNT'].mean():.1f}")

    # Show sample statistics
    for metric in REDFIN_METRICS:
        if metric in result_df.columns:
            count = result_df[metric].notna().sum()
            print(f"  - {metric}: {count} {geo_name.lower()}s with data")

    return result_df


def apply_log_transformation(df):
    """Apply log transformation to skewed metrics"""
    print("\n" + "="*80)
    print("APPLYING LOG TRANSFORMATION")
    print("="*80)

    for metric in LOG_METRICS:
        if metric in df.columns:
            log_col = f"{metric}_LOG"
            df[log_col] = np.log1p(df[metric].fillna(0))

            # Stats
            before_skew = df[metric].skew()
            after_skew = df[log_col].skew()
            print(f"  {metric}:")
            print(f"    Skewness: {before_skew:.2f} -> {after_skew:.2f} (improved {before_skew - after_skew:.2f})")

    print("OK Log transformation complete")
    return df


def apply_percentile_ranking(df):
    """Apply percentile ranking for smooth gradients (0.0 to 1.0)"""
    print("\n" + "="*80)
    print("APPLYING PERCENTILE RANKING")
    print("="*80)

    for metric in RANK_METRICS:
        if metric in df.columns:
            rank_col = f"{metric}_RANK"

            # Calculate percentile rank (0.0 to 1.0)
            df[rank_col] = df[metric].rank(pct=True)

            # Stats
            min_rank = df[rank_col].min()
            max_rank = df[rank_col].max()
            median_rank = df[rank_col].median()

            print(f"  {metric}_RANK:")
            print(f"    Range: {min_rank:.4f} to {max_rank:.4f}")
            print(f"    Median: {median_rank:.4f}")

    print("OK Percentile ranking complete")
    return df


def save_json(df, output_path, geo_name):
    """Save aggregated data as JSON"""
    print(f"\n{'='*80}")
    print(f"SAVING {geo_name.upper()} DATA")
    print('='*80)

    # Convert DataFrame to dict
    data = df.to_dict('records')

    # Create output structure
    output = {
        'metadata': {
            'source': 'Redfin Market Tracker (aggregated from ZIP codes)',
            'geography_level': geo_name,
            'generated_at': datetime.now().isoformat(),
            'record_count': len(data),
            'latest_period': df['PERIOD_END'].iloc[0] if 'PERIOD_END' in df.columns else None,
            'aggregation_method': 'Population-weighted median',
            'log_transformed_metrics': LOG_METRICS,
            'percentile_ranked_metrics': RANK_METRICS,
            'usage_instructions': {
                'for_visualization': 'Use *_RANK columns for smooth color gradients (0.0-1.0)',
                'for_display': 'Use original columns in tooltips/labels (real values)',
                'alternative_viz': 'Use *_LOG columns for log-scale visualization'
            }
        },
        'data': data
    }

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)

    file_size = output_path.stat().st_size / 1024
    print(f"OK Saved to: {output_path}")
    print(f"OK File size: {file_size:.1f} KB")
    print(f"OK Records: {len(data)}")


def main():
    print("\n" + "="*80)
    print("REDFIN DATA AGGREGATION PIPELINE")
    print("ZIP -> State / County")
    print("="*80 + "\n")

    try:
        # Load data
        redfin_df = load_redfin_zip_data()
        zip_ref_df = load_zip_reference()

        # 1. AGGREGATE TO STATE LEVEL
        state_df = aggregate_to_geography(redfin_df, zip_ref_df, 'STATE_FIPS', 'State')
        state_df = apply_log_transformation(state_df)
        state_df = apply_percentile_ranking(state_df)
        save_json(state_df, STATE_OUTPUT, 'State')

        # 2. AGGREGATE TO COUNTY LEVEL
        county_df = aggregate_to_geography(redfin_df, zip_ref_df, 'COUNTY_FIPS', 'County')
        county_df = apply_log_transformation(county_df)
        county_df = apply_percentile_ranking(county_df)
        save_json(county_df, COUNTY_OUTPUT, 'County')

        print("\n" + "="*80)
        print("AGGREGATION COMPLETE!")
        print("="*80)
        print("\nGenerated files:")
        print(f"  1. {STATE_OUTPUT.name}")
        print(f"  2. {COUNTY_OUTPUT.name}")
        print("\nNext steps:")
        print("  - Run fetch_state_data.py to merge with Census data")
        print("  - Run fetch_county_data.py to merge with Census data")
        print("  - Update JavaScript to visualize Redfin metrics at all levels")

    except Exception as e:
        print(f"\nERROR: ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
