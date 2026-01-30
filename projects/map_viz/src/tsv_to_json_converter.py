import pandas as pd
import json
from pathlib import Path
from datetime import datetime

def convert_tsv_to_json(
    input_path="projects/map_viz/data/redfin/redfin_zip_all_states.tsv",
    output_path="projects/map_viz/data/redfin/redfin_zip_all_states.json",
    filter_latest_only=True,
    max_records=None
):
    """
    Convert Redfin TSV file to JSON format for Mapbox visualization.
    
    Parameters:
    -----------
    input_path : str
        Path to input TSV file
    output_path : str
        Path to output JSON file
    filter_latest_only : bool
        If True, only include the most recent period for each ZIP code
    max_records : int, optional
        Maximum number of records to include (for testing)
    """
    
    print(f"Reading TSV file from: {input_path}")
    
    # Read the TSV file
    df = pd.read_csv(input_path, sep='\t', low_memory=False)
    
    print(f"Total records loaded: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Convert PERIOD_END to datetime for filtering
    df['PERIOD_END'] = pd.to_datetime(df['PERIOD_END'])
    
    # Optional: Filter to only latest period per ZIP code
    if filter_latest_only:
        print("Filtering to latest period for each ZIP code...")
        df = df.sort_values('PERIOD_END').groupby('REGION').tail(1)
        print(f"Records after filtering: {len(df)}")
    
    # Optional: Limit records for testing
    if max_records:
        print(f"Limiting to {max_records} records for testing...")
        df = df.head(max_records)
    
    # Convert dates back to string format
    df['PERIOD_BEGIN'] = df['PERIOD_BEGIN'].astype(str)
    df['PERIOD_END'] = df['PERIOD_END'].astype(str)
    
    # Replace NaN/NA values with null for proper JSON
    df = df.replace({pd.NA: None, pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    
    # Convert to list of dictionaries
    data_records = df.to_dict('records')
    
    # Create the JSON structure
    json_output = {
        "metadata": {
            "source": "Redfin Market Tracker",
            "url": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz",
            "conversion_date": datetime.now().isoformat(),
            "total_records": len(data_records),
            "filter_latest_only": filter_latest_only,
            "columns": list(df.columns)
        },
        "data": data_records
    }
    
    # Create output directory if it doesn't exist
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to JSON file
    print(f"Writing JSON to: {output_path}")
    with open(output_path, 'w') as f:
        json.dump(json_output, f, indent=2)
    
    print(f"✓ Conversion complete!")
    print(f"  - Output file: {output_path}")
    print(f"  - Total records: {len(data_records)}")
    print(f"  - File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Print sample record
    if data_records:
        print("\nSample record:")
        print(json.dumps(data_records[0], indent=2))
    
    return json_output


def create_geojson_with_zipcodes(
    input_path="projects/map_viz/data/redfin/redfin_zip_all_states.tsv",
    output_path="projects/map_viz/data/redfin/redfin_zip_all_states.geojson",
    zipcode_lookup_file=None,
    filter_latest_only=True
):
    """
    Convert to GeoJSON format (ideal for Mapbox).
    Note: Requires a ZIP code to lat/lon lookup file.
    
    If you don't have coordinates, this will create a placeholder GeoJSON
    that you'll need to enhance with actual coordinates.
    """
    
    print(f"Reading TSV file from: {input_path}")
    df = pd.read_csv(input_path, sep='\t', low_memory=False)
    
    # Convert and filter
    df['PERIOD_END'] = pd.to_datetime(df['PERIOD_END'])
    
    if filter_latest_only:
        df = df.sort_values('PERIOD_END').groupby('REGION').tail(1)
    
    df['PERIOD_BEGIN'] = df['PERIOD_BEGIN'].astype(str)
    df['PERIOD_END'] = df['PERIOD_END'].astype(str)
    df = df.replace({pd.NA: None, pd.NaT: None})
    df = df.where(pd.notnull(df), None)
    
    # Extract ZIP code from REGION field (e.g., "Zip Code: 77622" -> "77622")
    df['ZIP_CODE'] = df['REGION'].str.extract(r'(\d{5})')
    
    # Create GeoJSON features
    features = []
    for _, row in df.iterrows():
        # Create a feature for each record
        # Note: You'll need actual coordinates - this is a placeholder
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [0, 0]  # PLACEHOLDER - needs real lon/lat
            },
            "properties": row.to_dict()
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "note": "Coordinates are placeholders. You need to add real ZIP code coordinates.",
            "conversion_date": datetime.now().isoformat()
        }
    }
    
    # Write GeoJSON
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    print(f"✓ GeoJSON created (with placeholder coordinates)")
    print(f"  - You need to add real coordinates for mapping")
    
    return geojson


if __name__ == "__main__":
    # Get the script's directory and build paths relative to it
    script_dir = Path(__file__).parent
    project_root = script_dir.parent  # Go up one level from src/ to map_viz/
    
    input_file = project_root / "data" / "redfin" / "redfin_zip_all_states.tsv"
    output_file = project_root / "data" / "redfin" / "redfin_zip_latest.json"
    
    print(f"Script directory: {script_dir}")
    print(f"Looking for input file at: {input_file}")
    print(f"File exists: {input_file.exists()}")
    
    if not input_file.exists():
        print("\n❌ ERROR: TSV file not found!")
        print(f"Expected location: {input_file}")
        print("\nPlease check:")
        print("1. Is the file name correct?")
        print("2. Is the file in the correct directory?")
        print(f"3. Current working directory: {Path.cwd()}")
    else:
        # Example 1: Convert full dataset to JSON (latest period only)
        convert_tsv_to_json(
            input_path=str(input_file),
            output_path=str(output_file),
            filter_latest_only=True
        )
    
    # Example 2: Convert with all historical data (may be very large)
    # convert_tsv_to_json(
    #     input_path=str(input_file),
    #     output_path=str(project_root / "data" / "redfin" / "redfin_zip_all.json"),
    #     filter_latest_only=False
    # )
    
    # Example 3: Create a small test file
    # convert_tsv_to_json(
    #     input_path=str(input_file),
    #     output_path=str(project_root / "data" / "redfin" / "redfin_test.json"),
    #     filter_latest_only=True,
    #     max_records=100
    # )