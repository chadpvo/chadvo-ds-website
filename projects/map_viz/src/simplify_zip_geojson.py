"""
Simplify ZIP code GeoJSON for Mapbox upload
Reduces file size while maintaining visual quality
"""

import json
import geopandas as gpd
from pathlib import Path

def simplify_zip_geojson(
    input_path=None,
    output_path=None,
    tolerance=0.001  # Degrees - adjust for more/less simplification
):
    """
    Simplify ZIP code boundaries for Mapbox upload.
    
    Parameters:
    -----------
    tolerance : float
        Simplification tolerance in degrees
        0.001 = ~100m accuracy (good balance)
        0.005 = ~500m accuracy (more aggressive)
    """
    
    print("="*80)
    print("SIMPLIFYING ZIP CODE GEOJSON FOR MAPBOX")
    print("="*80)
    
    # Auto-detect paths relative to script location
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    if input_path is None:
        input_file = project_root / "assets" / "us-zips.geojson"
    else:
        input_file = Path(input_path)
    
    if output_path is None:
        output_file = project_root / "assets" / "us-zips-simplified.geojson"
    else:
        output_file = Path(output_path)
    
    if not input_file.exists():
        print(f"\n❌ Input file not found: {input_file}")
        return
    
    # Load GeoJSON
    print(f"\n1️⃣ Loading GeoJSON...")
    print(f"   Source: {input_file}")
    gdf = gpd.read_file(input_file)
    
    original_size = input_file.stat().st_size / 1024 / 1024
    print(f"   ✓ Loaded {len(gdf):,} ZIP codes")
    print(f"   ✓ Original size: {original_size:.1f} MB")
    
    # Simplify geometries
    print(f"\n2️⃣ Simplifying geometries...")
    print(f"   Tolerance: {tolerance} degrees (~{tolerance*111:.0f}m)")
    gdf['geometry'] = gdf['geometry'].simplify(tolerance, preserve_topology=True)
    print(f"   ✓ Simplification complete")
    
    # Keep only essential properties
    print(f"\n3️⃣ Cleaning properties...")
    essential_props = ['ZIP', 'GEOID', 'AREA_LAND', 'AREA_WATER']
    available_props = [col for col in essential_props if col in gdf.columns]
    
    # Add geometry column back
    gdf_clean = gdf[available_props + ['geometry']].copy()
    print(f"   ✓ Kept {len(available_props)} essential properties")
    
    # Save simplified GeoJSON
    print(f"\n4️⃣ Saving simplified GeoJSON...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    gdf_clean.to_file(output_file, driver='GeoJSON')
    
    new_size = output_file.stat().st_size / 1024 / 1024
    reduction = ((original_size - new_size) / original_size) * 100
    
    print(f"   ✓ Saved to: {output_file}")
    print(f"   ✓ New size: {new_size:.1f} MB")
    print(f"   ✓ Reduction: {reduction:.1f}%")
    
    # Check if under Mapbox free tier limit
    print(f"\n5️⃣ Mapbox Upload Status:")
    if new_size <= 50:
        print(f"   ✅ File is {new_size:.1f} MB - Ready for Mapbox free tier!")
    elif new_size <= 300:
        print(f"   ⚠️  File is {new_size:.1f} MB - Requires Mapbox Pro plan")
    else:
        print(f"   ❌ File is {new_size:.1f} MB - Still too large")
        print(f"   💡 Try increasing tolerance to 0.005 or 0.01")
    
    print("\n" + "="*80)
    print("✅ SIMPLIFICATION COMPLETE!")
    print("="*80)
    
    print(f"\n📋 Next Steps:")
    print(f"1. Go to: https://studio.mapbox.com/")
    print(f"2. Click 'Tilesets' → 'New tileset'")
    print(f"3. Upload: {output_file.name}")
    print(f"4. Copy the Tileset ID (looks like: username.abc123xyz)")
    print(f"5. Use that ID in your map code")
    
    return output_file


def create_multiple_versions():
    """Create multiple simplified versions to test"""
    
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    tolerances = {
        'high_quality': 0.0005,   # ~50m - very detailed
        'balanced': 0.001,         # ~100m - good balance
        'aggressive': 0.005        # ~500m - smaller file
    }
    
    for name, tolerance in tolerances.items():
        output = project_root / "assets" / f"us-zips-{name}.geojson"
        print(f"\n{'='*80}")
        print(f"Creating {name.upper()} version (tolerance: {tolerance})")
        print(f"{'='*80}")
        simplify_zip_geojson(
            output_path=str(output),
            tolerance=tolerance
        )


if __name__ == "__main__":
    print("\n🗺️  ZIP Code GeoJSON Simplifier for Mapbox\n")
    
    # Check dependencies
    try:
        import geopandas
        print("✓ Dependencies installed\n")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("\nInstall with: pip install geopandas")
        exit(1)
    
    # Try very aggressive simplification to get under 50MB
    print("\nTrying multiple tolerance levels...\n")
    
    for tolerance in [0.005, 0.01, 0.015]:
        print(f"\n{'='*80}")
        print(f"Testing tolerance: {tolerance}")
        print(f"{'='*80}")
        simplify_zip_geojson(
            output_path=f"../assets/us-zips-t{tolerance}.geojson",
            tolerance=tolerance
        )
    
    # Option 2: Create multiple versions to compare
    # Uncomment to create 3 versions:
    # create_multiple_versions()