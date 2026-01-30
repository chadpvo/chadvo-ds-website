"""
Download All Market Data - Redfin + Zillow
Downloads, filters for CA, saves as both TSV and JSON
"""

import requests
import gzip
import io
import csv
import json
from pathlib import Path
from datetime import datetime

def download_redfin_full():
    """Download full Redfin data for all states, save as TSV and JSON"""
    print("="*70)
    print("REDFIN - DOWNLOADING FULL DATASET (ALL STATES)")
    print("="*70)
    
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data' / 'redfin'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'
    
    print(f"\n📥 Downloading Redfin ZIP-level data...")
    
    try:
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        print(f"✅ Downloaded! Size: {len(response.content) / 1024 / 1024:.1f} MB") 
        # Decompress
        print(f"📦 Decompressing...")
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            content = f.read().decode('utf-8')
        lines = content.strip().split('\n')
        reader = csv.reader([lines[0]], delimiter='\t', quotechar='"')
        headers = next(reader)
        
        print(f"✅ Total records: {len(lines) - 1:,}")
        print(f"   Columns: {len(headers)}")
        
        # Save full TSV
        output_tsv = data_dir / 'redfin_zip_all_states.tsv'
        print(f"\n💾 Saving full TSV...")
        with open(output_tsv, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Saved: {output_tsv}")
        print(f"   Size: {output_tsv.stat().st_size / 1024 / 1024:.1f} MB")
        
        # Convert to JSON (sample for performance)
        print(f"\n🔄 Converting to JSON (sampling for performance)...")
        json_data = []
        print(f"   Strategy: Include first 10k records + all CA ZIP records")
        for i, line in enumerate(lines[1:]):
            if i >= 10000:
                # After first 10k, only include CA ZIPs (starting with 9)
                reader = csv.reader([line], delimiter='\t', quotechar='"')
                try:
                    row = next(reader)
                    if len(row) > 7:  # REGION column
                        region = row[7]
                        if not region.startswith('9'):
                            continue
                except:
                    continue
            reader = csv.reader([line], delimiter='\t', quotechar='"')
            try:
                row = next(reader)
                record = dict(zip(headers, row))
                json_data.append(record)
            except:
                continue
            if (i + 1) % 100000 == 0:
                print(f"   Processed {i + 1:,} records ({len(json_data):,} included)...")
        output_json = data_dir / 'redfin_zip_sample.json'
        print(f"\n💾 Saving JSON (sampled)...")
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'source': 'Redfin Market Tracker',
                    'url': url,
                    'download_date': datetime.now().isoformat(),
                    'total_records_in_tsv': len(lines) - 1,
                    'records_in_json': len(json_data),
                    'sampling_strategy': 'First 10k + all CA ZIPs',
                    'columns': headers
                },
                'data': json_data
            }, f, indent=2)
        print(f"✅ Saved: {output_json}")
        print(f"   Records: {len(json_data):,}")
        print(f"   Size: {output_json.stat().st_size / 1024 / 1024:.1f} MB")
        return True
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def download_zillow_data():
    """Download all working Zillow datasets"""
    print("\n" + "="*70)
    print("ZILLOW - DOWNLOADING ALL DATASETS")
    print("="*70)
    
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data' / 'zillow'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    datasets = {
        'ZHVI_Metro': {
            'name': 'Home Value Index (Metro)',
            'url': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
            'filename': 'zillow_zhvi_metro'
        },
        'ZHVI_ZIP': {
            'name': 'Home Value Index (ZIP)',
            'url': 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
            'filename': 'zillow_zhvi_zip'
        },
        'ZORI_Metro': {
            'name': 'Observed Rent Index (Metro)',
            'url': 'https://files.zillowstatic.com/research/public_csvs/zori/Metro_zori_uc_sfr_sm_sa_month.csv',
            'filename': 'zillow_zori_metro'
        },
        'ZORDI_Metro': {
            'name': 'Rent Days on Market (Metro)',
            'url': 'https://files.zillowstatic.com/research/public_csvs/zordi/Metro_zordi_uc_sfr_month.csv',
            'filename': 'zillow_zordi_metro'
        }
    }
    
    results = {}
    
    for key, dataset in datasets.items():
        print(f"\n📥 Downloading: {dataset['name']}")
        print(f"   URL: {dataset['url']}")
        
        try:
            response = requests.get(dataset['url'], timeout=60)
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                
                # Save CSV
                output_csv = data_dir / f"{dataset['filename']}.csv"
                with open(output_csv, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                print(f"✅ Saved CSV: {output_csv}")
                print(f"   Records: {len(lines) - 1:,}")
                print(f"   Size: {output_csv.stat().st_size / 1024:.1f} KB")
                
                # Convert to JSON
                reader = csv.reader(lines)
                headers = next(reader)
                
                json_data = []
                for row in reader:
                    json_data.append(dict(zip(headers, row)))
                
                output_json = data_dir / f"{dataset['filename']}.json"
                with open(output_json, 'w', encoding='utf-8') as f:
                    json.dump({
                        'metadata': {
                            'source': 'Zillow Research',
                            'dataset': dataset['name'],
                            'url': dataset['url'],
                            'download_date': datetime.now().isoformat(),
                            'total_records': len(json_data),
                            'columns': headers
                        },
                        'data': json_data
                    }, f, indent=2)
                
                print(f"✅ Saved JSON: {output_json}")
                print(f"   Size: {output_json.stat().st_size / 1024:.1f} KB")
                
                results[key] = 'success'
            else:
                print(f"❌ Failed: Status {response.status_code}")
                results[key] = 'failed'
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            results[key] = 'error'
    
    return results

def create_master_inventory():
    """Create a master inventory file listing all downloaded datasets"""
    print("\n" + "="*70)
    print("CREATING MASTER INVENTORY")
    print("="*70)
    
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    
    inventory = {
        'created': datetime.now().isoformat(),
        'datasets': {
            'redfin': [],
            'zillow': []
        }
    }
    
    # Scan Redfin files
    redfin_dir = data_dir / 'redfin'
    if redfin_dir.exists():
        for file in redfin_dir.glob('*'):
            if file.is_file():
                inventory['datasets']['redfin'].append({
                    'filename': file.name,
                    'path': str(file.relative_to(data_dir)),
                    'size_mb': file.stat().st_size / 1024 / 1024,
                    'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
    
    # Scan Zillow files
    zillow_dir = data_dir / 'zillow'
    if zillow_dir.exists():
        for file in zillow_dir.glob('*'):
            if file.is_file():
                inventory['datasets']['zillow'].append({
                    'filename': file.name,
                    'path': str(file.relative_to(data_dir)),
                    'size_mb': file.stat().st_size / 1024 / 1024,
                    'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
    
    # Save inventory
    inventory_file = data_dir / 'market_data_inventory.json'
    with open(inventory_file, 'w', encoding='utf-8') as f:
        json.dump(inventory, f, indent=2)
    
    print(f"\n✅ Inventory created: {inventory_file}")
    print(f"\n📊 Summary:")
    print(f"   Redfin files: {len(inventory['datasets']['redfin'])}")
    print(f"   Zillow files: {len(inventory['datasets']['zillow'])}")
    
    total_size = sum(f['size_mb'] for f in inventory['datasets']['redfin'] + inventory['datasets']['zillow'])
    print(f"   Total size: {total_size:.1f} MB")
    
    return inventory

def main():
    print("╔═══════════════════════════════════════════════════════════════════╗")
    print("║     DOWNLOAD ALL MARKET DATA (REDFIN + ZILLOW)                   ║")
    print("║     Save as TSV & JSON in data/ directory                        ║")
    print("╚═══════════════════════════════════════════════════════════════════╝")
    
    # Download Redfin
    redfin_success = download_redfin_full()
    
    # Download Zillow
    zillow_results = download_zillow_data()
    
    # Create inventory
    inventory = create_master_inventory()
    
    # Final summary
    print("\n" + "="*70)
    print("DOWNLOAD COMPLETE - SUMMARY")
    print("="*70)
    
    print(f"\n📦 Redfin: {'✅ Success' if redfin_success else '❌ Failed'}")
    
    print(f"\n📦 Zillow:")
    for key, status in zillow_results.items():
        print(f"   {key}: {status}")
    
    print(f"\n📁 All data saved to:")
    print(f"   projects/map_viz/data/redfin/")
    print(f"   projects/map_viz/data/zillow/")
    
    print(f"\n📋 Inventory file:")
    print(f"   projects/map_viz/data/market_data_inventory.json")
    
    print("\n" + "="*70)

if __name__ == '__main__':
    main()