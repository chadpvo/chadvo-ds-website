from flask import Flask, jsonify, request
from flask_cors import CORS
import polars as pl
import os
from functools import lru_cache

app = Flask(__name__)
CORS(app)  # Allow requests from GitHub Pages

# Path to your TSV file - update this to match your local path
# Line 11 - Change to:
DATA_PATH = os.environ.get('DATA_PATH', r'C:\personal_projects\chadvo-ds-website\projects\map_viz\data\redfin\raw\redfin_zip_all_states.tsv')

@lru_cache(maxsize=1)
def load_data():
    """Load the TSV file using Polars (cached for performance)"""
    print(f"Loading data from {DATA_PATH}...")
    df = pl.read_csv(
        DATA_PATH, 
        separator='\t',
        null_values=['NA', 'N/A', ''],  # Treat these as null
        ignore_errors=True,  # Skip parsing errors
        try_parse_dates=True  # Auto-parse date columns
    )
    print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    return df

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check if API is running"""
    return jsonify({'status': 'healthy', 'message': 'Redfin API is running'})

@app.route('/api/schema', methods=['GET'])
def get_schema():
    """Return column names and types"""
    try:
        df = load_data()
        schema = {
            'columns': df.columns,
            'dtypes': {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            'row_count': len(df),
            'sample': df.head(5).to_dicts()
        }
        return jsonify(schema)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/states', methods=['GET'])
def get_states():
    """Get list of unique states"""
    try:
        df = load_data()
        # Use STATE_CODE column which has values like 'CA', 'TX', etc.
        if 'STATE_CODE' in df.columns:
            states = df['STATE_CODE'].unique().drop_nulls().sort().to_list()
            return jsonify({'states': states})
        else:
            return jsonify({'error': 'STATE_CODE column not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data', methods=['GET'])
def get_data():
    """
    Query data with filters
    
    Query params:
    - state: Filter by state (optional)
    - zip_code: Filter by zip code (optional)
    - start_date: Filter by start date YYYY-MM-DD (optional)
    - end_date: Filter by end date YYYY-MM-DD (optional)
    - metric: Column to aggregate (e.g., 'median_sale_price')
    - group_by: Column to group by (e.g., 'period_begin', 'state')
    - limit: Max rows to return (default 1000)
    """
    try:
        df = load_data()
        
        # Apply filters
        state = request.args.get('state')
        zip_code = request.args.get('zip_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        metric = request.args.get('metric')
        group_by = request.args.get('group_by')
        limit = int(request.args.get('limit', 1000))
        
        # Filter by state
        if state:
            if 'STATE_CODE' in df.columns:
                df = df.filter(pl.col('STATE_CODE') == state)
        
        # Filter by zip code
        if zip_code:
            zip_col = next((col for col in df.columns if 'zip' in col.lower()), None)
            if zip_col:
                df = df.filter(pl.col(zip_col) == zip_code)
        
        # Filter by date range
        if start_date or end_date:
            if 'PERIOD_BEGIN' in df.columns:
                if start_date:
                    df = df.filter(pl.col('PERIOD_BEGIN') >= start_date)
                if end_date:
                    df = df.filter(pl.col('PERIOD_BEGIN') <= end_date)
        
        # Group and aggregate if requested
        if group_by and metric:
            if group_by in df.columns and metric in df.columns:
                df = df.group_by(group_by).agg(
                    pl.col(metric).mean().alias(f'avg_{metric}'),
                    pl.col(metric).median().alias(f'median_{metric}'),
                    pl.col(metric).count().alias('count')
                ).sort(group_by)
        
        # Limit results
        df = df.head(limit)
        
        # Convert to JSON
        result = df.to_dicts()
        
        return jsonify({
            'data': result,
            'count': len(result),
            'filters_applied': {
                'state': state,
                'zip_code': zip_code,
                'start_date': start_date,
                'end_date': end_date,
                'metric': metric,
                'group_by': group_by
            }
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()  # Print full error to Flask terminal
        return jsonify({'error': str(e)}), 500

@app.route('/api/aggregate', methods=['POST'])
def aggregate_data():
    """
    Advanced aggregation endpoint with limit to prevent timeouts
    """
    try:
        df = load_data()
        params = request.get_json()
        
        # Apply filters first to reduce data size
        filters = params.get('filters', {})
        for col, value in filters.items():
            if col in df.columns:
                df = df.filter(pl.col(col) == value)
        
        # Limit data before aggregation to prevent timeout
        df = df.head(10000)  # Process max 10k rows
        
        # Group by
        group_cols = params.get('group_by', [])
        aggregations = params.get('aggregations', {})
        
        if group_cols and aggregations:
            agg_exprs = []
            for col, agg_funcs in aggregations.items():
                if col in df.columns:
                    for func in agg_funcs:
                        if func == 'mean':
                            agg_exprs.append(pl.col(col).mean().alias(f'{col}_{func}'))
                        elif func == 'median':
                            agg_exprs.append(pl.col(col).median().alias(f'{col}_{func}'))
                        elif func == 'sum':
                            agg_exprs.append(pl.col(col).sum().alias(f'{col}_{func}'))
                        elif func == 'min':
                            agg_exprs.append(pl.col(col).min().alias(f'{col}_{func}'))
                        elif func == 'max':
                            agg_exprs.append(pl.col(col).max().alias(f'{col}_{func}'))
                        elif func == 'count':
                            agg_exprs.append(pl.col(col).count().alias(f'{col}_{func}'))
            
            if agg_exprs:
                df = df.group_by(group_cols).agg(agg_exprs).sort(group_cols[0])
        
        # Limit final results
        df = df.head(100)
        result = df.to_dicts()
        
        return jsonify({
            'data': result,
            'count': len(result)
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)