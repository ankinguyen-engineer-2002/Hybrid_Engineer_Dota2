import duckdb
import json
import os

conn = duckdb.connect('/dbt/omniverse.duckdb')

tables_to_export = [
    'silver_dota2_matches',
    'silver_players', 
    'gold_match_analytics'
]

export_summary = []

for table in tables_to_export:
    try:
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        output_path = f'/dbt/{table}.parquet'
        conn.execute(f'COPY {table} TO "{output_path}" (FORMAT PARQUET)')
        size_bytes = os.path.getsize(output_path)
        size_mb = round(size_bytes / (1024 * 1024), 2)
        
        export_summary.append({
            'table': table,
            'rows': count,
            'size_mb': size_mb,
            'file': f'{table}.parquet',
            'status': 'SUCCESS'
        })
        
        print(f'✅ Exported {table}: {count} rows, {size_mb} MB')
        
    except Exception as e:
        export_summary.append({
            'table': table,
            'status': 'FAILED',
            'error': str(e)
        })
        print(f'❌ Failed {table}: {e}')

with open('/dbt/export_manifest.json', 'w') as f:
    json.dump(export_summary, f, indent=2)

print('\n📊 Export Summary:')
print(json.dumps(export_summary, indent=2))
