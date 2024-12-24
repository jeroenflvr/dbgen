import duckdb

# Connect to your DuckDB database
con = duckdb.connect('/data/tpch/sf3000_2/tpch-sf3000.db')
con.execute('load httpfs;')

con.execute("""
CREATE SECRET secret1 (
    TYPE S3,
    KEY_ID 'AKIA2FAD7R26QK425N7V',
    SECRET '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',
    REGION 'eu-west-3',
    ENDPOINT 's3.eu-west-3.amazonaws.com'
);
"""
)

# Determine the total number of rows
total_rows = con.execute('SELECT COUNT(*) FROM lineitem').fetchone()[0]

# Set the batch size
batch_size = 5000000

# Calculate the total number of batches
total_batches = (total_rows + batch_size - 1) // batch_size  # Ceiling division

for batch_number in range(0, total_batches):
    offset = batch_number * batch_size
    output_file = f's3://tp-bench-data/sf3000/lineitem/part_{str(batch_number).zfill(6)}.parquet'
    query = f'''
    COPY (
        SELECT * FROM lineitem
        LIMIT {batch_size} OFFSET {offset}
    ) TO '{output_file}' (FORMAT PARQUET);
    '''
    print(f'Processing batch {batch_number + 1}/{total_batches}')
    con.execute(query)

con.close()

