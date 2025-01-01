import duckdb
import os
import sys
import logging

log_file_path = "parts_gen.log"
created_tables = []

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

def check_or_create_table_dir(table: str, root: str) -> None:
    if not table in created_tables:
        os.makedirs(os.path.join(root, table), exist_ok=True)
        created_tables.append(table)



def get_existing_tables(conn):

    try:
        result = conn.execute("SHOW TABLES").fetchall()
        tables = {row[0] for row in result}
        return tables
    except Exception as e:
        logging.error(f"Error fetching tables: {e}")
        return set()

def export_tables_to_parquet(conn, tables, step, prefix):

    for table in tables:
        
        check_or_create_table_dir(table, prefix)
        parquet_file = os.path.join(prefix, table, f"{table}_{str(step).zfill(7)}.parquet")
        try:
            logging.info(f"Exporting table '{table}' to '{parquet_file}'...")
            conn.execute(f"COPY {table} TO '{parquet_file}' (FORMAT 'parquet')")
            logging.info(f"Successfully exported '{table}' to '{parquet_file}'.")
        except Exception as e:
            logging.error(f"Failed to export table '{table}': {e}")

def main():
    setup_logging()

    DBGEN_STEPS = 10000 
    SCALE_FACTOR = 3000
    CHILDREN = 2000
    STEP_START = 0
    OUTPUT_DIR = "/home/jeroen/data/gen/sf3000"
    OUTPUT_PREFIX = "gen/sf3000_s3"
    BUCKET = "output"
    DUCKDB_DATABASE = ":memory:"

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Parquet files will be saved to '{OUTPUT_DIR}' (in table dir).")

    try:
        conn = duckdb.connect(database=DUCKDB_DATABASE)
        conn.execute('load tpch;')

        conn.execute("""
        CREATE SECRET secret1 (
            TYPE S3,
            KEY_ID '',
            SECRET '',
            REGION 'eu-west-3',
            ENDPOINT '192.168.0.192:9000',
            URL_STYLE 'path',
            USE_SSL false
        );
        """
        )


        logging.info(f"Connected to DuckDB database '{DUCKDB_DATABASE}'.")
    except Exception as e:
        logging.error(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)


    initial_tables = get_existing_tables(conn)
    logging.info(f"Initial tables in the database: {initial_tables}")

    for step in range(STEP_START, CHILDREN ):
        logging.info(f"=== Step {step} ===")


        try:
            logging.info(f"Generating data with dbgen (sf={SCALE_FACTOR}, children={CHILDREN}, step={step})...")
            conn.execute(f"CALL dbgen(sf={SCALE_FACTOR}, children={CHILDREN}, step={step})")
            logging.info("Data generation completed.")
        except Exception as e:
            logging.error(f"dbgen failed at step {step}: {e}")
            continue 


        current_tables = get_existing_tables(conn)
        new_tables = current_tables - initial_tables
        if not new_tables:
            logging.warning(f"No new tables generated at step {step}.")
            continue
        logging.info(f"New tables generated at step {step}: {new_tables}")

        export_tables_to_parquet(conn, new_tables, step, OUTPUT_DIR)

        for table in new_tables:

            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                logging.info(f"Dropped table '{table}' after export.")
            except Exception as e:
                logging.error(f"Failed to drop table '{table}': {e}")

    try:
        conn.close()
        logging.info("Closed DuckDB connection.")
    except Exception as e:
        logging.error(f"Error closing DuckDB connection: {e}")

    logging.info("Data generation and export process completed.")

if __name__ == "__main__":
    main()

