import asyncio
import duckdb
import os
import sys
import logging
import argparse
from concurrent.futures import ProcessPoolExecutor

log_file_path = "parts_gen.log"
created_tables = []


def setup_logging():

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

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


def get_existing_tables(conn: duckdb.DuckDBPyConnection) -> set[duckdb.table]:

    try:
        result = conn.execute("SHOW TABLES").fetchall()
        tables = {row[0] for row in result}
        return tables
    except Exception as e:
        logging.error(f"Error fetching tables: {e}")
        return set()


def export_tables_to_parquet(conn: duckdb.DuckDBPyConnection,
            tables: set[duckdb.table],
            step: int,
            output_path: str):

    for table in tables:
        check_or_create_table_dir(table, output_path)
        parquet_file = os.path.join(
            output_path, table, f"{table}_{str(step).zfill(7)}.parquet"
        )

        try:
            logging.info(f"Exporting table '{table}' to '{parquet_file}'...")
            conn.execute(f"COPY {table} TO '{parquet_file}' (FORMAT 'parquet')")
            logging.info(f"Successfully exported '{table}' to '{parquet_file}'.")
        except Exception as e:
            logging.error(f"Failed to export table '{table}': {e}")


def process_part_test(part: int, chunks: int, sf: int, output_dir: str) -> None:
    logging.info(f"processing part {part} out of {chunks} and saving to {output_dir}")


# async def run_blocking_process_part(step, chunks, sf, output_dir):
#     try:
#         result = await asyncio.to_thread(process_part, step, chunks, sf, output_dir)
#         return result
#     except asyncio.CancelledError:
#         print(f"Task {step}: Was canceled.")
#         raise 
#     except Exception as e:
#         print(f"Task {step}: Encountered an exception - {e}")
#         return e  



def process_part(step: int, chunks: int, sf: int, output_dir: str) -> None:

    DUCKDB_DATABASE = (
        ":memory:"  # Use ':memory:' for in-memory DB or provide a file path
    )

    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Parquet files will be saved to '{output_dir}' (in table dir).")

    try:
        conn = duckdb.connect(database=DUCKDB_DATABASE)
        conn.execute("load tpch;")


        logging.info(f"Connected to DuckDB database '{DUCKDB_DATABASE}'.")
    except Exception as e:
        logging.error(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)
    
    initial_tables = get_existing_tables(conn)
    logging.info(f"Initial tables in the database: {initial_tables}")

    logging.info(f"=== Step {step} ===")

    try:
        logging.info(
            f"STEP {step}: Generating data with dbgen (sf={sf}, children={chunks}, step={step})..."
        )
        conn.execute(
            f"CALL dbgen(sf={sf}, children={chunks}, step={step})"
        )
        logging.info(f"STEP {step}: Data generation completed.")
    except Exception as e:
        logging.error(f"dbgen failed at step {step}: {e}")
        return

    current_tables = get_existing_tables(conn)
    new_tables = current_tables - initial_tables
    if not new_tables:
        logging.warning(f"STEP {step}:  No new tables generated at step {step}.")
        return
    logging.info(f"STEP {step}: New tables generated at step {step}: {new_tables}")

    export_tables_to_parquet(conn, new_tables, step, output_dir)

    for table in new_tables:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            logging.info(f"STEP {step}: Dropped table '{table}' after export.")
        except Exception as e:
            logging.error(f"STEP {step}: Failed to drop table '{table}': {e}")

    try:
        conn.close()
        logging.info(f"STEP {step}: Closed DuckDB connection.")
    except Exception as e:
        logging.error(f"STEP {step}: Error closing DuckDB connection: {e}")


async def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Generate TPC-H Benchmark parquet data using duckdb."
    )

    parser.add_argument(
        "--sf",
        type=int,
        default=1,
        help="Scale Factor (default is 1, range: 1, 3, 10, 30, 100, 300, 1000, 3000).",
    )

    parser.add_argument(
        "--parts",
        type=int,
        default=10,
        help="Number of parquet files (default is 10). 0 for no partitioning.",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        default=None,
        help="Output location on disk.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        required=False,
        default=2,
        help="Number of concurrent processes",
    )


    args = parser.parse_args()


    SCALE_FACTOR = args.sf
    CHILDREN = args.parts
    STEP_START = 0
    CONCURRENCY = args.concurrency
    OUTPUT_DIR = (
        args.output
    )


    max_workers = CONCURRENCY  
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(executor, process_part, step, CHILDREN, SCALE_FACTOR, OUTPUT_DIR)
            for step in range(STEP_START, CHILDREN)
        ]


        results = await asyncio.gather(*tasks, return_exceptions=True)


    print(f"{results=}")

    logging.info("Data generation and export process completed.")


if __name__ == "__main__":
    asyncio.run(main())
