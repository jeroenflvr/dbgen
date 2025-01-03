# dbgen

Standalone partitioned version of duckdb's TPC-H decision-making benchmark dataset generation.  It's basically a wrapper around duckdb's ```dbgen(sf, children, step)```.

This is a prereq for further benchmarking different frameworks, for which I'm building a benchmarking framework with prometheus and grafana.


I also have distributed versions with ray, ballista, rust/arrow and spark, which I'll add soon. Here, for simplicity and if you have the patience and hardware, we're using asyncio and ProcessPoolExecutor with reusable workers (which easily translates to ie. remote [ray](https://github.com/dmatrix/ray-core-tutorial) workers).

Using python, obviously, you'l need to install duckdb with the following prereqs:
- duckdb
- asyncio

TIP: save yourself some time and use [uv](https://docs.astral.sh/uv/) to install the above in a virtual environment.

```shell
$ uv pip install -r requirements.txt 
Resolved 2 packages in 107ms
Installed 2 packages in 11ms
 + asyncio==3.4.3
 + duckdb==1.1.3
$
```

# gist

Define a scale factor, choose the number of partitions, number of worker processes and output location, and sit back and measure temps as this will take a while and contribute to global warming.

Start with scale factor 1, 3 and 10 and work your way up to 3000.

Find the balance between scaling up and scaling out: less processes = more memory, more processes = more overhead.  

WARNING: Too many processes will trigger segfaults.  I guess duckdb and C++ have their limits on thread safety.  On a machine with 36 cores/72 threads, 35 processes has been consistently stable.  So ymmv, but #cores - 1 seems golden.  Duckdb will use all of the available cores for each process, so expect scheduling overhead.


For more information, see this [duckdb extension doc](https://duckdb.org/docs/extensions/tpch) and this [TPC-H paper](https://www.tpc.org/tpch/).

I recommend using [btop](https://github.com/aristocratos/btop) for realtime monitoring your hardware, but [glances](https://github.com/nicolargo/glances), [htop](https://github.com/htop-dev/htop), classic top and sar, [xymon](https://www.xymon.com/), .. of course will work equally well.


There is a "local" version as well as an s3 compatible one.  You decide.
### local
```shell
(venv) $ python scripts/partsgen_param.py --help
usage: partsgen_param.py [-h] [--sf SF] [--parts PARTS] --output OUTPUT [--concurrency CONCURRENCY]

Generate TPC-H Benchmark parquet data using duckdb.

options:
  -h, --help            show this help message and exit
  --sf SF               Scale Factor (default is 1, range: 1, 3, 10, 30, 100, 300, 1000, 3000).
  --parts PARTS         Number of parquet files (default is 10). 0 for no partitioning.
  --output OUTPUT       Output location on disk.
  --concurrency CONCURRENCY
                        Number of concurrent processes

(venv) $ 
```

### s3
```shell
(venv) $ python scripts/partsgen_param_s3.py --help
usage: partsgen_param_s3.py [-h] [--sf SF] [--parts PARTS] --bucket BUCKET --prefix PREFIX
                            [--concurrency CONCURRENCY] [--endpoint ENDPOINT]

Generate TPC-H Benchmark parquet data using duckdb.

options:
  -h, --help            show this help message and exit
  --sf SF               Scale Factor (default is 1, range: 1, 3, 10, 30, 100, 300, 1000, 3000).
  --parts PARTS         Number of parquet files (default is 10). 0 for no partitioning.
  --bucket BUCKET       bucket on s3.
  --prefix PREFIX       prefix on s3.
  --concurrency CONCURRENCY
                        Number of concurrent processes
  --endpoint ENDPOINT   s3 endpoint
(venv) $ 
```

# example
running sf 1000

```shell
(venv) jeroen@biggie:~/projects/dbgen$ time python scripts/partsgen_param.py --sf 1000 --parts 1000 --output /home/moi/data/gen1000 --concurrency 35
2025-01-04 00:34:23,537 [INFO] Parquet files will be saved to '/home/jeroen/data/gen1000' (in table dir).
2025-01-04 00:34:23,537 [INFO] Parquet files will be saved to '/home/jeroen/data/gen1000' (in table dir).
2025-01-04 00:34:23,537 [INFO] Parquet files will be saved to '/home/jeroen/data/gen1000' (in table dir).

```

yielding this load

