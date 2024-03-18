import duckdb
import time


def run_queries(path: str):
    dataset = f"read_parquet('{path}')"
    queries = [
        f"SELECT COUNT(*) FROM {dataset};",
        f"SELECT bool_col_1, AVG(int_col_1) AS avg_int_col_1 FROM {dataset} GROUP BY bool_col_1;",
        f"SELECT SUM(int_col_2) FROM {dataset} WHERE date_col_1 > '2020-06-01';",
        f"SELECT date_col_2, COUNT(DISTINCT str_col_2) FROM {dataset} WHERE bool_col_2 = False GROUP BY date_col_2;",
        f"SELECT bool_col_4, int_col_4, SUM(int_col_4) OVER (PARTITION BY bool_col_4 ORDER BY date_col_3) as running_total FROM {dataset};",
        f"SELECT int_col_5 FROM {dataset} ORDER BY int_col_5 DESC LIMIT 10;",
        f"SELECT * FROM {dataset} WHERE int_col_6 BETWEEN 100 AND 200 AND str_col_4 LIKE '%abc%';",
        f"SELECT str_col_5, AVG(int_col_7) as avg_value FROM {dataset} GROUP BY str_col_5 HAVING AVG(int_col_7) > 500;",
        f"SELECT AVG(int_col_8) FROM {dataset} WHERE int_col_9 > (SELECT AVG(int_col_9) FROM {dataset});",
        f"SELECT date_col_4, MAX(int_col_10) - MIN(int_col_10) AS range_int_col_10 FROM {dataset} GROUP BY date_col_4;",
    ]
    for idx, query in enumerate(queries):
        start_time = time.time()
        duckdb.sql(query)
        time_in_ms = (time.time() - start_time) * 1000
        print(f"Execution time for '{idx+1}': {time_in_ms} ms")


sizes = [1_000_000, 5_000_000]

for size in sizes:
    file_path = f"./data_{size}_rows.parquet"
    print(f"Running queries on {file_path}")
    run_queries(file_path)
