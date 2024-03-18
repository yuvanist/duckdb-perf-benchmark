import time
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta


def generate_and_save_parquet(n_rows, file_path):
    int_data = {
        f"int_col_{i}": np.random.randint(0, 1000, size=n_rows) for i in range(1, 11)
    }
    str_data = {
        f"str_col_{i}": [
            "".join(np.random.choice(list("abcdefghijklmnopqrstuvwxyz"), size=10))
            for _ in range(n_rows)
        ]
        for i in range(1, 11)
    }
    bool_data = {
        f"bool_col_{i}": np.random.choice([True, False], size=n_rows)
        for i in range(1, 6)
    }
    date_data = {
        f"date_col_{i}": np.array(
            [
                datetime(2020, 1, 1) + timedelta(days=int(x))
                for x in np.random.randint(1, 365, size=n_rows)
            ]
        )
        for i in range(1, 6)
    }

    data = {**int_data, **str_data, **bool_data, **date_data}

    df = pd.DataFrame(data)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)


sizes = [1_000_000, 5_000_000]

for size in sizes:
    file_path = f"./data_{size}_rows.parquet"
    print(f"Generating {size} rows of data...")
    time_start = time.time()
    generate_and_save_parquet(size, file_path)
    print(f"Time taken: {time.time() - time_start:.2f} seconds")
