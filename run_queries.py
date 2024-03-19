import duckdb
import time


def run_queries(path: str):
    dataset = f"read_parquet('{path}')"
    queries = [
        f"SELECT deal_region, deal_status, COUNT(*) AS deal_count, AVG(deal_amount) AS average_deal_amount FROM {dataset} GROUP BY deal_region, deal_status ORDER BY average_deal_amount DESC LIMIT 10;",
        f"SELECT deal_region, AVG(customer_satisfaction) AS avg_satisfaction, COUNT(*) AS responses FROM {dataset} WHERE customer_satisfaction IS NOT NULL GROUP BY deal_region HAVING COUNT(*) > 50 ORDER BY avg_satisfaction DESC LIMIT 5;",
        f"SELECT EXTRACT(YEAR FROM deal_creation_date) AS year, EXTRACT(MONTH FROM deal_creation_date) AS month, SUM(deal_amount) AS total_deal_amount, COUNT(*) AS deal_count FROM {dataset} WHERE deal_priority > 7 GROUP BY year, month ORDER BY year, month;",
        f"SELECT animal_name, COUNT(*) AS num_deals FROM {dataset} GROUP BY animal_name ORDER BY num_deals DESC LIMIT 5 UNION ALL SELECT plant_name, COUNT(*) AS num_deals FROM {dataset} GROUP BY plant_name ORDER BY num_deals DESC LIMIT 5;",
        f"SELECT deal_name, deal_probability, customer_satisfaction FROM {dataset} WHERE deal_probability > 80 AND customer_satisfaction < 3 ORDER BY customer_satisfaction, deal_probability DESC LIMIT 10;",
        f"SELECT deal_priority, AVG(deal_amount) AS avg_amount FROM {dataset} WHERE deal_status = 'Closed Won' GROUP BY deal_priority ORDER BY deal_priority;",
        f"SELECT EXTRACT(YEAR FROM deal_close_date) AS year, EXTRACT(MONTH FROM deal_close_date) AS month, COUNT(*) AS deal_count, AVG(sparse_decimal) AS avg_sparse_decimal FROM {dataset} WHERE deal_status = 'Closed Won' AND sparse_decimal IS NOT NULL GROUP BY year, month ORDER BY year, month;",
    ]
    for idx, query in enumerate(queries):
        start_time = time.time()
        duckdb.sql(query).show()
        time_in_ms = (time.time() - start_time) * 1000
        print(f"Execution time for '{idx+1}': {time_in_ms} ms")


run_queries("DDS*.parquet")
