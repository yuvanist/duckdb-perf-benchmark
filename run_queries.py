import duckdb
import time


def run_queries(path: str):
    dataset = f"read_parquet('{path}')"
    queries = [
        f"SELECT deal_region, deal_status, COUNT(*) AS deal_count, AVG(deal_amount) AS average_deal_amount FROM {dataset} GROUP BY deal_region, deal_status ORDER BY average_deal_amount DESC LIMIT 10;",
        f"SELECT deal_region, AVG(customer_satisfaction) AS avg_satisfaction, COUNT(*) AS responses FROM {dataset} WHERE customer_satisfaction IS NOT NULL GROUP BY deal_region HAVING COUNT(*) > 10 ORDER BY avg_satisfaction DESC LIMIT 5;",
        f"SELECT deal_name, deal_probability, customer_satisfaction FROM {dataset} WHERE deal_probability > 0.7 AND customer_satisfaction < 1000 ORDER BY customer_satisfaction, deal_probability DESC LIMIT 10;",
        f"SELECT deal_priority, AVG(deal_amount) AS avg_amount FROM {dataset} WHERE deal_status = 'Closed Won' GROUP BY deal_priority ORDER BY deal_priority;",
        f"SELECT deal_owner_email, COUNT(*) AS deals_closed FROM {dataset} WHERE deal_status = 'Closed Won' GROUP BY deal_owner_email ORDER BY deals_closed DESC LIMIT 5;",
        f"SELECT deal_state, AVG(deal_amount) AS average_deal_amount, COUNT(*) AS total_deals FROM {dataset} GROUP BY deal_state ORDER BY total_deals DESC;",
        f"SELECT deal_priority, COUNT(*) AS deal_count, MIN(deal_amount) AS min_deal_amount, MAX(deal_amount) AS max_deal_amount, AVG(deal_amount) AS avg_deal_amount FROM {dataset} GROUP BY deal_priority ORDER BY deal_priority;",
    ]

    for idx, query in enumerate(queries):
        start_time = time.time()
        duckdb.sql(query).show()
        time_in_ms = (time.time() - start_time) * 1000
        print(f"Execution time for '{idx+1}': {time_in_ms} ms")


run_queries("DDS*.parquet")
