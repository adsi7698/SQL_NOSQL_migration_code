import threading

from concurrent.futures import ThreadPoolExecutor
from SQL_PROCESSING.sql_processing import get_total_rows, fetch_data_from_sql


if __name__ == "__main__":
	threads = []
	total_rows = get_total_rows()

	each_shard = total_rows // 6
	remainder = total_rows % 6
	start = 0

	with ThreadPoolExecutor(max_workers=6) as executor:
		for i in range(6):
			count = each_shard + (1 if i < remainder else 0)
			executor.submit(fetch_data_from_sql, start, count)
			start += count

	print("Threads work done.")
