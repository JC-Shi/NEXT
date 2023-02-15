import psycopg2
import numpy as np
import csv

import time

database = "test"

conn = psycopg2.connect(
    host="localhost",
    database=database,
    user="jingyi",
    port="5444"
)

cur = conn.cursor()

queries = open(
    "/home_nfs/jingyi/lsm/data/query_india_0.001_0.01_test_10000.sql", "r")

query_list = []
for line in queries:
    query_list.append(line.strip())

start = time.time()

# n_test_queries = min(100, len(query_list))
# n_test_queries = len(query_list)
n_test_queries = 100

total_time = 0
for i in range(n_test_queries):
    if i % 100 == 0:
        print("{} queries executed".format(i))
    query_start = time.time()
    cur.execute(query_list[i])
    query_end = time.time()
    total_time += (query_end - query_start)
    # print("query execution time: {}".format(query_end - query_start))
    


print("Total time takes for executing {} queries: {}".format(
        n_test_queries, total_time))

# output_file = "./query_latency/{}.csv".format(database)
# with open(output_file, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     for row in time_list:
#         writer.writerow(row)