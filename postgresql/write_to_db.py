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


n = 10000000
dataset_name = "india"
data = np.load("/home_nfs/jingyi/lsm/data/data_{}_{}.npy".format(dataset_name, n))


print(data.shape)
print(data[:5])



cur.execute('CREATE TABLE data(id int, geom geometry(Polygon, 4326));')

for i in range(n):
    cur.execute("INSERT INTO data(id, geom) VALUES ({},ST_GeometryFromText('POLYGON(({} {},{} {},{} {},{} {},{} {}))'));".format(i, data[i,0], data[i,2], data[i,1], data[i,2], data[i,1], data[i,3], data[i,0], data[i,3], data[i,0], data[i,2]))

conn.commit() # <--- makes sure the change is shown in the database
conn.close()
cur.close()
