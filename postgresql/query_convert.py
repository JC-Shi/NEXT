import geopandas as gpd
import numpy as np
import csv

n_queries = 10000
window_size = 'india_0.01_0.1_test'
queryFile = '/home_nfs/jingyi/lsm/data/query_{}_{}'.format(window_size, n_queries)

resultList = []

with open(queryFile, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='"')
    for line in reader:
        row = [line[2], line[4], line[3], line[5]]
    # query = "SELECT * FROM data \
    # WHERE ST_Intersects(geom, ST_GeomFromText('Polygon(({} {}, {} {}, {} {}, {} {}, {} {}))',4326));\n" \
    # .format(row['minx'], row['miny'], row['maxx'], row['miny'], row['maxx'], row['maxy'], row['minx'], row['maxy'], row['minx'], row['miny'])
    # resultList.append(query)
        query = "SELECT * FROM data \
        WHERE ST_Intersects(geom, ST_GeomFromText('Polygon(({} {}, {} {}, {} {}, {} {}, {} {}))',4326));\n" \
        .format(row[0], row[2], row[1], row[2], row[1], row[3], row[0], row[3], row[0], row[2])
        resultList.append(query)
with open("/home_nfs/jingyi/lsm/data/query_{}_{}.sql".format(window_size, n_queries), 'w') as sqlFile:
    for query in resultList:
        sqlFile.write(query)