import numpy as np
import csv
import geopandas as gpd
from shapely import geometry
import matplotlib.pyplot as plt 
import matplotlib


n = 100000000

info = "india"
# dataList = np.load("/home_nfs/jingyi/lsm/data/data_{}_{}.npy".format(info, n))
dataList = np.load("/home/jingyi/spatial-lsm/data/data_{}_{}.npy".format(info, n))
if dataList.shape[1] == 2:
    data_list = [dataList[:, 0], dataList[:, 0], dataList[:, 1], dataList[:, 1]]
    dataList = np.transpose(np.stack(data_list))
    print(dataList.shape)
print(dataList[:5])
# MBRList = [[1, int(dataList[i][4]), dataList[i][0], dataList[i][2], dataList[i][1], dataList[i][3]] for i in range(dataList.shape[0])]
MBRList = [[1, i, dataList[i][0], dataList[i][2], dataList[i][1], dataList[i][3]] for i in range(dataList.shape[0])]

# with open("/home_nfs/jingyi/lsm/data/data_{}_{}".format(info, n), 'w') as csvfile:
with open("/home/jingyi/spatial-lsm/data/data_{}_{}".format(info, n), 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for row in MBRList:
        writer.writerow(row)