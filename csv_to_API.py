import pandas as pd
from csv import reader
import time
mypath="D:\\UNI\\data_engineering2\\heidelberg.csv"
def csvReader(path,step):
    with open(path,'r') as F:
        csv_reader = reader(F)
        header = next(csv_reader)
        row_list = []
        for idx,row in enumerate(csv_reader):
            row_list.append(row)

            if (idx+1)%step==0:
                yield row_list
                row_list.clear()

for rows in csvReader(mypath,20):
    time.sleep(0.1)
    print(rows)



