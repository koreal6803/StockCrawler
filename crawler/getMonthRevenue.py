import requests
import pandas as pd
import os
import datetime
import zipfile
import io
import time
from .utility import checkDuplicate

def getMonthRevenue(path):
    if os.path.isdir(path) == False:
       os.mkdir(path) 

    obtainedFiles = [i[:-4] for i in os.listdir(path)]

    
    now = datetime.datetime.now()
    for year in range(2000, now.year+1):
        for month in range(1, 13):

            if year == now.year and month == now.month:
                break

            monthStr = '0' + str(month) if month < 10 else str(month)
            dateStr = str(year) + monthStr

            if dateStr in obtainedFiles:
                print('skip revenue: ' + dateStr)
                continue
            
            print('downloading revenue: ' + dateStr)
            
            r = requests.get('http://www.tse.com.tw/ch/inc/download.php?l1=%A4W%A5%AB%A4%BD%A5q%A4%EB%B3%F8&l2=%B0%EA%A4%BA%A4W%A5%AB%A4%BD%A5q%C0%E7%B7%7E%C3B%A4%CE%ADI%AE%D1%ABO%C3%D2%AA%F7%C3B%B7J%C1%60%AA%ED&url=/ch/statistics/download/04/003/'+dateStr+'_C04003.zip', stream=True)
            print('download success')
            try:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                fname = z.filelist[0].filename
                z.extractall(path)
                print('unzip success')
                os.rename(path+'/'+fname, path+'/'+dateStr+'.xls')
                print('rename success')
            except:
                print('fail to obtain data')

            time.sleep(20)

def refineMonthRevenue(path):
    
    obtainedFiles = [i for i in os.listdir(path) if i != 'summary.csv']

    tbl = pd.DataFrame()
    for f in obtainedFiles:
        df = pd.read_excel(path + '/' + f)
        df.columns = [i for i in range(0,len(df.columns))]
        df = df[df[0].str.split().str[0].str.isdigit() == True]
        df = df.drop([1,3,4,5,6,7,8,9],axis=1)
        dft = df.T
        dft.columns = df[0].str.split().str[0]
        dft = dft.drop(0, axis=0)

        year = int(f[:4])
        month = int(f[4:6])

        if month == 12:
            month = 1
            year += 1
        else:
            month = month + 1

        monthStr = '0' + str(month) if month < 10 else str(month)
        yearStr = str(year)

        dft['date'] = [yearStr + '-' + monthStr + '-10']
        dft['date'] = pd.to_datetime(dft['date'])
        dft = dft.set_index(dft['date'])
        dft = checkDuplicate(dft, saveOne=True)
        
        try:
            tbl = tbl.append(dft)
            print(dft.head())
            print(yearStr + '-' + monthStr + '-10')
        except:
            print(tbl.columns)
            print(dft.columns)
    os.remove(path+'/summary.csv')
    tbl.to_csv(path+'/summary.csv')
    



