import requests
import pandas as pd
import os
import datetime
import zipfile
import math
import io
import time
import re
import collections
from .utility import checkDuplicate

def getSimpleStatement(path):
    if os.path.isdir(path) == False:
       os.mkdir(path) 

    obtainedFiles = [i[:-4] for i in os.listdir(path)]

    
    now = datetime.datetime.now()
    for year in range(2000, now.year+1):
        for season in range(1, 5):

            dateStr = str(year) + 'Q' + str(season)

            if dateStr in obtainedFiles:
                print('skip statement: ' + dateStr)
                continue
            
            print('downloading statement: ' + dateStr)
            
            r = requests.get('http://www.tse.com.tw/ch/inc/download.php?l1=%A4W%A5%AB%A4%BD%A5q%A9u%B3%F8&l2=%A4W%A5%AB%AA%D1%B2%BC%A4%BD%A5q%B0%5D%B0%C8%B8%EA%AE%C6%C2%B2%B3%F8&url=/ch/statistics/download/05/001/'+dateStr+'_C05001.zip', stream=True)
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

def readExcelOld2(f):

    df = pd.read_excel(f)
    df.columns = [i for i in range(0,len(df.columns))]
    #df = df[df[0].str.split().str[0].str.isdigit() == True]

    # find the title rows by calculating the charateracter.
    # if the row contains the maximum chinese charateracter, 
    # then it is the title row
    titlteRow = -1
    maxChRow = 0
    for idx, row in enumerate(df.values):
        chCnt = 0
        for s in row:
            s = str(s)
            s = s.replace('年','/')
            s = s.replace('月',' ')
            if len(re.findall(u"[\u4e00-\u9fff]+",s)) != 0:
                chCnt += 1
        if chCnt > maxChRow:
            titleRow = idx
            maxChRow = chCnt
        if idx > 20:
            break

    # merge the some information in the index
    l = df.index.tolist()
    if isinstance(l[0], collections.Iterable):
        for i in range(len(l[0])):
            s = pd.Series([row[i] for row in l], index = df.index)
            df[str(i)] = s
    
    # the title is seperate in two rows (titleRow, titleRow + 1)
    # merge titleRow , titleRow+1, and remove the spacing
    row1 = df.values[titleRow]
    row2 = df.values[titleRow+1]
    row3 = df.values[titleRow+2]

    ts = []
    for idx in range(len(df.columns)):
        t = str(row1[idx]) + str(row2[idx]) + str(row3[idx])
        t = t.replace('nan','')
        t = t.replace(' ','')
        t = t.replace('\n','')
        t = t.replace('　','')
        ts.append(t)
    
    def standardColName(cname):

        names = ['營業收入','營業利益','營業外','稅後純益','期末股本','每股稅後純益', '每股淨值', '淨值佔總資產','流動比率','速動比率','稅前純益','公司']
        for n in names:
            idx = cname.find(n)
            if idx != -1:
                return n
        return cname

    df.columns = [standardColName(t) for t in ts]
    #print(df.columns)

    # find the index
    idxColId = -1
    for row in df.values:
        b = False
        for idx, e in enumerate(row):
            if isinstance(e, str) and e.find('1101') != -1:
                idxColId = idx
                #print([i[idxColId] for i in df.values])
                b = True
                break
        if b == True:
            break

    index = []
    for d in df.values:
        d = re.findall(r'\d+', str(d[idxColId]))
        d = ''.join(d)
        index.append(d)

    # remain only digit in index row
    df = df.set_index(pd.Series(index))
    df.index.name = 'stock'

    # drop all strange lines
    df = df[df.index.str.isdigit() == True]
    df.to_csv(f[:-4] + '.refine.csv')

def refineSimpleStatement(path):
    
    obtainedFiles = [i for i in os.listdir(path) if i[-3:] == 'xls']

    newFiles = {}

    for f in obtainedFiles:
        print('build '+ f)
        df = readExcelOld2(path + '/' + f)
        print('success')

def splitDataToStocks(path):
    obtainedFiles = [i for i in os.listdir(path) if i[-10:] == 'refine.csv']
    newFiles = {}
    names = ['營業收入','營業利益','營業外','稅後純益','期末股本','每股稅後純益', '每股淨值', '淨值佔總資產','流動比率','速動比率','稅前純益','公司']
    for f in obtainedFiles:
        df = pd.read_csv(path+'/'+f, index_col=[0])
        #df = pd.read_csv(path + '/' + f)
        for idx, row in df.iterrows():
            if(math.isnan(idx)):
                continue
            idx = str(int(idx))
            print(f + ' ' + idx)
            row = row.drop([c for c in row.index if not c in names],axis=0)
            row['year&season'] = int(f[:4] + '0' + f[5])

            if not idx in newFiles:
                newFiles[idx] = pd.DataFrame()

            #row = checkDuplicate(row, saveOne=True)
            newFiles[idx] = newFiles[idx].append(row)

    for key, df in newFiles.items():
        df.reset_index()
        df = df.set_index('year&season').sort_index()
        df.index = df.index.astype(int)
        fname = path + '/s' + key + '.csv'
        if os.path.isfile(fname):
            os.remove(fname)
        df.to_csv(fname)



