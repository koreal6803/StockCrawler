#!/usr/bin/python
import requests
import pandas as pd
import datetime
import time
import random
import numpy
import re

def refineDf(df): # todo: move this function to utility

    # we only need the target season
    df = df.drop(df.columns[list(range(2, len(df.columns)))], axis=1)

    # remove the rows with null elements
    df = df.dropna()

    # transpose the table
    dft = df.T

    # set column names to the first column of df
    dft.columns = df[0]

    # remove the original column names
    dft = dft.drop(0, axis=0)
    
    return dft


def getStatement(s, year, season):

    # convert all the integer or string to string
    s = str(s)
    year = str(year)
    season = str(season)

    # fetch the data
    req = requests.post('http://mops.twse.com.tw/server-java/t164sb01', data = {
        'step':'1',
        'DEBUG':'',
        'CO_ID':s,
        'SYEAR':year,
        'SSEASON':season,
        'REPORT_ID':'C'
        })

    #
    # check whether the data is fetched successfully
    #
    empty = pd.DataFrame()
    
    # check the status of connection
    if req.status_code != 200:
        print('**ERROR: cannot obtain fundamental for stock ' + s + ' year&season: ' + year + ' ' + season)
        return empty, empty, empty

    req.encoding = 'big5'
    
    # check the table is obtained
    tables = pd.read_html(req.text)

    if len(tables) == 1:
        print('**WARRN: cannot obtain fundamental for stock ' + s + ' year&season: ' + year + ' ' + season)
        print(req.text)
        return empty, empty, empty
    #
    # refine the tables
    #
    df1 = refineDf(tables[1])
    df2 = refineDf(tables[2])
    df3 = refineDf(tables[3])
    
    #
    # find public date
    #

    # obtain the sub-string of website
    idx = req.text.find('通過財報之日期及程序')
    dateStr = req.text[idx:idx+500]

    # obtain a smaller string of subStr
    posYear = dateStr.find('年')
    assert(posYear != -1)
    dateStr = dateStr[posYear - 10: posYear + 10]

    # extract the date
    ch2int = {'○': 0, '零': 0, '十': 1, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
    for key, val in ch2int.items():
        dateStr = dateStr.replace(key, str(val)) 
    
    numbers = re.sub("\D", ' ', dateStr).split()

    if int(numbers[0]) < 200:
        numbers[0] = str(int(numbers[0]) + 1911)

    if len(numbers[2]) == 3:
        numbers[2] = numbers[2][0] + numbers[2][2]

    # build the date
    dateStr = numbers[0] + '/' + numbers[1] + '/' + numbers[2]

    print('date: ' + dateStr)

    df1['date'] = pd.Series([dateStr],index=df1.index)
    df2['date'] = pd.Series([dateStr],index=df2.index)
    df3['date'] = pd.Series([dateStr],index=df3.index)

    return df1, df2, df3

def checkDuplicate(df): # todo: move this function to utility
    
    duplicateName = []
    for idx, r in enumerate(df):
        for e in df.columns[idx+1:]:
            if(r == e):
                duplicateName.append(r)

    for r in duplicateName:
        print('**WARRN: duplicate column names')
        print(df[r])

def row2Refine(row):

    idxs = numpy.where(row.columns.values == '繼續營業單位淨利（淨損）')[0]
    if len(idxs) == 0 or len(idxs) == 2:
        typeName = ['基本','稀釋']
        for idx, value in enumerate(idxs):
            row.columns.values[value] = typeName[idx] + '繼續營業單位淨利（淨損）'
    elif len(idxs == 1):
        if row.columns.str.find('基本').all(-1):
            assert(row.columns.str.find('稀釋').all(-1) == False)
            row.columns.values[idxs[0]] = '稀釋繼續營業單位淨利（淨損）'
        if row.columns.str.find('稀釋').all(-1):
            assert(row.columns.str.find('基本').all(-1) == False)
            row.columns.values[idxs[0]] = '基本繼續營業單位淨利（淨損）'
    else:
        assert(0)

    return row
            

def getFundamental(stock, fnames):

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    for year in range(2013, datetime.datetime.now().year+1):
        for season in range(1,5):
            print('parsing stock: ' + stock + ' ' + str(year) + ' ' + str(season))
            time.sleep(random.randint(5,15))
            row1, row2, row3 = getStatement(stock, year, season)
            row2 = row2Refine(row2)
            checkDuplicate(row1)
            checkDuplicate(row2)
            checkDuplicate(row3) 
            if len(row1) != 0:
                row1 = row1.T.drop_duplicates().T
                row2 = row2.T.drop_duplicates().T
                row3 = row3.T.drop_duplicates().T
                
                df1 = df1.append(row1, ignore_index=True)
                df2 = df2.append(row2, ignore_index=True)
                df3 = df3.append(row3, ignore_index=True)

    # set index
    df1 = df1.set_index(pd.DatetimeIndex(df1['date']))
    df1.index.name = 'date'
    df2 = df2.set_index(pd.DatetimeIndex(df2['date']))
    df2.index.name = 'date'
    df3 = df3.set_index(pd.DatetimeIndex(df3['date']))
    df3.index.name = 'date'

    df1 = df1.drop('date', axis=1)
    df2 = df2.drop('date', axis=1)
    df3 = df3.drop('date', axis=1)

    # save all files
    df1.to_csv(fnames[0])
    df2.to_csv(fnames[1])
    df3.to_csv(fnames[2])

#getFundamental('1101', ['1','2','3'])
