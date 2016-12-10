#!/usr/bin/python
import json
import requests
import pandas as pd
import datetime
import time
import random
import numpy
import re
import os.path
from .utility import refineDf, checkDuplicate
'''
def findProxy():
    ips = [
            '85.117.59.142:8080',
            '5.189.171.14:1455',
            '82.114.92.33:8080',
            '118.96.31.91:3128',
            '5.135.164.181:3128'
            ]
    ip = ips[random.randint(0, len(ips)-1)];
    return {
            'http': 'http://'+ip,
            'https': 'http://'+ip
            }
'''
def queryStatement(s, year, season):
    while True:
        try:
            req = requests.post('http://mops.twse.com.tw/server-java/t164sb01', data = {
                'step':'1',
                'DEBUG':'',
                'CO_ID':s,
                'SYEAR':year,
                'SSEASON':season,
                'REPORT_ID':'C'
                })#, proxies=proxies)
            if req.status_code == 200:
                break

        except:
            print('ip is locked. wait for 10 min')
            time.sleep(600)
    req.encoding = 'big5'

    return req

def getStatement(s, year, season):

    # convert all the integer or string to string
    s = str(s)
    year = str(year)
    season = str(season)

    # fetch the data
    req = queryStatement(s, year, season)

    
    # check the table is obtained
    empty = pd.DataFrame()
    try:
        tables = pd.read_html(req.text)
    except:
        print('**WARRN: cannot create any table')
        return [empty, empty, empty]
    
    if len(tables) == 1:
        print('**WARRN: fundamental not found for stock ' + s + ' year&season: ' + year + ' ' + season)
        return [empty, empty, empty]

    #try:
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
    idx = req.text.find('核閱或查核日期')
    dateStr = req.text[idx:idx+100]

    # obtain a smaller string of subStr
    posYear = dateStr.find('-')
    assert(posYear != -1)
    dateStr = dateStr[posYear - 10: posYear + 10]

    numbers = re.sub("\D", ' ', dateStr).split()

    if int(numbers[0]) < 200:
        numbers[0] = str(int(numbers[0]) + 1911)

    # build the date
    dateStr = numbers[0] + '/' + numbers[1] + '/' + numbers[2]

    print('date: ' + dateStr)

    df1['date'] = pd.Series([dateStr],index=df1.index)
    df2['date'] = pd.Series([dateStr],index=df2.index)
    df3['date'] = pd.Series([dateStr],index=df3.index)
    '''
    except e:
        print(e)
        print(req.text)
    '''
    return [df1, df2, df3]

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

    df1 = pd.read_csv(fnames[0], parse_dates=True, index_col=[0]) if os.path.exists(fnames[0]) else pd.DataFrame()
    df2 = pd.read_csv(fnames[1], parse_dates=True, index_col=[0]) if os.path.exists(fnames[1]) else pd.DataFrame()
    df3 = pd.read_csv(fnames[2], parse_dates=True, index_col=[0]) if os.path.exists(fnames[2]) else pd.DataFrame()

    for year in range(2013, datetime.datetime.now().year+1):
        for season in range(1,5):
            # do not parse the statement which is already parsed
            if len(df1) > 0 and len(df1[(df1['year'] == year)&(df1['season'] == season)]) > 0:
                print('jump ' + str(year) + ' ' + str(season))
                continue
            if (year == 2016 and season == 4):
                continue
            
            # parsing the statement
            print('parsing stock: ' + stock + ' ' + str(year) + ' ' + str(season))
            time.sleep(random.randint(5,5))
            rows = getStatement(stock, year, season)
            
            # if the statement cannot be found
            if len(rows[0]) == 0 and year == 2013 and season == 1:
                return
            elif len(rows[0]) == 0:
                continue
            
            # refine row2
            rows[1] = row2Refine(rows[1])
            
            # add year and season
            for idx, r in enumerate(rows):
                rows[idx] = checkDuplicate(rows[idx])
                rows[idx]['year'] = pd.Series([year], index=rows[idx].index)
                rows[idx]['season'] = pd.Series([season], index=rows[idx].index)
                rows[idx]['date'] = pd.to_datetime(rows[idx]['date'])
                rows[idx] = rows[idx].set_index('date')
            
            # add the row into dataframe
            df1 = df1.append(rows[0])
            df2 = df2.append(rows[1])
            df3 = df3.append(rows[2])
    
    df1 = df1.sort_index()
    df2 = df2.sort_index()
    df3 = df3.sort_index()

    # save all files
    df1.to_csv(fnames[0])
    df2.to_csv(fnames[1])
    df3.to_csv(fnames[2])
#getStatement('1626', '2015', '3')
#getFundamental('1101', ['1','2','3'])
