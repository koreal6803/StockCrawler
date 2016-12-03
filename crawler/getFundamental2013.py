#!/usr/bin/python
import requests
import pandas as pd
import datetime
import time
import random

def refineDf(df): # todo: move this function to utility
    df = df.drop(df.columns[list(range(2, len(df.columns)))], axis=1)
    df = df.dropna()
    dft = df.T
    dft.columns = df[0]
    dft = dft.drop(0, axis=0)
    
    return dft


def getStatement(s, year, season):

    s = str(s)
    year = str(year)
    season = str(season)

    req = requests.post('http://mops.twse.com.tw/server-java/t164sb01', data = {
        'step':'1',
        'DEBUG':'',
        'CO_ID':s,
        'SYEAR':year,
        'SSEASON':season,
        'REPORT_ID':'C'
        })
    empty = pd.DataFrame()

    if req.status_code != 200:
        print('**ERROR: cannot obtain fundamental for stock ' + s + ' year&season: ' + year + ' ' + season)
        return empty, empty, empty

    req.encoding = 'big5'
    
    tables = pd.read_html(req.text)

    if len(tables) == 1:
        print('**WARRN: cannot obtain fundamental for stock ' + s + ' year&season: ' + year + ' ' + season)
        return empty, empty, empty

    df1 = refineDf(tables[1])
    df2 = refineDf(tables[2])
    df3 = refineDf(tables[3])

    season2Date = ['', '05/30', '08/31', '11/29', '3/31']
    season2Year = ['', year, year, year, str(int(year)+1)]
    dateStr = season2Year[int(season)] + '/'+season2Date[int(season)]
    df1['date'] = pd.Series([dateStr],index=df1.index)
    df2['date'] = pd.Series([dateStr],index=df2.index)
    df3['date'] = pd.Series([dateStr],index=df3.index)

    return df1, df2, df3

def checkDuplicate(df): # todo: move this function to utility
    
    duplicateName = []
    for idx, r in enumerate(df):
        cnt = 0
        for e in df:
            if(r == e):
                cnt += 1
        if (cnt > 1):
            duplicateName.append(r)

    for r in duplicateName:
        dd = df[r]
        dd.columns = list(range(0,len(dd.columns)))
        if 1 != len(set(dd.T.values[0])):
            print('**WARRN: duplicate is reduced: ' + r)
            print(dd)

def getFundamental(stock, fnames):

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()

    for year in range(2013, datetime.datetime.now().year+1):
        for season in range(1,5):
            print('parsing stock: ' + stock + ' ' + str(year) + ' ' + str(season))
            time.sleep(random.randint(5,15))
            row1, row2, row3 = getStatement(stock, year, season)
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

