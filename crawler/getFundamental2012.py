#!/usr/bin/python
import requests
import pandas as pd
import datetime
import time
import random


def refineDf(df):
    df = df.drop(df.columns[list(range(2, len(df.columns)))], axis=1)
    df = df.dropna()
    dft = df.T
    dft.columns = df[0]
    dft = dft.drop(0, axis=0)
    
    return dft

def getBalanceSheet(stock, fname):
    pass
    ''' it is just an example pseudo code
    df = pd.DataFrame()
    for each year:
        for each season:
            req = requests.post('http://', data = {
                'CO_ID':stock,
                'SYEAR':year,
                'SSEASON':season,
                })

            if check(req) == False:
                return

            req.encoding = 'big5'
            tables = pd.read_html(req.text)
            row = refineDf(tables[1])# i guess is table 1 but i am not sure

            checkDuplicate(row)
            row = row.T.drop_duplicates().T

            df = df.append(row, ignore_index=True)

            season2Date = #not the same as 2013~
            season2Year = ['', year, year, year, str(int(year)+1)]
            dateStr = season2Year[int(season)] + '/'+season2Date[int(season)]
            df['date'] = pd.Series([dateStr],index=df.index)

    df = df.set_index(pd.DatetimeIndex(df['date']))
    df.index.name = 'date'
    df.to_csv(fname)
    '''
def getComprehensiveIncome(stock, fname):
    pass

def getCashFlow(stock, fname):
    pass

