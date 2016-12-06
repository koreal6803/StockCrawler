#!/usr/bin/python
import requests
import pandas as pd
import datetime
import time
import random
import os.path
from .utility import refineDf, checkDuplicate


def queryBalanceSheet(stock, year, season, statement):
    url = {
            'balance_sheet':'http://mops.twse.com.tw/mops/web/ajax_t05st33',
            'comprehensive_income':'http://mops.twse.com.tw/mops/web/ajax_t05st34',
            'cash_flow':'http://mops.twse.com.tw/mops/web/ajax_t05st39'
    }
    while True:
        try:
            req = requests.post(url[statement], data = {
                'encodeURIComponent':1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'queryName': stock,
                'InpuType': stock,
                'TYPEK': 'all',
                'isnew': 'false',
                'co_id': stock,
                'year': year - 1911,
                'season': '0' + str(season)
                })
            if req.status_code == 200:
                req.encoding = 'utf-8'
                break
        except:
            print('ip is locked. wait for 10 min')
            time.sleep(600)

    return req

def parseHtml(html, statement):
    if statement == 'comprehensive_income':
        tables = pd.read_html(html)
        row = refineDf(tables[2])
        checkDuplicate(row)
        return row
    elif statement == 'cash_flow':
        for c in '$(),':
            html = html.replace(c,'')
        html = html.split('\n')
        html = [h.split() for h in html]

        row = pd.DataFrame()
        for h in html:
            if len(h) > 1 and h[1].isdigit():
                row[h[0]] = pd.Series([h[1]])

        return row
    elif statement == 'balance_sheet':

        # remove some redundant tables that cause error
        html = html.lower()
        tbodyIdx = html.index('<tbody>')
        tbodyIdx2 = html.index('</tbody>')
        html = html[:tbodyIdx] + html[tbodyIdx2+8:]
        
        tables = pd.read_html(html)
        row = refineDf(tables[2])
        checkDuplicate(row)
        return row

def getFundamental2012(stock, fname, statement):

    # get the old balance sheet
    # if there is no balance sheet, create an empty one
    df = pd.read_csv(fname, parse_dates=True, index_col=[0]) if os.path.exists(fname) else pd.DataFrame()

    # read all the time
    for year in range(2008, 2009):
        for season in range(1,5):

            if len(df) > 0 and len(df[(df['year'] == year)&(df['season'] == season)]) > 0:
                print('jump ' + str(year) + ' ' + str(season))
                continue

            print('parsing balance sheet stock: ' + stock + ' ' + str(year) + ' ' + str(season))
            req = queryBalanceSheet(stock, year, season, statement)
            row = parseHtml(req.text, statement)

            season2Date = ['',
                    str(year)+'/04/30',
                    str(year)+'/08/31',
                    str(year)+'/10/31',
                    str(int(year)+1)+'/03/31']

            row['date'] = pd.Series([season2Date[season]],index=row.index)
            row['year'] = pd.Series([year], index=row.index)
            row['season'] = pd.Series([season], index=row.index)
            row['date'] = pd.to_datetime(row['date'])
            row = row.set_index('date')
            df = df.append(row)

    df = df.sort_index()
    print(df)
    exit()
    df.to_csv(fname)

def getComprehensiveIncome(stock, fname):
    pass

def getCashFlow(stock, fname):
    pass

