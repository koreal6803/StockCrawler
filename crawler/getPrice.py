import datetime
import pandas as pd
import sys
import os.path
import random
import time
import requests
from simplejson import loads
from urllib.parse import urlencode
from urllib.request import Request, urlopen

def get_historical_prices(symbol, start_date, end_date):
    """
    Get historical prices for the given ticker symbol.
    Date format is 'YYYY-MM-DD'
    Returns a nested dictionary (dict of dicts).
    outer dict keys are dates ('YYYY-MM-DD')
    """
    params = urlencode({
        's': symbol,
        'a': int(start_date[5:7]) - 1,
        'b': int(start_date[8:10]),
        'c': int(start_date[0:4]),
        'd': int(end_date[5:7]) - 1,
        'e': int(end_date[8:10]),
        'f': int(end_date[0:4]),
        'g': 'd',
        'ignore': '.csv',
        })
    url = 'http://real-chart.finance.yahoo.com/table.csv?%s' % params
    req = Request(url)
    resp = urlopen(req)
    print('download success')
    content = str(resp.read().decode('utf-8').strip())
    daily_data = content.splitlines()
    hist_dict = []
    keys = daily_data[0].split(',')
    for day in daily_data[1:]:
        day_data = day.split(',')
        date = day_data[0]
        hist_dict.append(\
            {keys[0]: day_data[0],
            keys[1]: day_data[1],
            keys[2]: day_data[2],
            keys[3]: day_data[3],
            keys[4]: day_data[4],
            keys[5]: day_data[5],
            keys[6]: day_data[6]})
    return hist_dict

def approxEqual(x, y, tolerance=0.001):
    return abs(x-y) <= 0.5 * tolerance * (x + y)
def updatePrice(stock, fname):
    try:
        df = pd.read_csv(fname, parse_dates=True, index_col=[0])
    except:
        assert(os.path.isfile(fname) == False)
        return False

    lastDate = str(df.index[-10]).split()[0]
    today = str(datetime.datetime.now()).split()[0]
    tomorrow = str(datetime.date.today() + datetime.timedelta(days=1)).split()[0]

    if lastDate == today:
        print('data is fresh!')
        return False

    print('obtain from ' + lastDate + ' to ' + today)

    #try:
    prices = get_historical_prices(stock+'.TW', lastDate, tomorrow)
    #except:
    #    print('**ERROR: cannot obtain prices of the stock')
    dfnew = pd.DataFrame()
    for p in prices:
        print(p['Date'])
        if not p['Date'] in df.index:
            obj = {key: [value] for key, value in p.items()}
            row = pd.DataFrame(obj)
            row.columns = [i.lower().replace(' ','_') for i in row.columns]
            dfnew = dfnew.append(row)
        else:
            adjClose = float(p['Adj Close'])
            newf = adjClose/df['adj_close'][p['Date']]
            print('adj_close modify factor:',newf)
            if approxEqual(newf, 1):
                df['adj_close'] *= newf

    #print('find dividen factor',f)

    if len(dfnew) > 0:
        dfnew['date'] = pd.to_datetime(dfnew['date'])
        dfnew = dfnew.set_index('date')
        dfnew = dfnew.sort_index()
        df = df.append(dfnew)
        df.sort_index(inplace=True)
        df.to_csv(fname)
    else:
        print("cannot obtain price of any date")



    return True

def getPrice(stock, fname, startDay='2000-01-01'):
    
    if os.path.exists(fname):
        print('stock ' + stock + ' is already obtained')
        return

    todayStr = str(datetime.datetime.now()).split()[0]
    df = pd.DataFrame()
    try:
        prices = get_historical_prices(stock+'.TW', startDay, todayStr)
        print('obtained prices length: ' + str(len(prices)))

    except:
        print('**ERROR: cannot obtain prices of the stock')
        wait = random.randint(20,60)
        return
    else:

        for p in prices:
            obj = {key: [value] for key, value in p.items()}
            row = pd.DataFrame(obj)
            row.columns = [i.lower().replace(' ','_') for i in row.columns]
            df = df.append(row)
        if len(df) > 0:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            df = df.sort_index()
            df.to_csv(fname)
        else:
            print("cannot obtain price of any date")
