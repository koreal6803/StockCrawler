from yahoo_finance import Share
import datetime
import pandas as pd
import os.path
import time

def getPrice(stock, fname):

    if os.path.exists(fname):
        print('stock ' + stock + ' is already obtained')
        return

    time.sleep(10)
    s = Share(stock+'.TW')
    #print(s.get_historical('2000-01-01', '2000-01-10'))
    todayStr = ''+str(datetime.datetime.now().year) + '-' + str(datetime.datetime.now().month) + '-' + str(datetime.datetime.now().day)
    try:
        prices = s.get_historical('2000-01-01',todayStr)
    except:
        print('**ERROR: cannot obtain prices of the stock')
    else:
        df = pd.DataFrame()

        for p in prices:
            obj = {key: [value] for key, value in p.items()}
            df = df.append(pd.DataFrame(obj))

        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
        df.to_csv(fname)

#getPrice('2330','test.csv')
