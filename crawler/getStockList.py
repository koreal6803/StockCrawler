#!/usr/bin/python
import pandas as pd
import datetime as dt
import requests
import pickle

def getTradingStockList(fname):
    tbl = pd.read_html(io="http://isin.twse.com.tw/isin/C_public.jsp?strMode=2", encoding='big5')[0]

    # for testing without refetch tbl
    #pickle.dump(tbl, open('tbl.txt', 'wb'))
    #tbl = pickle.load(open('tbl.txt', 'rb'))
    
    # set column names
    tbl.columns = tbl.T[0]

    # remove the row with column names
    tbl = tbl.drop(0, axis=0)
    tbl = tbl.drop(1, axis=0)

    # remove the column 備註
    tbl = tbl.drop('備註', axis=1)

    tbl = tbl.dropna()

    #print(tbl)

    # set date to index
    tbl = tbl.set_index(pd.DatetimeIndex(tbl['上市日']))
    tbl.index.name = 'date'

    # seperate the stock sequence number and its name
    seqs = []
    names = []
    for idx, sName in tbl['有價證券代號及名稱'].iteritems():

        # some of the white space is larger, change it to smaller one
        sName = sName.replace('　',' ')
        sName = sName.split(' ')
        seqs.append(sName[0])
        names.append(sName[1])
    
    # add new series we just made
    tbl['證券代號'] = pd.Series(seqs, index=tbl.index)
    tbl['證券名稱'] = pd.Series(names, index=tbl.index)
    
    # remove the old series
    tbl = tbl.drop('有價證券代號及名稱', axis=1)
    
    # save the file
    tbl.to_csv(fname)
    #test = pd.read_csv('stock_list.csv', parse_dates=True, index_col=[0])


def getSuspendedStockList(fname):

    # query from website
    r = requests.post('http://www.tse.com.tw/ch/listed/suspend_listing.php',data = {'download':'csv'})

    # encoding
    r.encoding = 'big5'

    print(r.text)
    exit(0)

    # remove all '"'
    lines = r.text.replace('"','')

    # seperate single line to lines
    lines = lines.split('\n')

    # seperate one line to serveral columns
    lines = [l.split(',') for l in lines]

    # refine the suspend time and stock id
    time = []
    stockID = []
    for l in lines:
        if len(l) == 4 and len(l[2]) == 4 and l[2].isdigit():
            date = l[0].split('.')
            if (date[0].isdigit() == False):
                continue
            date[0] = str(int(date[0]) + 1911)
            stockID.append(l[2])
            time.append('/'.join(date))
    
    # make a dataframe
    df = pd.DataFrame({'證券代號': stockID}, index=time)
    df.index.name = 'date'
    
    # save the file
    df.to_csv(fname)

#getTradingStockList('newfile.csv')
#getStockList()
#getSuspendStockList()
