#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import sys
import os
import pandas as pd
from crawler.getStockList import getTradingStockList
from crawler.getStockList import getSuspendedStockList
from crawler.getFundamental2013 import getFundamental
from crawler.getFundamental2012 import getFundamental2012
from crawler.getPrice import getPrice

tradingStockListDir = 'data/trading_stock_list.csv'
suspendedStockListDir = 'data/suspended_stock_list.csv'
priceDir = 'price'
fundamentalFolders = ['balance_sheet', 'comprehensive_income', 'cash_flow']

def stockList():
    if os.path.isdir('data') == False:
        os.mkdir('data')


    # parse the trading stocks on the market
    print('start parsing trading stocks...', end='')
    sys.stdout.flush()
    getTradingStockList(tradingStockListDir)
    print('done')

    # parse the suspended stocks off the market
    print('start parsing suspended stocks...', end='')
    sys.stdout.flush()
    getSuspendedStockList(suspendedStockListDir)
    print('done')
    print('the files are added:')
    print(tradingStockListDir)
    print(suspendedStockListDir)

def loadAllStock(args):
    try:
        tradingStockDf = pd.read_csv(tradingStockListDir, parse_dates=True, index_col=[0])
        suspendedStockDf = pd.read_csv(suspendedStockListDir, parse_dates=True, index_col=[0])
    except:
        print('stock list is not exist!')
        exit(0)
    
    stocks = [str(stockID) for stockID in tradingStockDf['證券代號'].values   if len(str(stockID)) == 4] \
         + [str(stockID) for stockID in suspendedStockDf['證券代號'].values if len(str(stockID)) == 4]

    # check flag
    if(args.start != None):
        idx = stocks.index(args.start)
        if idx == -1:
            print('cannot find stock in the list: '+ args.start)
        stocks = stocks[idx:]
    return stocks
    # stocks contains trading stocks and suspended stocks to prevent survivorship bias

def fundamental2013(args):
    # get csv file of trading stocks and suspended stocks
    stocks = loadAllStock(args)
    
    # parse the fundamental data after 2013
    for d in fundamentalFolders:
        if os.path.isdir('data/' + d + '2013') == False:
            os.mkdir('data/' + d + '2013')

    for s in stocks:
        print('get fundamental 2013~ for stock ' + s)
        fileNames = ['data/' + f + '2013/s' + s + '.csv' for f in fundamentalFolders]
        getFundamental(s, fileNames)

def fundamental2012(args):

    # get csv file of trading stocks and suspended stocks
    stocks = loadAllStock(args)

    # parse the fundamental data before 2013
    for d in fundamentalFolders:
        if os.path.isdir('data/' + d + '2012') == False:
            os.mkdir('data/' + d + '2012')

    for s in stocks:
        print('get fundamental ~2012 for stock ' + s)
        fileNames = ['data/' + f + '2012/s' + s + '.csv' for f in fundamentalFolders]
        #getFundamental2012(s, fileNames[0], 'balance_sheet')
        #getFundamental2012(s, fileNames[1], 'comprehensive_income')
        getFundamental2012(s, fileNames[2], 'cash_flow')

def prices(args):

    if os.path.isdir('data/price') == False:
        os.mkdir('data/price')

    stocks = loadAllStock(args)
    for s in stocks:
        print('getting price for s' + s + '...')
        sys.stdout.flush()
        getPrice(s, 'data/price/s' + s)
        print('done')
     

operations = {
    "stocks_list" : stockList,
    "fundamental2013": fundamental2013,
    "fundamental2012": fundamental2012,
    "price": prices
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='This is a Python crawler program')
    parser.add_argument("crawler_data", help="stock_list, price, fundamental2013, fundamental2012")
    parser.add_argument("-s", "--start", help="start id to start")
    parser.add_argument("-d", "--delete",  action="store_true", default=False, help="delete the original data")
    args = parser.parse_args()

    operations[args.crawler_data](args)
    


    #if args.operation == 'initial':
