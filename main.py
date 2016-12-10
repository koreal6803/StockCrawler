#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import sys
import os
import pandas as pd
import time
import random
from crawler.getStockList import getTradingStockList
from crawler.getStockList import getSuspendedStockList
from crawler.getFundamental2013 import getFundamental
from crawler.getFundamental2012 import getFundamental2012
from crawler.getPrice import getPrice, updatePrice
from crawler.getEconomic import getEconomic
from crawler.getMonthRevenue import refineMonthRevenue, getMonthRevenue
from crawler.getSimpleStatement import refineSimpleStatement, getSimpleStatement, splitDataToStocks

tradingStockListDir = 'data/trading_stock_list.csv'
suspendedStockListDir = 'data/suspended_stock_list.csv'
priceDir = 'data/price'
economicDir = 'data/economic'
monthRevenueDir = 'data/month_revenue'
simpleFundamentalDir = 'data/fundamental_simple'
fundamentalFolders = ['balance_sheet', 'comprehensive_income', 'cash_flow']

def stockList(args):
    if os.path.isdir('data') == False:
        os.mkdir('data')


    # parse the trading stocks on the market
    print('start parsing trading stocks...', end='')
    sys.stdout.flush()
    #getTradingStockList(tradingStockListDir)
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
    
    # makd dir
    if os.path.isdir('data/fundamental2013') == False:
        os.mkdir('data/fundamental2013')

    # get csv file of trading stocks and suspended stocks
    stocks = loadAllStock(args)
    
    # parse the fundamental data after 2013
    for d in fundamentalFolders:
        if os.path.isdir('data/fundamental2013/' + d) == False:
            os.mkdir('data/fundamental2013/' + d)

    for s in stocks:
        print('get fundamental 2013~ for stock ' + s)
        fileNames = ['data/fundamental2013/' + f + '/s' + s + '.csv' for f in fundamentalFolders]
        getFundamental(s, fileNames)

def fundamental2012(args):
    
    # make dir
    if os.path.isdir('data/fundamental2012') == False:
        os.mkdir('data/fundamental2012')

    # get csv file of trading stocks and suspended stocks
    stocks = loadAllStock(args)

    # parse the fundamental data before 2013
    for d in fundamentalFolders:
        if os.path.isdir('data/fundamental2012/' + d ) == False:
            os.mkdir('data/fundamental2012/' + d )

    for s in stocks:
        print('get fundamental ~2012 for stock ' + s)
        fileNames = ['data/fundamental2012/' + f + '/s' + s + '.csv' for f in fundamentalFolders]
        #getFundamental2012(s, fileNames[0], 'balance_sheet')
        #getFundamental2012(s, fileNames[1], 'comprehensive_income')
        getFundamental2012(s, fileNames[2], 'cash_flow')

def prices(args):

    if os.path.isdir('data/price') == False:
        os.mkdir('data/price')

    stocks = loadAllStock(args)
    for s in stocks:
        if args.build == False:
            print('updating price for s' + s + '...')
            try:
                success = updatePrice(s, priceDir + '/s' + s + '.csv')
            except:
                print('strange format...rebuild the stock')
                os.remove(priceDir + '/s' + s + '.csv')
                time.sleep(10)
                getPrice(s, priceDir + '/s' + s + '.csv')
                success = True
        else:
            print('getting price for s' + s + '...')
            getPrice(s, priceDir + '/s' + s + '.csv')
        print('done')

        # count down for waiting the next query
        if success:
            wait = random.randint(10,10)
            for i in range(wait):
                print(int(i/wait*100),end=' ')
                sys.stdout.flush()
                time.sleep(1)
            print()

     
def economic(args):
    getEconomic(economicDir)

def monthRevenue(args):
    getMonthRevenue(monthRevenueDir)
    refineMonthRevenue(monthRevenueDir)

def simpleFundamental(args):
    getSimpleStatement(simpleFundamentalDir)
    refineSimpleStatement(simpleFundamentalDir)
    splitDataToStocks(simpleFundamentalDir)

operations = {
    "stock_list" : stockList,
    "price": prices,
    "economic": economic,
    "month_revenue": monthRevenue,
    "fundamental_simple": simpleFundamental,
    "fundamental2013": fundamental2013,
    "fundamental2012": fundamental2012
}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='This is a Python crawler program')

    helpMsg = """select the section of data to update:\n
                 1. stock_list\n
                 2. price\n
                 3. fundamental_simple\n
                 4. month_revenue\n
                 5. economic\n
                 6. fundamental2013\n
                 7. fundamental2012(not ready yet)"""
    parser.add_argument("crawler_data", help=helpMsg)
    parser.add_argument("-s", "--start", help="stock id to start")
    parser.add_argument("-b", "--build", dest='build', help="complete refinement")
    parser.add_argument("-u", "--update", dest='update', help="only small update for extra amount data (faster)")

    parser.set_defaults(build=False)
    parser.set_defaults(update=True)

    args = parser.parse_args()

    operations[args.crawler_data](args)
