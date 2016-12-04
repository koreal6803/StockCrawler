#!/usr/bin/python
import argparse
import sys
import os
import pandas as pd
from crawler.getStockList import getTradingStockList
from crawler.getStockList import getSuspendedStockList
from crawler.getFundamental2013 import getFundamental
from crawler.getFundamental2012 import getBalanceSheet, getComprehensiveIncome, getCashFlow


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("operation", help="initial or update")
    args = parser.parse_args()

    if args.operation == 'initial':

        if os.path.isdir('data') == False:
            os.mkdir('data')

        tradingStockListDir = 'data/trading_stock_list.csv'
        suspendedStockListDir = 'data/suspended_stock_list.csv'

        '''

        #
        # fetching these files is slow, turn off if the files are already exist during testing
        #

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

        '''

        # get csv file of trading stocks and suspended stocks
        tradingStockDf = pd.read_csv(tradingStockListDir, parse_dates=True, index_col=[0])
        suspendedStockDf = pd.read_csv(suspendedStockListDir, parse_dates=True, index_col=[0])
        
        # stocks contains trading stocks and suspended stocks to prevent survivorship bias
        stocks = [str(stockID) for stockID in tradingStockDf['證券代號'].values   if len(str(stockID)) == 4] \
               + [str(stockID) for stockID in suspendedStockDf['證券代號'].values if len(str(stockID)) == 4]


        # parse fundamentals 
        folders = ['balance_sheet', 'comprehensive_income', 'cash_flow']
        
        # parse the fundamental data after 2013
        for d in folders:
            if os.path.isdir('data/' + d + '2013') == False:
                os.mkdir('data/' + d + '2013')

        for s in stocks:
            print('get fundamental 2013~ for stock ' + s)
            fileNames = ['data/' + f + '2013/s' + s + '.csv' for f in folders]
            getFundamental(s, fileNames)

        # parse the fundamental data before 2013
        for d in folders:
            if os.path.isdir('data/' + d + '2012') == False:
                os.mkdir('data/' + d + '2012')

        for s in stocks:
            print('get fundamental ~2012 for stock ' + s)
            fileNames = ['data/' + f + '2012/s' + s + '.csv' for f in folders]
            getBalanceSheet(s, fileNames[0])
            getComprehensiveIncome(s, fileNames[1])
            getCashFlow(s, fileNames[2])




