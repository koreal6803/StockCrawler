import requests
import json
import datetime
import time
import pandas as pd


def getEconomic(path):
    r = requests.get('https://stock-ai.com/economicQuery.php?a=c&k=1&r=1&cCountry=tw&cType=10&hash=d41d8cd98f00b204e9800998ecf8427e&_=1481124674278')
    itemList = json.loads(r.text)['data']

    for idx, l in enumerate(itemList):
        time.sleep(5) 
        linkStart = l['r5'].index('eom-1-')
        symbolCode = l['r5'][linkStart:].split('\'')[0].split('-')[2]
        print(str(idx) + '/' + str(len(itemList)) + ':' + symbolCode)

        r = requests.post('https://stock-ai.com/eomDataQuery.php',data={\
            'a': 'c',
            'showType': 'Value',
            'symbolCode': symbolCode,
            'startYear': '2000',
            'startMonth': '01',
            'endYear': datetime.datetime.now().year,
            'endMonth': datetime.datetime.now().month,
            'hash':'d41d8cd98f00b204e9800998ecf8427e'})
        #return r
        df = pd.DataFrame()
        data = json.loads(r.text)
        fname = data['eName'].replace(' ','') + '_' + data['sName'] + '_' + data['sUnit'] + '.csv'
        for r in data['rows']:
            obj = {key: [value] for key, value in r.items()}
            df = df.append(pd.DataFrame(obj))
        df['sDate'] = pd.to_datetime(df['sDate'])
        df = df.set_index('sDate')
        df.to_csv(path + '/' + fname)
        print(fname)



        

