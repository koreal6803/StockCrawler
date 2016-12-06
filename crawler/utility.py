
import pandas as pd

def refineDf(df): # todo: move this function to utility

    # we only need the target season
    df = df.drop(df.columns[list(range(2, len(df.columns)))], axis=1)

    # remove the rows with null elements
    df = df.dropna()

    # transpose the table
    dft = df.T

    # set column names to the first column of df
    dft.columns = df[0]

    # remove the original column names
    dft = dft.drop(0, axis=0)
    
    return dft

def checkDuplicate(df): # todo: move this function to utility
    duplicateName = set()
    for idx, r in enumerate(df):
        for e in df.columns[idx+1:]:
            if(r == e):
                duplicateName.add(r)

    for r in duplicateName:
        print('**WARRN: duplicate column names')
        print(df[r])
        df = df.drop(r, axis=1)
    return df
