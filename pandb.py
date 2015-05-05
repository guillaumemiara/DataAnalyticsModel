__author__ = 'guillaumemiara'

import pandas as pd

def createFrameFromTable(tbname,con):
    query = "SELECT * from "+ tbname
    return pd.read_sql(query, con)

def createFrameFromQuery(query,con):
    return pd.read_sql(query, con)

#TODO: Fix function ( need to add integer index)

def getTopX(frame,X):
    '''
    :param frame: a sorted frame
    :param X: % of list we want to retrieve ( integer in range [0,100])
    :return: the top X % of the frame
    '''
    i = 1
    t = True
    ixmax = frame.cumsum().max()
    while t:
        t = (frame.cumsum().ix(i) % frame.cumsum().ix(ixmax) < X )
        i = i+1
    return frame.head(i)
