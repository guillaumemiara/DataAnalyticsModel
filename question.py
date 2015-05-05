__author__ = 'guillaumemiara'

'''
Let's focus on the question:
You should examine the trends in quote rates over time.
Is there evidence that product changes over the last two months have caused site-wide shifts in quoting behavior?
'''


import sqlite3 as lite
import pandb as pdb
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

con = lite.connect("database.sqlite")

'''
Part 5: question
'''

#Build a MetaDataframe for request analysis
#It would be a bad design for production ( query could be expensive)


q_request = """
            SELECT i.invite_id as inv_id,
                   i.user_id as service_provider_id,
                   r.user_id as customer_id,
                   r.creation_time as req_creation_time,
                   i.sent_time as inv_sent_time,
                   q.sent_time as quo_sent_time,
                   r.request_id as req_id,
                   q.quote_id as quote_id,
                   c.category_id as category_id,
                   c.name as category_name,
                   l.name as location_name,
                   l.location_id as location_id,
                   (strftime('%s',q.sent_time) - strftime('%s',i.sent_time))/60 as elapsed_qtoi_min,
                   (strftime('%s',q.sent_time) - strftime('%s',i.sent_time))/3600 as elapsed_qtoi_hour,
                   (strftime('%s',i.sent_time) - strftime('%s',r.creation_time))/60 as elapsed_itor_min,
                   (strftime('%s',i.sent_time) - strftime('%s',r.creation_time))/3600 as elapsed_itor_hour
                   FROM requests r
                    LEFT OUTER JOIN invites i
                        ON i.request_id = r.request_id
                    LEFT OUTER JOIN quotes q
                        ON i.invite_id = q.invite_id
                    LEFT OUTER JOIN categories c
                        ON r.category_id = c.category_id
                    LEFT OUTER JOIN locations l
                        ON r.location_id = l.location_id
            """

inv = pdb.createFrameFromQuery(q_request,con)

inv['converted'] = 1*inv['quo_sent_time'].isnull()
print "Let's see the duration of the dataset"
print "Earliest date is :" + inv.sort('req_creation_time')['req_creation_time'].min()
print "Latest date is: " + inv.sort('req_creation_time')['req_creation_time'].max()

inv['creation_time'] = pd.to_datetime(inv['req_creation_time'])
inv['inv_sent_time'] = pd.to_datetime(inv['inv_sent_time'])
inv['quo_sent_time'] = pd.to_datetime(inv['quo_sent_time'])
inv['week'] = pd.DatetimeIndex(inv['req_creation_time']).week
inv['date'] = pd.DatetimeIndex(inv['req_creation_time']).date
inv['inv_hour'] = pd.DatetimeIndex(inv['inv_sent_time']).hour

'''
Analysis of the evolution of total number of quotes, invites, requests per week
'''
temp_quote_by_week = inv.groupby(['week']).agg(['count','nunique'])[['req_id','inv_id','quote_id']]

tot_quote_by_week = pd.DataFrame()
tot_quote_by_week['requests']= temp_quote_by_week['req_id']['nunique']
tot_quote_by_week['invites']= temp_quote_by_week['inv_id']['count']
tot_quote_by_week['quotes']= temp_quote_by_week['quote_id']['count']

tot_quote_by_week.plot(title = ' Total number of quotes,invites, requests, per week ')

'''
Analysis of the evolution of total number of quotes, invites, requests per day
'''
temp_quote_by_day = inv.groupby(['date']).agg(['count','nunique'])[['req_id','inv_id','quote_id']]

tot_quote_by_day = pd.DataFrame()
tot_quote_by_day['requests']= temp_quote_by_day['req_id']['nunique']
tot_quote_by_day['invites']= temp_quote_by_day['inv_id']['count']
tot_quote_by_day['quotes']= temp_quote_by_day['quote_id']['count']

tot_quote_by_day.plot( title = ' Total number of quotes,invites, requests per day')

'''
Analysis of the evolution of total number of quotes, invites, requests per time of invite sent
'''
temp_quote_by_hour = inv.groupby(['inv_hour']).agg(['count','nunique'])[['req_id','inv_id','quote_id']]

tot_quote_by_hour = pd.DataFrame()
tot_quote_by_hour['requests']= temp_quote_by_hour['req_id']['nunique']
tot_quote_by_hour['invites']= temp_quote_by_hour['inv_id']['nunique']
tot_quote_by_hour['quotes']= temp_quote_by_hour['quote_id']['nunique']

print temp_quote_by_hour
print "Check this out"
print tot_quote_by_hour['requests'].cumsum()



tot_quote_by_hour.plot(title = 'Total number of quotes, invite, requests per invite sending time')

'''
Analysis of the evolution of delay beetween req creation and invite sent
'''
elapse_time = inv.groupby(['date']).agg(['mean','std'])[['elapsed_itor_min','elapsed_qtoi_min']]

elapse_time.plot( title = ' Analysis of delays beetween invite sending/ request creation & quote sending/ invite')

'''
Analysis of the behavior of customers and service providers over time
'''
# Get only the numbers for a certain number of relevant users

by_week_prov = inv.groupby(['week']).agg(['mean','std'])


'''
Invite to quote rate by categories over time

'''
#TODO Review this part

conv_by_week = inv.groupby('week')['converted'].count()

inv_bycat_week = inv.groupby(['week','category_id'])['inv_id','quote_id'].count()
inv_bycat_week['answer_ratio'] = inv_bycat_week['quote_id']/inv_bycat_week['inv_id']

inv_bysrvprov = inv.groupby(['date','category_id'])['inv_id','quote_id'].count()
inv_bysrvprov['answer_ratio'] = inv_bysrvprov['quote_id']/inv_bysrvprov['inv_id']

#inv_bycat_week.plot(kind='hexbin', x='week', y='category_id', c='answer_ratio', gridsize=50)


'''
Analysis of inv to quote rate trend for top 10 service providers ( # quotes sent)
'''

top_servprov = inv.groupby(['service_provider_id']).count('quote_id').sort('converted', ascending = False).head(10)

fig, axes = plt.subplots(nrows=10)

n = 0
for i in top_servprov.index:
    provider_id = top_servprov.ix[i].name
    frame = pd.DataFrame(inv.groupby('service_provider_id').get_group(provider_id))
    frame1 = frame.groupby('date')['req_id','inv_id','quote_id'].agg(['nunique'])
    #print frame1.count()
    frame1.plot(ax=axes[n])
    n = n+1

'''
Analysis of inv to quote rate trend for top 10 categories providers ( # quotes sent)
'''

top_cat = inv.groupby(['category_id']).count('quote_id').sort('converted', ascending = False).head(10)

fig, axes = plt.subplots(nrows=10)

n = 0
for i in top_cat.index:
    cat_id = top_cat.ix[i].name
    frame = pd.DataFrame(inv.groupby('category_id').get_group(cat_id))
    frame1 = frame.groupby('date')['req_id','inv_id','quote_id'].agg(['nunique'])
    #print frame1.count()
    frame1.plot(ax=axes[n])
    n = n+1

'''
Analysis of inv to quote rate trend for top 10 locations ( # quotes sent)
'''

top_loc = inv.groupby(['location_id']).count('quote_id').sort('converted', ascending = False).head(10)

fig, axes = plt.subplots(nrows=10)

n = 0
for i in top_loc.index:
    loc_id = top_loc.ix[i].name
    frame = pd.DataFrame(inv.groupby('location_id').get_group(loc_id))
    frame1 = frame.groupby('date')['req_id','inv_id','quote_id'].agg(['nunique'])
    #print frame1.count()
    frame1.plot(ax=axes[n])
    n = n+1


'''
Analysis of inv to quote rate trend for top 10 categories providers ( # requests sent)
'''

top_cat = inv.groupby(['category_id'])['req_id'].agg(['nunique']).sort('nunique',ascending = False).head(10)
print top_cat

fig, axes = plt.subplots(nrows=10)

n = 0
for i in top_cat.index:
    cat_id = top_cat.ix[i].name
    frame = pd.DataFrame(inv.groupby('category_id').get_group(cat_id))
    frame1 = frame.groupby('date')['req_id','inv_id','quote_id'].agg(['nunique'])
    #print frame1.count()
    frame1.plot(ax=axes[n])
    n = n+1

fig, axes = plt.subplots(nrows=10)
'''
Analysis of inv to quote rate trend for top 10 locations ( # requests sent)
'''

top_loc = inv.groupby(['location_id'])['req_id'].agg(['nunique']).sort('nunique', ascending = False).head(10)


n = 0
for i in top_loc.index:
    loc_id = top_loc.ix[i].name
    frame = pd.DataFrame(inv.groupby('location_id').get_group(loc_id))
    frame1 = frame.groupby('date')['req_id','inv_id','quote_id'].agg(['nunique'])
    #print frame1.count()
    frame1.plot(ax=axes[n])
    n = n+1


'''
plotting
'''
plt.show()