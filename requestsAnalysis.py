__author__ = 'guillaumemiara'

import sqlite3 as lite
import pandb as pdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

con = lite.connect("database.sqlite")

'''
Part 2: Data Analysis - Requests
'''

#Build a MetaDataframe for request analysis
#It would be a bad design for production ( query could be expensive)


q_request = """
            SELECT i.invite_id as inv_id,
                   i.sent_time as inv_sent_time,
                   q.sent_time as quo_sent_time,
                   i.request_id as req_id,
                   q.quote_id as quote_id,
                   c.name as category_name,
                   l.name as location_name,
                   (strftime('%s',q.sent_time) - strftime('%s',i.sent_time))/60 as duration_in_min,
                   (strftime('%s',q.sent_time) - strftime('%s',i.sent_time))/3600 as duration_in_hour
                   FROM invites i
                    LEFT OUTER JOIN quotes q
                        ON i.invite_id = q.invite_id
                    LEFT OUTER JOIN requests r
                        ON i.request_id = r.request_id
                    LEFT OUTER JOIN categories c
                        ON r.category_id = c.category_id
                    LEFT OUTER JOIN locations l
                        ON r.location_id = l.location_id
            """

inv = pdb.createFrameFromQuery(q_request,con)

#Adding category columns to describe location and category ( natural data type here)
inv["cat_category_name"] = inv["category_name"].astype('category')
inv["cat_location_name"] = inv["location_name"].astype('category')

print 50*'_'
print "High level statistics on the requests for quotes"
print inv.describe()
print 50*'_'

print "High levels statistic on the category name"
print inv["cat_category_name"].describe()
print 50*'_'

print "High level statistics on the location"
print inv["cat_location_name"].describe()


print 50*'_'
print 50*'_'

'''
Answer to the questions for requests
'''

'''
Question: What's the average quote invites / quotes ratio
A. High level
- per location
- per catergory
'''

inv_byloc = inv.groupby('location_name')
inv_bycat = inv.groupby('category_name')

# By categories Group by categories and count the invites sent and quotes received
df_inv_bycat = pd.DataFrame(inv_bycat['inv_id','quote_id'].agg(['count']))
df_inv_bycat['answer_rate'] = df_inv_bycat['quote_id']/df_inv_bycat['inv_id']
df_inv_bycat = df_inv_bycat.sort('answer_rate')

print "High level statistics on the invites by categories"
print df_inv_bycat.describe()
print 50*'_'

# By locations Group by location and count the invites sent and quotes received, ratio
df_inv_byloc = pd.DataFrame(inv_byloc['inv_id','quote_id'].agg(['count']))
df_inv_byloc['answer_rate'] = df_inv_byloc['quote_id']/df_inv_byloc['inv_id']
df_inv_byloc = df_inv_byloc.sort('answer_rate')

print "High level statistics on the invites by locations"
print df_inv_byloc.describe()
print 50*'_'


df_inv_byloc_count =  inv.groupby('location_name')['inv_id'].agg(['count'])
df_inv_bycat_count =  inv.groupby('category_name')['inv_id'].agg(['count'])

df_inv_byloc_sorted_top = df_inv_byloc_count.sort('count', ascending = False).head(20)
df_inv_byloc_sorted_bottom = df_inv_byloc_count.sort('count', ascending = True).head(20)
df_inv_bycat_sorted_top = df_inv_bycat_count.sort('count', ascending = False).head(20)
df_inv_bycat_sorted_bottom = df_inv_bycat_count.sort('count', ascending = True).head(20)

print "Top 20 cities with most invites"
print df_inv_byloc_sorted_top
print 50*'_'

print "Top 20 cities with least invites"
print df_inv_byloc_sorted_bottom
print 50*'_'

print "Top 20 categories with most invites"
print df_inv_bycat_sorted_top
print 50*'_'

print "Top 20 categories with least invites"
print df_inv_bycat_sorted_bottom
print 50*'_'

'''
Further Analysis post results
'''

print df_inv_bycat.sort('answer_rate', ascending = True).head(30)


print "Return the standard deviation of the categories answer ratio in view of the location"

print "STD deviation for categories with low answer ratio rate"
for i in df_inv_bycat.sort('answer_rate', ascending = True).head(30).index:
    iname =  df_inv_bycat.sort('answer_rate', ascending = True).head(30).ix[i].name
    frame = pd.DataFrame(inv_bycat.get_group(iname))
    frame1 = pd.DataFrame( frame.groupby('location_name').count(['inv_id','quote_id']).sort('inv_id', ascending = False))
    frame1['answer_ratio'] = frame1['quote_id'] / frame1['inv_id']
    print "Standard deviation  for category %s is %f and the ratio mean is %f" % (iname, frame1['answer_ratio'].std(), frame1['answer_ratio'].mean())

print 50*'_'

print "STD deviation for categories with high answer ratio rate"
for i in df_inv_bycat.sort('answer_rate', ascending = False).head(30).index:
    iname =  df_inv_bycat.sort('answer_rate', ascending = False).head(30).ix[i].name
    frame = pd.DataFrame(inv_bycat.get_group(iname))
    frame1 = pd.DataFrame( frame.groupby('location_name').count(['inv_id','quote_id']).sort('inv_id', ascending = False))
    frame1['answer_ratio'] = frame1['quote_id'] / frame1['inv_id']
    print "Standard deviation  for category %s is %f and the ratio mean is %f" % (iname, frame1['answer_ratio'].std(), frame1['answer_ratio'].mean())

for i in df_inv_bycat_sorted_top.index:
    iname =  df_inv_bycat_sorted_top.ix[i].name
    frame = pd.DataFrame(inv_bycat.get_group(iname))
    frame1 = pd.DataFrame( frame.groupby('location_name').count(['inv_id','quote_id']).sort('inv_id', ascending = False))
    frame1['answer_ratio'] = frame1['quote_id'] / frame1['inv_id']
    print "Standard deviation  for category %s is %f and ratio mean is %f " % (iname, frame1['answer_ratio'].std(), frame1['answer_ratio'].mean())

# TODO : Rework function in pdb
#print pdb.getTopX(df_inv_byloc_count.sort('count', ascending = False),80)

#Plot


df_inv_bycat.plot(kind = 'bar', y = 'answer_rate')
df_inv_byloc.sort('answer_rate').plot(kind = 'bar', y = 'answer_rate')

plt.show()
