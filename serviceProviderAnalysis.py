__author__ = 'guillaumemiara'

import sqlite3 as lite
import pandb as pdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

con = lite.connect("database.sqlite")

'''
Part 3: Data Analysis - Service Provider
'''

#Build a MetaDataframe for request analysis
#It would be a bad design for production ( query could be expensive)


q_request = """
            SELECT i.invite_id as inv_id,
                   i.user_id as service_provider_id,
                   r.user_id as customer_id,
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

inv_bysrvprov = inv.groupby(['service_provider_id'])['inv_id','quote_id'].count()
inv_bysrvprov['answer_ratio'] = inv_bysrvprov['quote_id']/inv_bysrvprov['inv_id']

print "High level description of the service providers behavior"
print inv_bysrvprov.describe()
print 50*'_'

# The service providers who receives the most invites
top_servprov_inv =  inv_bysrvprov.sort('inv_id',ascending = False).head(100)

print "Description of the 100 service providers who receive the most invites"
print top_servprov_inv.describe()
print 50*'_'

# The serive providers who send the most quotes
top_servprov_q =  inv_bysrvprov.sort('quote_id',ascending = False).head(100)
print top_servprov_q.describe()
print 50*'_'


# The serive providers who shave the highest answering rates
top_servprov_a =  inv_bysrvprov.sort('answer_ratio',ascending = False).head(100)
print top_servprov_a.describe()
print 50*'_'

c1=top_servprov_inv['answer_ratio']*200
c2=top_servprov_q['answer_ratio']*200
c3=top_servprov_a['answer_ratio']*200

title1= "The top 100 service providers by total number of invites to quote received"
title2= "The top 100 service providers by total number of quotes emitted"
title3= "The top 100 service providers by answer ratio"

#What are the categories with the top service providers?

inv_bysrvprov_cat = inv.groupby(['service_provider_id','category_name'])['inv_id','quote_id'].agg(['count' ])
inv_bysrvprov_cat['answer_ratio'] = inv_bysrvprov_cat['quote_id']/inv_bysrvprov_cat['inv_id']

#Let's look at the top 20 categories among the service providers that are the top quotes sender
frame = pd.DataFrame(inv_bysrvprov_cat)
frame1 =  pd.DataFrame(frame['quote_id']['count'])
frame2 =  pd.DataFrame(frame1.sort('count',ascending=False).head(100))
frame2.reset_index(inplace=True)
frame3 = frame2.groupby('category_name').count('category_name').sort('count', ascending = False).head(20)

print "These are the top 20 categories for the service providers sending out most quotes"
print frame3


#Plotting

top_servprov_inv.plot(kind='scatter', x ='inv_id',  y='quote_id', c=c1, s=100, title = title1)
top_servprov_q.plot(kind='scatter', x ='inv_id',  y='quote_id', c=c2, s=100, title = title2)
top_servprov_a.plot(kind='scatter', x ='inv_id',  y='quote_id', c=c3, s=100, title=title3)
frame3.plot(kind='bar',y='count', title = 'Top categories among service providers sending most quotes')
plt.show()

