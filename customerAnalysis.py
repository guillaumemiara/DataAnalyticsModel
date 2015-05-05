__author__ = 'guillaumemiara'


import sqlite3 as lite
import pandb as pdb
import pandas as pd
import matplotlib.pyplot as plt

con = lite.connect("database.sqlite")

'''
Part 4: Data Analysis - Customers
'''

#Build a MetaDataframe for request analysis
#It would be a bad design for production ( query could be expensive)


q_request = """
            SELECT i.invite_id as inv_id,
                   i.user_id as service_provider_id,
                   r.user_id as customer_id,
                   i.sent_time as inv_sent_time,
                   q.sent_time as quo_sent_time,
                   r.request_id as req_id,
                   q.quote_id as quote_id,
                   c.name as category_name,
                   l.name as location_name,
                   (strftime('%s',q.sent_time) - strftime('%s',i.sent_time))/60 as duration_in_min,
                   (strftime('%s',q.sent_time) - strftime('%s',i.sent_time))/3600 as duration_in_hour
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

inv_by_cust = inv.groupby(['customer_id'])

print inv_by_cust.describe

print inv_by_cust['req_id','customer_id'].agg(['count','nunique'])

print inv_by_cust.count()



