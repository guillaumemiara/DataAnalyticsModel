__author__ = 'guillaumemiara'

import sqlite3 as lite
import pandb as pdb

con = lite.connect("database.sqlite")

'''
Part 1: Data exploration
'''

#Retrieve tables names into a list
with con:
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

# Create a hash map of all tables called data
data = {}
for tab in tables:
    data[tab[0]] = pdb.createFrameFromTable(tab[0],con)

print "Tables and their columns"
for key in data:
    print '_'*50
    print "Table name: " + key
    print  data[key].columns.values

q_request = """
            SELECT *
            FROM requests
            """
req = pdb.createFrameFromQuery(q_request,con)


print req.count()

q_request2 = """
            SELECT *
            FROM invites
            """
inv = pdb.createFrameFromQuery(q_request2,con)

print inv.count()

q_request3 = """
            SELECT *
            FROM quotes
            """
quo = pdb.createFrameFromQuery(q_request3,con)

print quo.count()

q_request4 = """
            SELECT count(DISTINCT user_id)
            FROM requests
            """
quo = pdb.createFrameFromQuery(q_request4,con)

print quo