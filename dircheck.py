#! /usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys
import os
import datetime
import MySQLdb as mdb
import warnings

path = "/home/esbjorn/program/python"
db_credentials = {'host': 'localhost',
                  'user': 'dircheck',
                  'password': 'qwerty',
                  'database': 'dircheck'}

def get_files_mtimes (path, min_mtime):
    result = {}
    for file in os.listdir(path):
        mtime = int(os.stat(file).st_mtime)
        if mtime >= min_mtime:
            result[file] = mtime
    return result

def format_mtimes (filenames_mtimes):
    result = {}
    for f in filenames_mtimes.keys():
        result[f] = str(datetime.datetime.fromtimestamp(int(filenames_mtimes[f])))
    return result

def open_table(credentials):
    try:
        c = credentials
        conn =  mdb.connect(c['host'], c['user'], c['password'], c['database'])
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS Files(name VARCHAR(25) PRIMARY KEY, mtime DOUBLE)")
        return conn
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
def highest_db_mtime(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(mtime) from Files")
    result = cursor.fetchone()[0]
    cursor.close()
    if result == None:
        return 0
    else:
        return result

def write_to_table(conn, filenames_mtimes):
    cursor = conn.cursor()
    for filename in filenames_mtimes.keys():
        mtime = str(filenames_mtimes[filename])
        cursor.execute("INSERT INTO Files(name, mtime) VALUES('"+filename+"','"+mtime+"') \
                            ON DUPLICATE KEY UPDATE mtime='"+mtime+"'")
    conn.commit()
    cursor.close()


warnings.filterwarnings("ignore", "Table .* already exists")

conn = open_table(db_credentials)
highest_current_mtime = highest_db_mtime(conn)
files_mtimes = get_files_mtimes(path, highest_current_mtime)
print "New files: "+str(format_mtimes(files_mtimes))
write_to_table(conn, files_mtimes)
conn.close()
