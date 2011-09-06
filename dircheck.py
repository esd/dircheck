#! /usr/bin/env python
# -*- coding: iso-8859-15 -*-

# See REAME file for usage instructions

path = "/"
db_credentials = {'host': 'localhost',
                  'user': 'dircheck',
                  'password': 'qwerty',
                  'database': 'dircheck'}
db_table_settings = {'table': 'Files',
                     'name': 'name',   # VARCHAR(25)
                     'mtime': 'mtime'} # INT

################################################################################

import sys
import os
import datetime
import MySQLdb as mdb
import warnings

def get_files_mtimes (path, min_mtime):
    result = {}
    for file in os.listdir(path):
        mtime = int(os.stat(path+"/"+file).st_mtime)
        if mtime > min_mtime:
            result[file] = mtime
    return result

def format_mtimes (filenames_mtimes):
    result = {}
    for f in filenames_mtimes.keys():
        result[f] = str(datetime.datetime.fromtimestamp(int(filenames_mtimes[f])))
    return result

def open_table(credentials, table_setting):
    try:
        c = credentials
        s = table_setting
        conn =  mdb.connect(c['host'], c['user'], c['password'], c['database'])
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS %s(%s VARCHAR(25) PRIMARY KEY, %s INT)" % (s['table'], s['name'], s['mtime']))
        return conn
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
def highest_db_mtime(conn, table_setting):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(%s) from %s" % (table_setting['mtime'], table_setting['table']))
    result = cursor.fetchone()[0]
    cursor.close()
    if result == None:
        return 0
    else:
        return result

def write_to_table(conn, table_setting, filenames_mtimes):
    cursor = conn.cursor()
    s = table_setting
    for filename in filenames_mtimes.keys():
        mtime = str(filenames_mtimes[filename])
        cursor.execute("INSERT INTO %s(%s, %s) VALUES('%s','%s') ON DUPLICATE KEY UPDATE %s=%s" %
                       (s['table'], s['name'], s['mtime'], filename, mtime, s['mtime'], mtime))
    conn.commit()
    cursor.close()

def main():
    warnings.filterwarnings("ignore", "Table .* already exists")
    conn = open_table(db_credentials, db_table_settings)
    highest_current_mtime = highest_db_mtime(conn, db_table_settings)
    files_mtimes = get_files_mtimes(path, highest_current_mtime)
    print "New files: "+str(format_mtimes(files_mtimes))
    write_to_table(conn, db_table_settings, files_mtimes)
    conn.close()

if __name__ == "__main__":
    main()
