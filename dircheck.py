#! /usr/bin/env python
# -*- coding: iso-8859-15 -*-

# Copyright (C) 2011 by Esbjörn Dominique (esbjorn.dominique@gmail.com)
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# 

# NOTE: Requires the package python-mysqldb to be installed
#
# Create the database user, create the table and grant given permissions
#  CREATE USER 'dircheck'@'localhost' IDENTIFIED BY 'qwerty';
#  CREATE DATABASE dircheck;
#  GRANT SELECT,INSERT,UPDATE,CREATE ON dircheck.* TO 'dircheck'@'localhost';
#
# This script will automatically create the table if it doesn't exist.
# If you want to set it up manually, use:
#  CREATE TABLE dircheck.Files(name VARCHAR(25) PRIMARY KEY, mtime INT);
#

# Settings:

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
        cursor.execute("CREATE TABLE IF NOT EXISTS "+s['table']+"("+s['name']+" VARCHAR(25) PRIMARY KEY, "+s['mtime']+" INT)")
        return conn
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
def highest_db_mtime(conn, table_setting):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX("+table_setting['mtime']+") from "+table_setting['table'])
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
        cursor.execute("INSERT INTO "+s['table']+"("+s['name']+", "+s['mtime']+") VALUES('"+filename+"','"+mtime+"') \
                        ON DUPLICATE KEY UPDATE "+s['mtime']+"='"+mtime+"'")
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
