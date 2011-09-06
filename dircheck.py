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

class FileTable:
    _conn = None
    _cursor = None
    _table_setting = None

    def __init__(self, credentials, table_setting):
        self._table_setting = table_setting
        self._open_table(credentials)

    def __del__(self):
        self._cursor.close()
        self._conn.close()

    def _open_table(self, credentials):
        try:
            c = credentials
            self._conn =  mdb.connect(c['host'], c['user'], c['password'], c['database'])
            self._cursor = self._conn.cursor()
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            sys.exit(1)

    def create_table(self):
        warnings.filterwarnings("ignore", "Table .* already exists")
        s = self._table_setting
        self._cursor.execute("CREATE TABLE IF NOT EXISTS %s(%s VARCHAR(25) PRIMARY KEY, %s INT)" % (s['table'], s['name'], s['mtime']))

    def highest_db_mtime(self):
        self._cursor.execute("SELECT MAX(%s) from %s" % (self._table_setting['mtime'], self._table_setting['table']))
        result = self._cursor.fetchone()[0]
        if result == None:
            return 0
        else:
            return result

    def write_to_table(self, filenames_mtimes):
        s = self._table_setting
        for filename in filenames_mtimes.keys():
            mtime = str(filenames_mtimes[filename])
            self._cursor.execute("INSERT INTO %s(%s, %s) VALUES('%s','%s') ON DUPLICATE KEY UPDATE %s=%s" %
                             (s['table'], s['name'], s['mtime'], filename, mtime, s['mtime'], mtime))
        self._conn.commit()

def main():
    file_table = FileTable(db_credentials, db_table_settings)
    highest_current_mtime = file_table.highest_db_mtime()
    files_mtimes = get_files_mtimes(path, highest_current_mtime)
    print "New files: "+str(format_mtimes(files_mtimes))
    file_table.write_to_table(files_mtimes)

if __name__ == "__main__":
    main()
