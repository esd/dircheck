#! /usr/bin/env python
# -*- coding: iso-8859-15 -*-

# See REAME file for usage instructions

path = "."
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

class FilesMtimes():
    _files_mtimes = {}

    def _clear(self):
        self._files_mtimes = {}

    def from_path(self, path):
        self._clear()
        for file in os.listdir(path):
            mtime = int(os.stat(path+"/"+file).st_mtime)
            self._files_mtimes[file] = mtime
        return self

    def from_tuples(self, tuples):
        self._clear()
        for (name, mtime) in tuples:
            self._files_mtimes[name] = int(mtime)
        return self

    def from_dict(self, dict):
        self._clear()
        self._files_mtimes = dict
        return self

    def dict(self):
        return self._files_mtimes

    def keys(self):
        return self._files_mtimes.keys()

    def mtime(self, name):
        return self._files_mtimes[name]

    def format(self):
        result = {}
        for filename in self.dict():
            mtime = self._files_mtimes[filename]
            result[filename] = str(datetime.datetime.fromtimestamp(mtime))
        return str(result)

    def newer_than(self, mtime):
        result = {}
        for filename in self.dict():
            tmp_mtime = self._files_mtimes[filename]
            if tmp_mtime > mtime:
                result[filename] = tmp_mtime
        return FilesMtimes().from_dict(result)

    ## Return the common elements (with common keys)
    def intersection(self, files_mtimes):
        intersect = []
        for item in self._files_mtimes.keys():
            if files_mtimes.has_key(item):
                intersect.append(item)
        return FilesMtimes().from_dict(intersect)                

    ## Find the elements that were in given set, but are not in this object
    def deleted(self, old_files_mtimes):
        result = {}
        for item in old_files_mtimes.keys():
            if not self._files_mtimes.has_key(item):
                result[item] = old_files_mtimes.dict()[item]
        return FilesMtimes().from_dict(result)                
        

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

    def insert(self, files_mtimes):
        s = self._table_setting
        for filename in files_mtimes.keys():
            mtime = str(files_mtimes.mtime(filename))
            self._cursor.execute("INSERT INTO %s(%s, %s) VALUES('%s','%s')" %
                                 (s['table'], s['name'], s['mtime'], filename, mtime))
        self._conn.commit()

    def delete(self, files_mtimes):
        s = self._table_setting
        for filename in files_mtimes.keys():
            self._cursor.execute("DELETE FROM %s WHERE %s='%s'" %
                                 (s['table'], s['name'], filename))
        self._conn.commit()
        
    def get_all(self):
        s = self._table_setting
        self._cursor.execute("SELECT %s, %s FROM %s" %
                             (s['name'], s['mtime'], s['table']))
        result = self._cursor.fetchall()
        if result == None:
            return ()
        else:
            return result

    # current_files_mtimes is supposed to be the current filesystem data
    # This method updates the current db table
    def update(self, current_files_mtimes):
        db_files_mtimes = FilesMtimes().from_tuples(self.get_all())
        deleted_files_mtimes = current_files_mtimes.deleted(db_files_mtimes)
        self.delete(deleted_files_mtimes)
        updated_files_mtimes = current_files_mtimes.newer_than(self.highest_db_mtime())
        self.delete(updated_files_mtimes)
        self.insert(updated_files_mtimes)

def main():
    current = FilesMtimes().from_path(path)
    FileTable(db_credentials, db_table_settings).update(current)

if __name__ == "__main__":
    main()
