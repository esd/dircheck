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
    __files_mtimes = {}

    def _clear(self):
        self.__files_mtimes = {}

    def from_path(self, path):
        self._clear()
        for file in os.listdir(path):
            mtime = int(os.stat(path+"/"+file).st_mtime)
            self.__files_mtimes[file] = mtime
        return self

    def from_tuples(self, tuples):
        self._clear()
        for (name, mtime) in tuples:
            self.__files_mtimes[name] = int(mtime)
        return self

    def from_dict(self, dict):
        self._clear()
        self.__files_mtimes = dict
        return self

    def dict(self):
        return self.__files_mtimes

    def keys(self):
        return self.__files_mtimes.keys()

    def has_key(self, key):
        return self.__files_mtimes.has_key(key)

    def mtime(self, name):
        return self.__files_mtimes[name]

    def format(self):
        result = {}
        for filename in self.dict():
            mtime = self.__files_mtimes[filename]
            result[filename] = str(datetime.datetime.fromtimestamp(mtime))
        return str(result)

    # Return the ones in the current set that have updated their mtime
    # since given files_mtimes.
    # NOTE: Does not handle the deleted or new ones
    def updated(self, old_files_mtimes):
        result = {}
        for filename in self.dict():
            tmp_mtime = self.__files_mtimes[filename]
            if (old_files_mtimes.has_key(filename) and
                tmp_mtime != old_files_mtimes.mtime(filename)):
                result[filename] = tmp_mtime
        return FilesMtimes().from_dict(result)

    # Return the ones in the current set that were not present in the old_files_mtimes
    def new(self, old_files_mtimes):
        result = {}
        for filename in self.dict():
            if not old_files_mtimes.has_key(filename):
                result[filename] = self.mtime(filename)
        return FilesMtimes().from_dict(result)
        
    # Return the ones not in the current set, but present in the old_files_mtimes
    def deleted(self, old_files_mtimes):
        result = {}
        for item in old_files_mtimes.keys():
            if not self.__files_mtimes.has_key(item):
                result[item] = old_files_mtimes.dict()[item]
        return FilesMtimes().from_dict(result)                
        

class FileTable:
    __conn = None
    __cursor = None
    __table_setting = None

    def __init__(self, credentials, table_setting):
        self.__table_setting = table_setting
        self.__open_table(credentials)

    def __del__(self):
        self.__cursor.close()
        self.__conn.close()

    def __open_table(self, credentials):
        try:
            c = credentials
            self.__conn =  mdb.connect(c['host'], c['user'], c['password'], c['database'])
            self.__cursor = self.__conn.cursor()
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            sys.exit(1)

    def create_table(self):
        warnings.filterwarnings("ignore", "Table .* already exists")
        s = self.__table_setting
        self.__cursor.execute("CREATE TABLE IF NOT EXISTS %s(%s VARCHAR(25) PRIMARY KEY, %s INT)" % (s['table'], s['name'], s['mtime']))

    def insert(self, files_mtimes):
        s = self.__table_setting
        for filename in files_mtimes.keys():
            mtime = str(files_mtimes.mtime(filename))
            self.__cursor.execute("INSERT INTO %s(%s, %s) VALUES('%s','%s')" %
                                 (s['table'], s['name'], s['mtime'], filename, mtime))
        self.__conn.commit()

    def delete(self, files_mtimes):
        s = self.__table_setting
        for filename in files_mtimes.keys():
            self.__cursor.execute("DELETE FROM %s WHERE %s='%s'" %
                                 (s['table'], s['name'], filename))
        self.__conn.commit()
        
    def update(self, files_mtimes):
        self.delete(files_mtimes)
        self.insert(files_mtimes)

    def get_all(self):
        s = self.__table_setting
        self.__cursor.execute("SELECT %s, %s FROM %s" %
                             (s['name'], s['mtime'], s['table']))
        result = self.__cursor.fetchall()
        if result == None:
            return ()
        else:
            return result

    # current_files_mtimes is supposed to be the current filesystem data
    # This method updates the current db table
    def update_all(self, current_files_mtimes):
        db_files_mtimes = FilesMtimes().from_tuples(self.get_all())

        deleted_files_mtimes = current_files_mtimes.deleted(db_files_mtimes)
        self.delete(deleted_files_mtimes)

        new_files_mtimes = current_files_mtimes.new(db_files_mtimes)
        self.insert(new_files_mtimes)

        updated_files_mtimes = current_files_mtimes.updated(db_files_mtimes)
        self.update(updated_files_mtimes)

        #print deleted_files_mtimes.format()
        #print new_files_mtimes.format()
        #print updated_files_mtimes.format()

def main():
    current = FilesMtimes().from_path(path)
    FileTable(db_credentials, db_table_settings).update_all(current)

if __name__ == "__main__":
    main()
