# -*- coding: utf-8 -*-
"""
Created on Thu May  7 16:30:01 2015

@author: gjm

Dumps the javascript map and reduce functions from views.
"""

from couchdb import CouchDB

couchdb_url = "http://127.0.0.1:5984"
db_name = "yabr"

def main():
    yabr = CouchDB(couchdb_url, db_name)
    yabr.dump_views()

if __name__ == "__main__":
    main()
