# -*- coding: utf-8 -*-
"""
Created on Thu May  7 16:30:01 2015

@author: gjm

Puts JSON documents into the CouchDB. Parallelized.
Takes two command line arguments; first is "doctype" to distinguish different 
kinds of documents; second is the path to look for JSON files.
It assumes all the files at that path are of the same doctype.
"""

import requests

couchdb_url = "http://127.0.0.1:5984/yabr/"

def main():
    view_str = "_design/users/_view/users"
    param_dict = {'group': 'true'}
    r = requests.get(couchdb_url + view_str, params=param_dict)
    users = r.json()    

if __name__ == "__main__":
    main()
