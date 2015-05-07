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
import json

couchdb_url = "http://127.0.0.1:5984/yabr/"

def main():
    view_str = "_design/users"
    rs = requests.get(couchdb_url + view_str)
    # Add in handling for multiple views
    filename = rs.json()["_id"].replace("/", "--") + ".json"
    json.dump(rs.json(), open(filename, 'w'), indent=4)    

if __name__ == "__main__":
    main()
