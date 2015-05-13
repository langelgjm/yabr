# -*- coding: utf-8 -*-
"""
Created on Wed May  6 14:08:53 2015

@author: gjm

Puts JSON documents into the CouchDB. Parallelized.
Takes two command line arguments; first is "doctype" to distinguish different 
kinds of documents; second is the path to look for JSON files.
It assumes all the files at that path are of the same doctype.
"""

import sys
import os
import json
from multiprocessing import Pool
from itertools import repeat
from couchdb import CouchDB

couchdb_url = "http://127.0.0.1:5984"
db_name = "yabr"
num_processes = 4

def post_json(filename, path, doctype):
    if filename.endswith(".json"):
        #print(filename)
        json_file = open(os.path.join(path, filename), 'r', encoding='utf8')
        # We have to convert the JSON to a Python dict, then index it, 
        # because the first element is always something like "id" or "thread"
        # and all other data is nested under it, so we want to discard the first element.
        # However, for collections, this means we lose the total number of items and publication date
        obj = json.load(json_file, encoding='utf8')
        obj_data = obj[list(obj)[0]]
        # Also we want to set a "doctype" key to indicate what kind of 
        # document we are loading. This is not in the original data, and 
        # comes from the first command line argument.
        obj_data['doctype'] = doctype
        # Collections need special handling. 
        # We want to add a field that gives the username to whom the collection belongs.
        if doctype == "collection":
            obj_data['@username'] = filename[:-5]
        # Convert back to JSON for POSTing
        # The implicit default ensure_ascii=True is necessary here;
        # Otherwise it is technically invalid JSON which CouchDB rejects
        json_data = json.dumps(obj_data, sort_keys=True, indent=4)
        yabr = CouchDB(couchdb_url, db_name)
        yabr.insert(json_data, batch=False)
    
def main():
    doctype = sys.argv[1]
    insert_path = sys.argv[2]
    
    file_list = os.listdir(insert_path)

    with Pool(num_processes) as p:
        p.starmap(post_json, zip(file_list, 
                                 repeat(insert_path, len(file_list)), 
                                 repeat(doctype, len(file_list))))
            
if __name__ == "__main__":
    main()
