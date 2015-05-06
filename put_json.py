# -*- coding: utf-8 -*-
"""
Created on Wed May  6 14:08:53 2015

@author: gjm

Puts JSON documents into the CouchDB
"""

import sys
import requests
import os
import json

couchdb_url = "http://127.0.0.1:5984/games/"

def main():
    
    insert_path = sys.argv[1]
    
    for filename in os.listdir(insert_path):
        if filename.endswith(".json"):
            print(filename)
            json_file = open(os.path.join(insert_path, filename), 'r', encoding='utf8')
            # We have to convert the JSON to a Python dict, then index it, 
            # because the first element is always something like "id" or "thread"
            # and all other data is nested under it, so we want to discard the first element.
            obj = json.load(json_file, encoding='utf8')
            obj_data = obj[list(obj)[0]]
            # Convert back to JSON for POSTing
            json_data = json.dumps(obj_data, sort_keys=True, indent=4)
            headers = {'Content-type': 'application/json'}
            r = requests.post(couchdb_url, data=json_data, headers=headers)
            if r.status_code != 201:
                print("Uh oh! Got status code", r.status_code, "while POSTing", filename)
            
if __name__ == "__main__":
    main()
