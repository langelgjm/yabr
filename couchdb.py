# -*- coding: utf-8 -*-
"""
Created on Fri May  8 11:30:11 2015

@author: gjm

Basic CouchDB class to make life easier
"""

import requests
import json
import time

class CouchDB(object):
    def __init__(self, url, db):
        r = requests.get('/'.join([url, "_all_dbs"]))
        if r.status_code != 200:
            raise Exception("HTTP " + str(r.status_code))
        elif not db in r.json():
            raise Exception("Database " + db + " not found.")
        else:
            self.url = url
            self.db = db
    def exists_design(self, design):
        design_str = '/'.join(['_design', design])
        param_dict = {'startkey': '"_design/"',
                      'endkey': '"_design0"'}
        r = requests.get('/'.join([self.url, self.db, "_all_docs"]), params=param_dict)
        if r.status_code != 200:
            raise Exception("HTTP " + str(r.status_code))
        elif not design_str in [d['id'] for d in r.json()['rows']]:
            raise Exception("Design document " + design + " not found.")
        else:
            return True
    def exists_view(self, design, view):
        design_str = '/'.join(['_design', design])
        param_dict = {'key': ''.join(['"', design_str, '"']),
                      'include_docs': 'true'}            
        r = requests.get('/'.join([self.url, self.db, "_all_docs"]), params=param_dict)
        if r.status_code != 200:
            raise Exception("HTTP " + str(r.status_code))
        elif not view in list(r.json()['rows'][0]['doc']['views'].keys()):
            raise Exception("View ", view, "not found in design document", design, ".")
        else:
            return True        
    def get_view(self, design, view, group=False):
        """
        Request views by design and view name..
        """
        if self.exists_design(design) and self.exists_view(design, view):
            param_dict = {'group': str(group).lower()}
            design_str = '/'.join(['_design', design])
            view_str = '/'.join(['_view', view])
            r = requests.get('/'.join([self.url, self.db, design_str, view_str]), params=param_dict)
            if r.status_code != 200:
                raise Exception("HTTP " + str(r.status_code))
            else:
                return r.json()
    def dump_designs(self):
        """
        Dumps design documents as JSON, as well as the JavaScript for views
        """
        param_dict = {'startkey': '"_design/"',
                      'endkey': '"_design0"',
                      'include_docs': 'true'}            
        r = requests.get('/'.join([self.url, self.db, "_all_docs"]), params=param_dict)
        # Write out the JSON document itself
        for row in r.json()['rows']:
            design_str = row['id'].replace('/', '%2F')
            json.dump(row, open(design_str + '.json', 'w'), indent=4)
            # Also create separate files for the map/reduce javascript
            for view in row["doc"]["views"]:
                view_str = '%2F'.join(['_view', view])
                f = open('%2F'.join([design_str, view_str + ".js"]), 'w')
                for func in row["doc"]["views"][view]:
                    f.write(func + " = " + row["doc"]["views"][view][func] + "\n")
                f.close()
    def insert(self, json_data, batch=None):
        """
        Insert JSON into the database with a POST request
        The time.sleep(0.01) was suggested to avoid "connection reset by peer" errors
        """
        headers = {'Content-type': 'application/json;charset=UTF-8'}
        url = '/'.join([self.url, self.db])
        time.sleep(0.01)
        if batch:
            r = requests.post(url, data=json_data, headers=headers, params={'batch': 'ok'})
        else:
            r = requests.post(url, data=json_data, headers=headers)            
        time.sleep(0.01)
        if not r.status_code in (201, 202):
            raise Exception("HTTP " + str(r.status_code))
    def delete(self, doc_id, doc_rev):
        """
        Delete a document with a DELET request. doc_rev is required.
        """
        headers = {'If-Match': doc_rev}
        url = '/'.join([self.url, self.db, doc_id])
        r = requests.delete(url, headers=headers)
        if r.status_code != 200:
            raise Exception("HTTP " + str(r.status_code))