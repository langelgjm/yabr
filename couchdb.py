# -*- coding: utf-8 -*-
"""
Created on Fri May  8 11:30:11 2015

@author: gjm

Basic CouchDB class to make life easier
"""

import requests

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
    def dump_views(self):
        '''FIXME'''
        param_dict = {'startkey': '"_design/"',
                      'endkey': '"_design0"',
                      'include_docs': 'true'}            
        r = requests.get('/'.join([self.url, self.db, "_all_docs"]), params=param_dict)
        for row in r.json()['rows']:
            for view in row["doc"]["views"]:
                f = open(view + ".js", 'w')
                for func in row["doc"]["views"][view]:
                    f.write(func + " = " + row["doc"]["views"][view][func] + "\n")
                f.close()