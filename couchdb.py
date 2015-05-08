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
    def exists_view(self, view):
        param_dict = {'startkey': '"_design/"',
                      'endkey': '"_design0"'}
        r = requests.get('/'.join([self.url, self.db, "_all_docs"]), params=param_dict)
        if r.status_code != 200:
            raise Exception("HTTP " + str(r.status_code))
        elif not view in [d['id'] for d in r.json()['rows']]:
            raise Exception("Design document " + view + " not found.")
        else:
            return True
    def get_view(self, view, group=False):
        """
        Request views by design document name, like '_design/users'.
        We assume all views have the same name as their design document.
        This will be fixed in the future by putting multiple views in a hardcoded? design document.
        """
        if self.exists_view(view):
            param_dict = {'group': str(group).lower()}
            r = requests.get('/'.join([self.url, self.db, view, view.replace("design", "view", 1)]), params=param_dict)
            if r.status_code != 200:
                raise Exception("HTTP " + str(r.status_code))
            else:
                return r.json()
    def dump_views(self):
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