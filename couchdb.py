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
    def get_view(self, view, group=False):
        """
        Request views by design document name, like '_design/users'.
        We assume all views have the same name as their design document.
        """
        param_dict = {'startkey': '"_design/"',
                      'endkey': '"_design0"'}
        r = requests.get('/'.join([self.url, self.db, "_all_docs"]), params=param_dict)
        if r.status_code != 200:
            raise Exception("HTTP " + str(r.status_code))
        elif not view in [d['id'] for d in r.json()['rows']]:
            raise Exception("Design document " + view + " not found.")
        else:
            param_dict = {'group': str(group).lower()}
            r = requests.get('/'.join([self.url, self.db, view, view.replace("design", "view", 1)]), params=param_dict)
            if r.status_code != 200:
                raise Exception("HTTP " + str(r.status_code))
            else:
                return r.json()