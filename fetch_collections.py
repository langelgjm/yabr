# -*- coding: utf-8 -*-
"""
Created on Wed May  6 08:55:39 2015

@author: gjm

Fetches collections for certain BGG users
"""

import pickle
import requests
import os
import time
from retrying import retry
from couchdb import CouchDB

couchdb_url = "http://127.0.0.1:5984"
bgg_url_collection = "http://www.boardgamegeek.com/xmlapi2/collection"

@retry(wait_exponential_multiplier=1000, wait_exponential_max=120000)
def fetch_collection(url, params):
    r = requests.get(url, params=params)
    if r.status_code == 202:
        time.sleep(10)
        # Recurse until the collection is ready
        r = fetch_collection(url, params)
    if r.status_code == 200:
        return r

def main():
    yabr = CouchDB(couchdb_url, "yabr")
    print("Getting user list from database.")
    users = yabr.get_view("_design/users", group=True)    
    # Filter to usernames that appear more than twice
    # Thus we bias our collection data to more active users, and presumably cut down 
    # on those users likely to have small or no collections
    user_list = [d['key'] for d in users['rows'] if d['value'] > 2]
    # Pickle this list so we know what users we've gotten collections for
    # For non-initial runs, we'll need to eliminate users that are in the pickled file
    # debug example: user_list = ['tacroy_', 'melissa', 'Meerkat', 'Morphie', 'nunovix']
    f = open('fetch_collections_user_list.pickle', mode='wb')
    pickle.dump(user_list, f)
    f.close()   
 
    for i, user in enumerate(user_list):
        print("Getting ", user, "'s collection (", i+1, " of ", len(user_list), ")", sep="")

        # Be sure to set the stats=1 parameter to get rating/ranking information
        param_dict = {'username': user, 'stats': 1}
        response = fetch_collection(bgg_url_collection, param_dict)

        xml_file_name = user + '.xml'
        print("Writing", xml_file_name)
        f = open(os.path.join('data/collections', xml_file_name), 'w', encoding='utf8')
        f.write(response.text)
        f.close()
        print ("Sleeping.")
        time.sleep(5)

if __name__ == "__main__":
    main()
