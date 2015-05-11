# -*- coding: utf-8 -*-
"""
Created on Wed May  6 08:55:39 2015

@author: gjm

Fetches 
"""

import random
import pickle
import requests
import os
import time
from retrying import retry
from couchdb import CouchDB

db_url = "http://127.0.0.1:5984"
db_name = "yabr"
bgg_url_thread = "http://www.boardgamegeek.com/xmlapi2/thread"
bgg_start_thread = 1
# Maximum thread number as of May 6, ~10 AM
bgg_stop_thread = 1366106
# Sample about 1% of the total number of threads
bgg_sample_stop = 13661
sleep_interval = 5

@retry(wait_exponential_multiplier=1000, wait_exponential_max=120000)
def requests_get(url, params):
    return requests.get(url, params=params)

def main():
    yabr = CouchDB(db_url, db_name)

    # Make a list of all possible ids
    thread_id_list = list(range(bgg_start_thread, bgg_stop_thread))
    full_set = set(thread_id_list)

    # Get all the thread ids we already have    
    db_thread_list = yabr.get_view("yabr", "threads")
    in_db_set = set([d['key'] for d in db_thread_list['rows']])
    print(len(in_db_set), "threads in database.")

    # Find those thread ids that we don't already have
    not_in_db_set = full_set.difference(in_db_set)   
    not_in_db_list = list(not_in_db_set)
    
    # Shuffle, then take the desired sample size, then sort
    random.shuffle(not_in_db_list)
    sampled_id_list = [not_in_db_list.pop() for i in range(0, bgg_sample_stop)]
    sampled_id_list.sort()
    
    # Pickle both lists in case something goes wrong.
    # By saving both we can continue to increase the sample without replacement, 
    # and we easily know what we have already sampled.
    for l in ('not_in_db_list', 'sampled_id_list'):
        f = open(l + '.pickle', mode='wb')
        pickle.dump(locals()[l], f)
        f.close()

    for i, t in enumerate(sampled_id_list):
        param_dict = {'id': str(t)}
        print("Getting thread ", t, " (", i+1, " of ", bgg_sample_stop, ")", sep="")
        response = requests_get(bgg_url_thread, param_dict)
        xml_file_name = str(t).zfill(7) + '.xml'
        print("Writing", xml_file_name)
        f = open(os.path.join('data/threads', xml_file_name), 'w', encoding='utf8')
        f.write(response.text)
        f.close()
        print ("Sleeping.")
        time.sleep(sleep_interval)

if __name__ == "__main__":
    main()
