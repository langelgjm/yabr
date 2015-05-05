# -*- coding: utf-8 -*-
"""
Created on Tue May  5 10:45:53 2015

@author: gjm

Fetches and saves the XML output from the Board Game Geek API.
Fetches thing ids in 100 id increments, waiting 30 seconds between requests.
Saves the output in XML files.
"""

import requests
import os
import time
from retrying import retry

#bgg_url = "http://www.boardgamegeek.com/xmlapi2/"
bgg_url_thing = "http://www.boardgamegeek.com/xmlapi2/thing"
# I don't actually know what the maximum item in the database is, but this seems close
bgg_max_item = 178000
sleep_interval = 30

def ranges(start, stop, step):
    """
    Yield successive step-width ranges up to stop. 
    The final range may be less than step-width so as not to exceed stop.
    As with range(), output stops at stop - 1.
    """
    for x in range(start, stop, step):
        if x + step <= stop:
            subrange_stop = x + step
        else:
            subrange_stop = stop
        yield range(x, subrange_stop)

@retry(wait_exponential_multiplier=1000, wait_exponential_max=120000)
def requests_get(url, params):
    return requests.get(url, params=params)

def main():
    for r in ranges(1,bgg_max_item + 1,100):
        id_list_str = ','.join([str(i) for i in r])
        param_dict = {'id': id_list_str}
        print("Getting items.")
        response = requests_get(bgg_url_thing, param_dict)
        # file name is the limits of the current range, zero padded
        xml_file_name = '-'.join([str(x).zfill(6) for x in [min(r), max(r)]]) + '.xml'
        print("Writing " + xml_file_name)
        f = open(os.path.join('data/things', xml_file_name), 'w', encoding='utf8')
        f.write(response.text)
        f.close()
        print ("Sleeping.")
        time.sleep(sleep_interval)

if __name__ == "__main__":
    main()