# -*- coding: utf-8 -*-
"""
Created on Wed May  6 11:11:19 2015

@author: gjm

Converts XML files retrieved through the BGG API to JSON files
Handles both individual items and files with many items
Takes a single argument, which is the path of the XML files to process

If you get weird parse errors, look for leading ... at the beginning of 
XML files, which oddly appeared in 9 of several hundred for no apparent reason.
"""

import sys
import xmltodict
import xml.etree.ElementTree as ET
import json
import os

def xml_to_json(root, path, filename):
    json_filename = filename[:-4].zfill(len(filename[:-4])) + '.json'    
    print("xmltodict", json_filename)
    root_dict = xmltodict.parse(ET.tostring(root, encoding='unicode'))
    root_json_str = json.dumps(root_dict, sort_keys=True, indent=4)
    json_file = open(os.path.join(path, json_filename), 'w', encoding='utf8')
    json_file.write(root_json_str)
    print("Wrote", json_filename)
    json_file.close()

def main():
    conversion_path = sys.argv[1]
    
    for filename in os.listdir(conversion_path):
        if filename.endswith(".xml"):
            print("ElementTree", filename)
            tree = ET.parse(os.path.join(conversion_path, filename))
            root = tree.getroot()
            
            # Multi-item files have this root tag
            if root.tag == 'items':
                for child in root:
                    zfill_len = len(filename.split('-')[0])
                    true_filename = child.get('id').zfill(zfill_len) + '.xml'
                    xml_to_json(child, conversion_path, true_filename)
            else:
                xml_to_json(root, conversion_path, filename)

if __name__ == "__main__":
    main()
