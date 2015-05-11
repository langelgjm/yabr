# -*- coding: utf-8 -*-
"""
Created on Wed May  6 11:11:19 2015

@author: gjm

Converts XML files retrieved through the BGG API to JSON files
Handles both individual items and files with many items
Takes a single argument, which is the path of the XML files to process. 

Parallelized.

If you get weird parse errors, look for leading ... at the beginning of 
XML files, which oddly appeared in less than a dozen of several hundred for no apparent reason.
You can find these files using something like: grep -F ".<?xml" *.xml
"""

import sys
import xmltodict
import xml.etree.ElementTree as ET
import json
import os
from multiprocessing import Pool
from itertools import repeat
import html
import re

num_processes = 4

def to_json(root, path, filename):
    json_filename = filename[:-4].zfill(len(filename[:-4])) + '.json'    
    #print("xmltodict", json_filename)   
    root_dict = xmltodict.parse(ET.tostring(root, encoding='unicode'))
    # Don't use ensure_ascii=True here;
    # We want the \u escaped strings is the JSON output
    root_json_str = json.dumps(root_dict, sort_keys=True, indent=4)
    json_file = open(os.path.join(path, json_filename), 'w', encoding='utf8')
    json_file.write(root_json_str)
    #print("Wrote", json_filename)
    json_file.close()

def xml_to_json(filename, path):
    """
    Get XML from file, pass to to_json.
    Handles files with multiple items.
    Also handle parsing errors caused by undefined entities.
    These are typically valid HTML entities improperly used in XML.
    Reopens the file, find the entity, convert it to UTF-8, and rewrites the file.
    """
    try:
        if filename.endswith(".xml"):
            #print("ElementTree", filename)
            tree = ET.parse(os.path.join(path, filename))
            root = tree.getroot()
            # Avoid converting missing threads or other missing responses
            # 'error' happens when no thread exists with that number
            # 'html' happens for some kinds of timeouts
            if root.tag in ('error', 'html'):
                return
            # Multi-item files have this root tag
            elif root.tag == 'items':
                for child in root:
                    zfill_len = len(filename.split('-')[0])
                    true_filename = child.get('id').zfill(zfill_len) + '.xml'
                    to_json(child, path, true_filename)
            else:
                to_json(root, path, filename)
    except ET.ParseError as e:
        if e.code == 11:
            print(os.path.join(path, filename), e)
    
            # Make a backup of the original file
            filenamepath = os.path.join(path, filename)
            backup_filenamepath = filenamepath + '.bak'
            # Delete existing backup file silently, if any
            try:
                os.unlink(backup_filenamepath)
            except OSError:
                pass
            os.rename(filenamepath, backup_filenamepath)
            xml_file = open(backup_filenamepath, 'r')
            new_xml_file = open(filenamepath, 'w')
            # Read backup file line by line, replacing entities as necessary
            # Wrie output to original file name
            for i, line in enumerate(xml_file):
                # Find entity using line, column tuple from e.position
                if i + 1 == e.position[0]:
                    entity = line[e.position[1]:].split(';')[0] + ';'
                    # Convert to unicode representation
                    new_entity = html.unescape(entity)
                    print(entity, "->", new_entity, file=sys.stderr)
                    new_xml_file.write((re.sub(entity, new_entity, line)))
                else:
                    new_xml_file.write(line)
            xml_file.close()
            new_xml_file.close()
            # Recursively call ourself to handle the next error in the same file
            xml_to_json(filename, path)
        else:
            print("Unhandled ParseError", e, "in", filename)

def main():
    conversion_path = sys.argv[1]
    file_list = os.listdir(conversion_path)
    with Pool(num_processes) as p:
        p.starmap(xml_to_json, zip(file_list, repeat(conversion_path, len(file_list))))
    
if __name__ == "__main__":
    main()
