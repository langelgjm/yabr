# -*- coding: utf-8 -*-
"""
Created on Wed May  6 11:11:19 2015

@author: gjm

Converts XML files retrieved through the BGG API to JSON files
Handles both individual items and files with many items
Takes two arguments: xml_type, which is either "thing" or ignored (but must be present), and 
the path of the XML files to process. 

Parallelized.

If you get weird parse errors, look for leading ... at the beginning of 
XML files, which oddly appeared in less than a dozen of several hundred for no apparent reason.
You can find these files using something like: grep -F ".<?xml" *.xml

Also, sometimes we might encounter files resulting from timeouts or bad gateway errors.
These will be HTML files, not XML, and will throw a ParseError.
Easiest to find them with grep -F "<html" *.xml beforehand and delete them.
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
import unicodedata

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

def xml_to_json(filename, path, xml_type=None):
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
            # But we only want to break them up like so when dealing with multi-game files
            elif root.tag == 'items' and xml_type == "thing":
                for child in root:
                    zfill_len = len(filename.split('-')[0])
                    true_filename = child.get('id').zfill(zfill_len) + '.xml'
                    to_json(child, path, true_filename)
            # Single item files and multi-item files that are not things (e.g., collections)
            else:
                to_json(root, path, filename)
    except ET.ParseError as e:
        # Handle undefined entity errors
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
        # Handle not well-formed (invalid token) errors
        elif e.code == 4:
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
                    offending_char = line[e.position[1]]
                    print("Removing control codes such as", repr(offending_char))
                    new_xml_file.write(''.join(char for char in line if unicodedata.category(char)[0] != 'C'))
                else:
                    new_xml_file.write(line)
            xml_file.close()
            new_xml_file.close()
            # Recursively call ourself to handle the next error in the same file
            xml_to_json(filename, path)       
        else:
            print("Unhandled ParseError", e, "in", filename)

def main():
    xml_type = sys.argv[1]
    conversion_path = sys.argv[2]
    file_list = os.listdir(conversion_path)
    with Pool(num_processes) as p:
        p.starmap(xml_to_json, zip(file_list, 
                                   repeat(conversion_path, len(file_list)), 
                                   repeat(xml_type, len(file_list))))
    
if __name__ == "__main__":
    main()
