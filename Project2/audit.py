# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 14:43:43 2015

@author: Andrius Dalisanskis

This script is to create the report on values for the keys that we are interested in.
Findings will be saved in 'report.txt'

"""

import codecs
import xml.etree.ElementTree as ET

FILE = 'dublin_ireland.osm'

KEYS = ['cuisine', 'amenity', 'shop', 'craft', 'phone']


def main():
    file_out = 'report.txt'
    
    report = {}

    
    for i in KEYS: # prepare the dictionaries
        report[i]={}
    
    for _, element in ET.iterparse(FILE):

        if element.tag!='tag':
            continue
        
        key = element.attrib['k']
        value = element.attrib['v']        
        
        # create a dictionary for every key that we are interested in
        # which will hold a list of different values and how many of each of them we find
        if key in KEYS:
            if value in report[key].keys():
                report[key][value]+=1
            else:
                report[key][value]=1
    
    with codecs.open(file_out, "w") as fo: # output report to file
        for key in report:
            fo.write(' >---- KEY = '+key+'  ---------------------------------<\n')
            for e in sorted(report[key]):
                fo.write('{} = {}\n'.format(e,report[key][e]))
        
    return
 
 

if __name__ == '__main__':
    main()
