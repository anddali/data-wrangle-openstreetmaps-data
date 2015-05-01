# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 14:43:43 2015

@author: Andrius Dalisanskis

Main project file. Reads/cleans data and then saves it to mongodb database
"""


import xml.etree.ElementTree as ET
import pprint
import re
import string
#import pymongo

PRINT_TO_FILE = True
PRETTY_PRINT = False
FILE = 'dublin_ireland.osm'
#FILE = 'dublin_part.osm'

problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

correct={
' ' : '_',
',etc.' : '',
'fish_&_chips' : 'fish_and_chips',
'_&_':',',
'brazillian' : 'brazilian',
',_' : ',',
';' : ',',
';_' : ',',
'gelati' : 'gelato',
'asian_' : 'asian',
' & ' : '_and_',
'chineese' : 'chinese',
'coffeehouse' : 'coffee_shop',
'chipper' : 'fish_and_chips',
'steak_house' : 'steak',
'astromony' : 'astronomy',
'betting_office' : 'betting',
'cameras' : 'camera',
'carpets' : 'carpet',
'dry_cleaners' : 'dry_cleaning',
'electrical' : 'electronics',
'agents' : 'agent',
'floor finishes' : 'flooring',
'models' : 'model',
'tattoo_studio':'tattoo',
'atm,bank' : 'bank',
'deli,bistro' : 'deli',
'preschool' : 'pre-school',
'pub,restaurant' : 'restaurant',
'baby_care' : 'baby',
'baby_goods' : 'baby',
'candy': 'confectionery',
'windows' : 'window',
'coffee_house' : 'coffee_shop'

}

# the following are used in fix_number() function
pr1 = re.compile('\A003531\d{7}\Z') #00 353 1 xxx xxxx
pr2 = re.compile('\A01\d{7}\Z')     #01 xxx xxxx
pr3 = re.compile('\A35301\d{7}\Z')  #353 01 xxx xxxx
pr4 = re.compile('\A3531\d{7}\Z')   #353 1 xxx xxxx
pr5 = re.compile('\A3538[3,5,6,7,9]\d{7}\Z') #353 [yy] xxx xxxx , yy can be 83,85,86,87,89
allc = string.maketrans('','')
nodigs= allc.translate(allc,string.digits)

def fix_value(value):
    for x in correct:             
            value = value.replace(x, correct[x])
    return value

    
def fix_phone(value):
    value=value.translate(allc,nodigs) # remove non digits
    
    # we can group phone numbers after we remove non digit characters
    # then we use regular expressions to see which goup number belongs to
    # then we can use this information to extract the required digits 
    # and make the phone number follow the format:
    # 00 353 XX XXX XXXX
    # and we drop some of the numbers which are not typed in properly (missing/too many numbers etc...)
    if pr1.match(value):
        value = str('0035301') + value[-7:]
    elif pr2.match(value):
        value = str('00353') + value               
    elif pr3.match(value):
        value = str('00') + value            
    elif pr4.match(value):
        value = str('0035301') + value[-7:]            
    elif pr5.match(value):
        value = str('00353') + value[-9:]
    else:
        value = None
    return value
    
def shape_element(element):
    valid = ['housenumber', 'street', 'amenity', 'cuisine', 'name', 'phone', 'shop' ] # tag keys that i use in my data model
    to_be_fixed = ['amenity', 'cuisine', 'shop']    # list of keys which values will have to be passed through fix_value() function
    node = {}
    
    # we will use 2 top level elements - node and way:    
    if element.tag == "node" or element.tag == "way" :        
        if 'id' in element.attrib: node['id'] = element.attrib['id']
        node['type'] = element.tag
        if 'visible' in element.attrib: node['visible'] = element.attrib['visible']
        if 'lat' in element.attrib: node['pos'] = [float(element.attrib['lat']), float(element.attrib['lon'])]
        if 'id' in element.attrib: 
            node['created'] = {'version' : element.attrib['version'],
                           'changeset' : element.attrib['changeset'],
                           'timestamp' : element.attrib['timestamp'],
                           'user' : element.attrib['user'],
                           'uid' : element.attrib['uid']}
        
        
        
        ads = {} # dictionary for adress
        nds = [] # list of node_refs
        
        for e in element:   # loop children elements. We are interested in 'tag' and 'nd'
            
            if e.tag == 'tag':
                if problemchars.match(e.attrib['k']) : # skip if key has problematic characters
                    continue
                key = e.attrib['k'].strip().split(':') # lets see if its an address
                
                if len(key) == 2 and key[0] == 'addr':                     
                    if key[-1] in valid:                # if tag key is house number or street
                        ads.update({key[-1] : e.attrib['v']}) #add it to the address dictionary
                        continue                
                
                if len(key)==1 and key[0] in valid: # if key is one that we need ('amenity', 'cuisine', 'name', 'phone', 'shop')
                    value = e.attrib['v']
                    if not value:
                        continue
                    if key[0] in to_be_fixed:      # 'amenity', 'cuisine', 'shop' key values need to be fixed      
                        value = fix_value(value.lower())                    
                    if key[0]=='cuisine':         # value might be a list for key 'cuisine'             
                        value = value.strip().split(',')
                    if key[0]=='phone':     # we need to format phone numbers as well
                        value = fix_phone(value)
                        if not value:
                            continue 
                    node[key[0]] = value
                    
            if e.tag == 'nd':
                nds.append(e.attrib['ref'])
                
        if len(ads.keys()): # add address to final element if we found one
            node['address'] = ads
        if nds:             # add node refs to final element if we found one
            node['node_refs'] = nds

        return node
    else:
        return None


def process_map(file_in):
    
    from pymongo import MongoClient    
    client = MongoClient()
    db = client.project2        # get project2 database  
    db.cleaned_data.remove()
    print db.cleaned_data.count()

    for _, element in ET.iterparse(file_in):
        el = shape_element(element)
        if el:
            db.cleaned_data.insert(el)

                

    print db.cleaned_data.count()
    pprint.pprint(db.cleaned_data.find_one())
    return          


def main():

    process_map(FILE)
         
    return
 
 

if __name__ == '__main__':
    main()
