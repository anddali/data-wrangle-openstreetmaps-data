# -*- coding: utf-8 -*-
"""
Created on Fri May 01 09:48:00 2015

@author: Andrius Dalisanskis

Queries against mongodb database
"""
import pprint

def main():
    
    from pymongo import MongoClient    
    client = MongoClient()
    db = client.project2 
    #'''    
    print '>>>Total count of documents: {}\n'.format(db.cleaned_data.find().count())
    print '>>>Find one document:'
    pprint.pprint(db.cleaned_data.find_one({'amenity':'place_of_worship','type':'node', 'name' : {'$exists':1}}))
    print '\n>>>Number of unique users: {}\n'.format(len(db.cleaned_data.distinct('created.user')))
    

    q = [{'$group':{'_id':'$created.user', 'count':{'$sum':1}}},
         {'$sort' : {'count' : -1}},
         {'$limit' : 3}]
    print '>>>Top 3 contributing users: \n{}\n'.format(db.cleaned_data.aggregate(q)['result'])

    
    q = [{'$group':{'_id':'$created.user', 'count':{'$sum':1}}},
        {'$group':{'_id':'$count', 'user_count':{'$sum':1}}},
        {'$sort':{'_id':1}},
        {'$limit':1}]
    print '>>>Number of users that have only one contribution:\n{}\n'.format(db.cleaned_data.aggregate(q)['result'])
    #'''
    
    q = [{'$match':{'shop':{'$exists':1}, 'name':{'$exists':0}}},
         {'$group':{'_id':'$created.user', 'count':{'$sum':1}}},
         {'$sort' : {'count' : -1}},
         {'$limit' : 3}]
    print '>>>Number of shops that have name missing:\n{}\n'.format(db.cleaned_data.find({'shop':{'$exists':1}, 'name':{'$exists':0}}).count())
    print '>Top 3 users who created them:\n{}\n'.format(db.cleaned_data.aggregate(q)['result'])
    
    q = [{'$group':{'_id':None, 'shops':{'$addToSet':'$shop'}, 'amenities':{'$addToSet':'$amenity'}}},
         {'$project':{'_id':0, 'commonToBoth':{'$setIntersection':['$shops','$amenities']}}}]
    print '>>>Values common to shop and amenity:\n{}\n'.format(db.cleaned_data.aggregate(q)['result'])
    
    q = [{'$unwind':'$cuisine'},
         {'$group':{'_id':'$cuisine', 'count':{'$sum':1}}},
         {'$sort':{'count':-1}},
         {'$limit': 2}]
    print '>>>Top 2 cuisines in Dublin:\n{}\n'.format(db.cleaned_data.aggregate(q)['result'])
         
         
    return
 
 

if __name__ == '__main__':
    main()
