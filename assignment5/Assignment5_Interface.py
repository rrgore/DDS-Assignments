#
# Assignment5 Interface
# Name: Rahul Gore
#

from pymongo import MongoClient
import os
import sys
import json
import math
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    businessList = collection.find({'city': re.compile(cityToSearch, re.IGNORECASE)})
    
    with open(saveLocation1, 'w') as f:
        for business in businessList:
            line = '{0}${1}${2}${3}\n'.format(
                business['name'].upper(), 
                business['full_address'].upper(), 
                cityToSearch.upper(), 
                business['state'].upper())
            f.write(line)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    # first filter by categories
    businessList = collection.find(
        {'categories': {'$in': categoriesToSearch}}
    )
    
    # calculate distance of location to myLocation
    businessList2 = []
    for business in businessList:
        bLat = business['latitude']
        bLon = business['longitude']
        if DistanceFunction(bLat, bLon, float(myLocation[0]), float(myLocation[1])) <= maxDistance:
            businessList2.append(business)
            
    # write to file 
    with open(saveLocation2, 'w') as f:
        for business in businessList2:
            line = business['name'].upper() + '\n'
            f.write(line)

def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    lat1Rad = math.radians(lat1)
    lat2Rad = math.radians(lat2)
    latDiffRad = math.radians(lat2-lat1)
    lonDiffRad = math.radians(lon2-lon1)
    a = math.sin(latDiffRad/2) * math.sin(latDiffRad/2) + math.cos(lat1Rad) * math.cos(lat2Rad) * math.sin(lonDiffRad/2) * math.sin(lonDiffRad/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d
