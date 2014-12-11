import csv
import json
from pymongo import MongoClient #import pymongo library
from pymongo import collection

#All three datasets are clubbed into one dataset

#connecting to mongodb and creating a database
#c= MongoClient('mongodb://localhost:27017/')
c = MongoClient('mongodb://sphv:2s3WW6ut@50.84.62.186/sphv')
db = c.sphv

RxTerms=db.RxTerms


path= '/Users/shridharmanvi/Desktop/ayoka/RxTerms'
rx=path+'/RxTerms201408.txt'
archive=path+'/RxTermsArchive201408.txt'
ingredients=path+'/RxTermsIngredients201408.txt'

r = open(rx, 'r')
a = open(archive, 'r')
i = open(ingredients, 'r')
ing={}
arc={}
rxmain_arc={}


#import ingredients file
for row in i.readlines():
    row= row.split('|')
    j={}
    #j['RXCUI']=int(row[0])
    j['INGREDIENT']=str(row[1])
    j['ING_RXCUI']=int(row[2])
    ing[int(row[0])]= j


#import rxarchive file
for row in a.readlines():
    row= row.split('|')
    j={}
    if(row[0]!=''):j['RXCUI']=int(row[0])
    if(row[1]!=''):j['GENERIC_RXCUI']=int(row[1])
    if(row[2]!=''):j['TTY']=row[2]
    if(row[3]!=''):j['FULL_NAME']=row[3]
    if(row[4]!=''):j['RXN_DOSE_FORM']=row[4]
    if(row[5]!=''):j['FULL_GENERIC_NAME']=row[5]
    if(row[6]!=''):j['BRAND_NAME']=row[6]
    if(row[7]!=''):j['DISPLAY_NAME']=row[7]
    if(row[8]!=''):j['ROUTE']=row[8]
    if(row[9]!=''):j['NEW_DOSE_FORM']=row[9]
    if(row[10]!=''):j['STRENGTH']=row[10]
    if(row[11]!=''):j['SUPPRESS_FOR']=row[11]
    if(row[12]!=''):j['DISPLAY_NAME_SYNONYM']=row[12]
    if(row[13]!=''):j['IS_RETIRED']=row[13]
    if(row[14]!=''):j['SXDG_RXCUI']=row[14]
    if(row[15]!=''):j['SXDG_TTY']=row[15]
    if(row[16]!=''):j['SXDG_NAME']=row[16]
    rxmain_arc[int(row[0])]= j



#import rxarchive file
for row in r.readlines():
    row= row.split('|')
    j={}
    if(row[0]!=''):j['RXCUI']=int(row[0])
    if(row[1]!=''):j['GENERIC_RXCUI']=int(row[1])
    if(row[2]!=''):j['TTY']=row[2]
    if(row[3]!=''):j['FULL_NAME']=row[3]
    if(row[4]!=''):j['RXN_DOSE_FORM']=row[4]
    if(row[5]!=''):j['FULL_GENERIC_NAME']=row[5]
    if(row[6]!=''):j['BRAND_NAME']=row[6]
    if(row[7]!=''):j['DISPLAY_NAME']=row[7]
    if(row[8]!=''):j['ROUTE']=row[8]
    if(row[9]!=''):j['NEW_DOSE_FORM']=row[9]
    if(row[10]!=''):j['STRENGTH']=row[10]
    if(row[11]!=''):j['SUPPRESS_FOR']=row[11]
    if(row[12]!=''):j['DISPLAY_NAME_SYNONYM']=row[12]
    if(row[13]!=''):j['IS_RETIRED']=row[13]
    if(row[14]!=''):j['SXDG_RXCUI']=row[14]
    if(row[15]!=''):j['SXDG_TTY']=row[15]
    if(row[16]!=''):j['SXDG_NAME']=row[16]
    rxmain_arc[int(row[0])]= j


final={}

for key in rxmain_arc.keys():
    try:
        final[key]=dict(rxmain_arc[key].items()+ing[key].items())
    except KeyError:
        final[key]=rxmain_arc[key]


for row1 in final.keys():
    x=final[row1]
    RxTerms.insert(x)
  

print 'Insertion complete!'

"""
#test example
#Works for both Rx and archive. Archive will have ingredient included.

#print final[851948] has ingredient
#print final[1000056] no ingredient
#print RxTerms.find_one({"RXCUI": 851948})  #for example.
"""
