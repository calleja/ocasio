# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 16:19:21 2017

Voter attribute parsed from the file:
    political party
    EMSID
    address 
    DOB
    name

Targeted towards Jabari's original voter file
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime, date, timedelta
from itertools import chain

#read lines from txt file in - don't treat
with open('/home/tio/Documents/politics/jabari_parker/voterRegistrationFiles/Council District 35.txt','r') as f:
    part = [line for line in f]
#when working from office
with open('G:\Property\Luis_C\politics\Council District 35.txt','r') as f:
    part = [line for line in f]
    
#discover political parties... BUT DON'T FILTER ON THESE
#we are only concerned with independents, greens and blk
parties=re.compile('[0-9](?:MBLK|FBLK|MGRE|FIND|MIND|FGRE|MREP|FREP|MDEM|FDEM|MWORK|FWOR)')
parties1=re.compile('MBLK|FBLK|MGRE|FIND|MIND|FGRE|MREP|FREP|MDEM|FDEM|MWOR|FWOR')
#select only those lines with the parties mentioned above
part_int=[line for line in part if re.search(parties,line)]
#part_int contains political party and sex

'''address of voter'''
sepa=re.compile('(brooklyn +?[0-9]+? |BKLYN NY +?[0-9]+? |BROOKLYN N.Y. +?[0-9]+? |BKLYN N.Y. +?[0-9]+? )',flags=re.IGNORECASE)
#splitting on different addresses
#this method stores the separator as an element in the list
address_int=[re.split(sepa,line) for line in part]
''' testing output...
#vanilla observations have 3 elements - can see the distribution from the below
d1=[len(x) for x in address_int]
d={x:d1.count(x) for x in d1}
d #oddly, 4 obs have just one element
'''

#after creating a list based on address, we try to discover the address from the first element of the list
#element number to parse depends on # of elements in list - we can create a function that returns all addresses of voter
address=re.compile(' [0-9]{2,5}.*',flags=re.DOTALL) #return everything after & including the house number
#to address missing data, create a function with a try-catch - all results should be in the form of one element-long lists
def addressParse(txt):
    #deal with IndexError; 
    if not address.findall(txt):
        return 'empty'
    else:
        return(address.findall(txt))
add_first=[addressParse(line[0]) for line in address_int]
#convert from list of lists to a list of strings
def stringWork(g):
    f=''.join(g)
    ret=re.compile(' {1,40}')
    f=re.split(ret,f)
    d=[x.strip() for x in f]
    f=' '.join(d).strip()
    return(f)
#test stringWork()
stringWork(add_first[1])
di=[stringWork(x) for x in add_first]

#format of output: single element list - house #, apt# (optional), street name and street type; I need to remove the apt#
def geoAddress(txt):
    r=re.compile(' {3,78}')
    res=re.split(r,txt) # a list
    res1=res[:-1]
    indexes=[0,2]
    if len(res1)==3:
        final=[res1[x] for x in indexes]
    elif len(res1)==2:
        final=res1
    else:
        final=['error']
    return(final)
houses=[geoAddress(x[0]) for x in add_first]

'''calculate age of voter'''
def findAge(line):
    age_re=re.compile('[0-9]{8}(?:MBLK|FBLK|MGRE|FIND|MIND|FGRE|MREP|FREP|MDEM|FDEM|MWOR|FWOR)',flags=re.IGNORECASE)
    try:
        dates=age_re.findall(line)[0]
        birth=dates[0:8]
    #convert to datetime object
        d=datetime.strptime(birth,'%Y%m%d')    
        age=(datetime.now() - d) // timedelta(days=365.2425)
        return age
    except (ValueError,IndexError):
        return 'error'

ty=[findAge(x) for x in part]

''' extract name of the voter '''
#return everything before the first pure number word 
def nom(line):
    stop_house=re.compile('.* [0-9]{1,5}')
    nom=re.compile('[A-Z]{1,15}-?[A-Z]{1,15}')
    try:
        g=stop_house.search(line[0])    
        g1=re.split(' {1,40}',g.group(0))
        #create one list of the names - list having multiple elements
        names=[x for g in g1 for x in nom.findall(g)]
        #create a string of all names per voter:
        return(names)
    except AttributeError:
        return('no name')
    
namey=[nom(x) for x in address_int]
names_f=[','.join(name) for name in namey]

'''extract political party and sex'''
#re parties (from above)
def partyFunct(doc):
    g=parties1.search(doc)
    try:
        return g[0]
    except AttributeError:
        return 'Weird Party'
    except TypeError:
        return 'Weird Party'

party_list=[partyFunct(x) for x in part]

#convert party_list into a dataframe then extract sex from party
pl=pd.DataFrame(party_list,columns=['both'])
pl['sex']=pl['both'].apply(lambda x: x[0:1])
pl['party']=pl['both'].apply(lambda x: x[1:])

'''extract EMSID '''
emsid_lista=[linea[0:9] for linea in part]
emsid_df=pd.DataFrame(emsid_lista,columns=['emsid'])

'''pass jabari a merged file of add_first, houses, ty, namey and party_list'''
geoAdd=pd.DataFrame(houses,columns=['house_no','street'])
#unlist addresses in add_first
add_g=list(chain.from_iterable(add_first))
final=pd.DataFrame({'address':di,'age':ty,})
names_df=pd.DataFrame(names_f,columns=['name_of_voter'])
#concatenate the four dataframes
frames=[emsid_df,final,geoAdd,pl,names_df]
final_full=pd.concat(frames,axis=1)
final_full.head()
final_full=final_full.drop('both',1)

final_full.to_csv('/home/tio/Documents/politics/jabari_parker/voterRegistrationFiles/dist_addresses_unfiltered.csv',index=False)

final_full=pd.read_csv('/home/tio/Documents/politics/jabari_parker/voterRegistrationFiles/dist_addresses_unfiltered.csv')
final_full.dtypes
empties=final_full.loc[final_full['address']=='empty',:]
   


