#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 31 19:20:05 2017

parse the voter registration file purchased aug 25, 2017

Parsing is based on position delimited; Does not place the output into a database - a dataframe, or a  list of dictionaries
"""
import pandas as pd
import numpy as np
import re
import datetime
import pymongo
from itertools import compress

raw='/home/tio/Documents/politics/adrienne/MN- Voter Data.txt'
with open(raw,'r') as f:
    raw_list=[line for line in f]
        
    
#create a dictionary from each line

def tgt_fields(s):
    dicy={'emsid':s[0:9].strip(),
    'lastname':s[9:39].strip(),
    'firstname':s[39:69].strip(),
    'housenumber':s[74:84].strip(),
    'housesuffix':s[84:94].strip(),
    'apt':s[94:109].strip(),
    'streetname':s[109:159].strip(),
    'city':s[159:199].strip(),
    'zipcode':s[199:204].strip(),
    'birthdate':datetime.datetime.strptime(s[408:416].strip(),"%Y%m%d"),
    'sex':s[416].strip(),
    'party':s[417:420].strip(),
    'electiondist':s[450:453].strip(),
    'ad':s[453:455].strip(),
    'congressionaldist':s[455:457].strip(),
    'councildist':s[457:459].strip(),
    'senatedist':s[459:461].strip(),
    'registrationdate':datetime.datetime.strptime(s[465:473].strip(),"%Y%m%d"),
    'phone':s[488:508].strip()}
    return(dicy)

def simple_fields(s):
    dicy={'emsid':s[0:9].strip(),
    'lastname':s[9:39].strip(),
    'firstname':s[39:69].strip(),
    'housenumber':s[74:84].strip(),
    'housesuffix':s[84:94].strip(),
    'apt':s[94:109].strip(),
    'streetname':s[109:159].strip(),
    'city':s[159:199].strip(),
    'zipcode':s[199:204].strip(),
    'birthdate':s[408:416].strip(),
    'sex':s[416].strip(),
    'party':s[417:420].strip(),
    'electiondist':s[450:453].strip(),
    'ad':s[453:455].strip(),
    'congressionaldist':s[455:457].strip(),
    'councildist':s[457:459].strip(),
    'senatedist':s[459:461].strip(),
    'registrationdate':datetime.datetime.strptime(s[465:473].strip(),"%Y%m%d"),
    'phone':s[488:508].strip()}
    return(dicy)

dic_list=[tgt_fields(item) for item in raw_list]
dic_list=[simple_fields(item) for item in raw_list]

#cases where birthdate did not conform to %Y-%m-%d, will need to be handled... some cases are %Y-%m
dl_df=pd.DataFrame(dic_list)

dl_df.dtypes

adrienne_dist=dl_df.loc[dl_df['ad']=='74',]
#convert the birthdates to datetime
adrienne_dist['birthdate_dt']=adrienne_dist.birthdate.apply(lambda x: datetime.datetime.strptime(x.strip(),"%Y%m%d"))
adrienne_dist.dtypes

adrienne_dist['age']
test2=adrienne_dist['birthdate_dt'].apply(lambda x:round((datetime.datetime.now()-x).days/365.25,1))
test2.head()

adrienne_dist['age']=test2

select=adrienne_dist.loc[:,['firstname','lastname','party','emsid','housenumber','housesuffix','streetname','apt','zipcode','phone','registrationdate','age','birthdate_dt','senatedist','congressionaldist','councildist','ad']]




select.to_csv('/home/tio/Documents/politics/adrienne/voter_registration.csv',index=False)



emsid_l=[g['emsid'] for g in dic_list]

del(raw_list)
#add a compatible district key, val in the dictionary
for dic in dic_list:
    if len(dic['electiondist'])==2:
        clean_ed='0'+dic['electiondist']
    else:
        clean_ed=dic['electiondist']
    dic['compatED']=dic['ad']+clean_ed
    dic['boe_report_end']=datetime.datetime.strptime('10-30-2017',"%m-%d-%Y")
    #dic['boe_report_begin']=datetime.datetime.strptime('01-01-1940',"%m-%d-%Y")
    dic['date_of_record']=datetime.datetime.now()

'''mongo updating (updates here and insert below)'''
maxSevSelDelay=5
client = pymongo.MongoClient(host='localhost',port=27017,serverSelectionTimeoutMS=maxSevSelDelay)
client.database_names()
db=client.voter_history
voters=db.voters2

#exclude these keys from the dictionary
exclude=['firstname','lastname','sex','emsid']

#for pushing to the end of the voter_registration array... argument "upsert" controls creation of a new document should the emsid not pre-exist
samp=dic_list[4:]
kj=[g['emsid'] for g in samp]
high_keys=['emsid','lastname','firstname','birthdate','sex','phone']
#build keys for the 'registration' embedded document
regis_keys=[key for key in list(dic_list[9]) if key not in high_keys]
bool_list=[]
for i in dic_list[4:]:
    reg_doc={m:g[m] for m in regis_keys}
    upd=voters.update_one({'emsid':i['emsid']},
                 {'$push': {
                         'registration_hist':reg_doc}},upsert=False)
    bool_list.append(upd.matched_count)        
#for pushing to a specific position in the voter_registration array
for i in samp:
    newDic={k: i[k] for k in i if k not in exclude}
    voters.update_one({'emsid':i['emsid']},
                 {'$push': {
                         'registration_hist':{
                                 '$each':[newDic], '$position':0}
                         }})
        
        
''' Reconcile for any voters that had not existed in the database... add these as entirely new documents'''
#mongo query for all emsid in the database, not including emsid... this is better than hitting the databse a bunch of times... better yet, run the above 'update' procedure and write to a boolean list if an update was successful... can then use the boolean list to index the BOE file and discover those emsids not in the database
rs=voters.find({},{'_id':0,'emsid':1})
#voters w/o a registration_hist array
no_regList=voters.find({'voter_history':{'$exists':False}},{'_id':0,'emsid':1})
all_emsid_list = list(rs)
nrl_emsid_list = list(no_regList)
mongo_emsid=[g['emsid'] for g in nrl_emsid_list]

#list of unique emsid from the BOE file
emsid_list=[f['emsid'] for f in dic_list]

boeEmsidNotIn=np.setdiff1d(emsid_list,mongo_emsid)


'''for those emsids not already in the mongodb, create new documents'''
#create two documents for every emsid: the top level doc and the registration_hist doc (placed in an array)

#discover those emsids in the registration file not in mongodb
bool_index=[g!=1 for g in bool_list]
#boolean indexing of a list
dic_list_nodb=list(compress(dic_list[4:],bool_index))
#verification
[g['emsid'] for g in dic_list_nodb[0:7]]
high_keys=['emsid','lastname','firstname','birthdate','sex','phone']

#build keys for the 'registration' embedded document
regis_keys=[key for key in list(dic_list_nodb[9]) if key not in high_keys]
for g in dic_list_nodb:
    highLevel={k:g[k] for k in high_keys}
    reg_doc={m:g[m] for m in regis_keys}
        #can try to do this via array, but will need to run a $push for array
    voters.insert_one(highLevel)
    voters.update_one({'emsid':g['emsid']},{'$push':{'registration_hist':reg_doc}})
        
        
#run a test of the above
for g in dic_list_nodb[2:]:
    highLevel={k:g[k] for k in high_keys}
    reg_doc={m:g[m] for m in regis_keys}
        #can try to do this via array, but will need to run a $push for array
    voters.insert_one(highLevel)
    voters.update_one({'emsid':g['emsid']},{'$push':{'registration_hist':reg_doc}})

#fixing the above
regis_keys=[key for key in list(dic_list_nodb[9]) if key not in high_keys]
for g in dic_list_nodb:
    highLevel={k:g[k] for k in high_keys}
    reg_doc={m:g[m] for m in regis_keys}
        #can try to do this via array, but will need to run a $push for array
    voters.insert_one(highLevel)
    voters.update_one({'emsid':g['emsid']},{'$set':{'registration_hist':reg_doc}})        

onevalue=[g for g in dic_list if g['emsid']=='00109883']

highLevel={k:onevalue[0][k] for k in high_keys}
reg_doc={m:onevalue[0][m] for m in regis_keys}
    
voters.update_one({'emsid':'00109883'},{'$set':{'registration_hist':[reg_doc]}})                
''' data CLEANIUP: those voters that did not receive a registration update'''

#query for the voters
rs=voters.find({'voter_history':{'$elemMatch':
    {'election_date':{'$lt':datetime.datetime.strptime("2009-09-04","%Y-%m-%d")},
    'election_date':{'$gt':datetime.datetime.strptime("2009-09-02","%Y-%m-%d")}}},
    'registration_hist.boe_report_end':{'$not':{'$exists':True}}},
    {'_id':0,'emsid':1})    
rs_l=list(rs)        
mongo_emsid=[i['emsid'] for i in rs_l]
pd.Series(mongo_emsid).to_csv('/home/tio/Documents/politics/jabari_parker/voterRegistrationFiles/voted2009_noRegistrationData.csv',index=False)
mongo_emsid[128835] in emsid_l

'00158036' in emsid_l

emsid_lackUpdate=voters.find({'registration_hist.boe_report_end':{'$exists':False}},{'emsid':1,'_id':0})
iu=list(emsid_lackUpdate)
emsidMongo_lackUpdate=[id_['emsid'] for id_ in iu]

samp=emsidMongo_lackUpdate[5:]
samp
for i in dic_list:
    if i['emsid'] in samp:
        newDic={k: i[k] for k in i if k not in exclude}
        voters.update_one({'emsid':i['emsid']},
                 {'$push': {
                         'registration_hist':{
                                 '$each':[newDic], '$position':0}}})
                         
''' end data CLEANUP '''                         
                         



'''Geocoding project distribution of dataset'''
#split list for geocoding delegation
sixths=len(dic_list)/6

for i in list(range(0,6)):
    j=i+1
    i_1=int(sixths*i)
    j_1=int(sixths*j)
    portion=df.iloc[i_1:j_1,:]
    name='voter_registration_'+str(i)+'.csv'
    placement='/home/tio/Documents/politics/jabari_parker/voterRegistrationFiles/aug_25_boeFile_allBK/'+name
    portion.to_csv(placement,index=False)

df_raw=pd.DataFrame(dic_list)
dist_35=df_raw[df_raw['councildist']=='35']
dist_35.to_csv('/home/tio/Documents/politics/jabari_parker/voterRegistrationFiles/aug_25_boeFile_allBK/district_35_only.csv',index=False) 

''' end geocoding project '''

'''for the greens project, select only greens'''
party_list=[]
for elem in dic_list:
    party_list.append(elem['party'])

#Green party code = 'GRE'
dic_list[0].keys()
dic_list[0]
select_not_greens=[g for g in dic_list if g['party']!='GRE']

del(select_not_greens)
select_greens[0].keys()
select_greens[0]['emsid','sex']
del([raw_list])
d={}
d['name_of_voter']=uno['firstname']+' '+uno['lastname']
#run these greens through the geocoder
''' end greens portion '''