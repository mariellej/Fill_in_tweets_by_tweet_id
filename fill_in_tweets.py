# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 10:53:00 2017

@author: marielle.jurist
"""
#####******************** EDIT THE FOLLOWING CODE BELOW ******************
# initialize file names and paths


# what file contains the twitter IDs you want to pull info for?
# Note: Make sure the sheet name in excel is called "Sheet1" !!!!
file2Load = 'C:/Users/marielle.jurist/Documents/join_test/test2.xlsx'

# where do you want the file to be saved?
file2savePath = 'C:/Users/marielle.jurist/Documents/join_test/'
# what do you want the file to be called? ** Make sure to include ".xlsx"
file2saveName = 'API_test.xlsx'


#####*************** DO NOT EDIT THE FOLLOWING CODE BELOW ***************
import tweepy
import pandas as pd
import time
import numpy as np
import sys
from urllib.parse import urlparse


def getExceptionMessage(msg):
    words = msg.split(' ')

    errorMsg = ""
    for index, word in enumerate(words):
        if index not in [0,1,2]:
            errorMsg = errorMsg + ' ' + word
    errorMsg = errorMsg.rstrip("\'}]")
    errorMsg = errorMsg.lstrip(" \'")

    return errorMsg


# load file
GUID_df = pd.read_excel(file2Load, 'Sheet1')
# Create filepath for new file
file2Save = file2savePath+file2saveName

#print(len(GUID_df['URL']))

temp_df = GUID_df
url = []
for l in range(len(GUID_df['URL'])):
   GUID_df['GUID'][l] = (temp_df['URL'][l].rsplit('/',1)[-1])


#print(len(GUID_df['URL']))

#set GUID column to type string
#GUID_df['GUID'] = GUID_df['GUID'].astype(str)
#GUID_df['GUID'] = [string.lower() for string in GUID_df['GUID']]

#seaprate data fram into complete and null values

is_null_df = GUID_df[GUID_df['Date.(EST)'].isnull()]
#print(len(is_null_df['URL']))

merged = GUID_df.merge(is_null_df, indicator=True, how='outer')

is_complete_df = merged[merged['_merge'] == 'left_only']


#print(len(is_complete_df))


#-----------------------------------------

#Initializing auth credentials
consumer_key = 'wu3nboSJ8DNRAP43gZpUURD0R'
consumer_secret  = 'EUENmFSOiAG9Rjv6KrNGSNZzT1VEGbqYfTNh1bI03GGEEkChQU'

access_token = '875719782351466497-HceyOenceUoP6TJqJ8gKqlKOPwbmqDB'
access_token_secret = 'FcdFI0X0oBBlXriOiGhS2t1UlNATukWCYjzY0m3D9lMvZ'

#build Auth handler
auth = tweepy.OAuthHandler(consumer_key = consumer_key, consumer_secret = consumer_secret)
#create insatnce of API
api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

#equip auth handler with access token
auth.set_access_token(access_token, access_token_secret)

#-----------------------

        
df_copy = is_null_df.copy()

#Iterating through DF, 100 at a time
rows = is_null_df.shape[0]

for i in range((rows // 100) + 1):
    
    ids = list(df_copy.iloc[i*100:(i*100+99),0])
    #print("printing ids:")
    #print(ids)

    #check if id list is empty
    numids = len(ids)
    if numids==0:
            print("Done Scraping ids")
            break
    else:
        try:
            #change data type of each id
            #for x in range(numids):
                #ids[x] = np.asscalar(ids[x])
            
            #change to comma separated string
            #idstring = ",".join([str(j) for j in ids])
            
            
            response = api.statuses_lookup(id_ = ids, include_entities=False) # get tweets
            print("received responses", i*100, "through", i*100 +99) #statuses_lookup call worked
        except tweepy.TweepError as e:
            print(e.api_code)
            print(getExceptionMessage(e.reason))
            sys.exit()
            # Wait five minutes, then reset authentication and API object
            # and try again
            print("Try Again in 5 minutes")
            time.sleep(60*5)
            auth = tweepy.OAuthHandler(consumer_key = consumer_key, consumer_secret = consumer_secret)
            api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
            try:
                response = api.statuses_lookup(ids) # get ids
            except tweepy.TweepError:
                # Wait fifteen minutes, then reset authentication and API object
                # and try again
                print("Try Again in 15 minutes")
                time.sleep(60*15)
                auth = tweepy.OAuthHandler(consumer_key = consumer_key, consumer_secret = consumer_secret)
                api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
                try:
                    response = api.statuses_lookup(ids) # get ids
                except tweepy.TweepError:
                    print("Something is broken, look into it")
                    break
        #print(len(response))
        for tweet in response:
            json = tweet._json
            index = is_null_df[is_null_df['GUID']== json['id']].index
    
            is_null_df.set_value(index, 'Date.(EST)', json['created_at'])
            is_null_df.set_value(index, 'Contents', json['text'])
            is_null_df.set_value(index, 'Author', json['user']['screen_name'])
            is_null_df.set_value(index, 'Name', json['user']['name'])
            is_null_df.set_value(index, 'Posts', json['user']['statuses_count'])
            is_null_df.set_value(index, 'Followers', json['user']['followers_count'])
            is_null_df.set_value(index, 'Following', json['user']['friends_count'])
            print(is_null_df.loc[index])
    
        

print("Done Scraping info")

print(is_null_df)
dataframes= [is_null_df, is_complete_df]
#rejoin the 2 dataframes
joined_df = pd.concat(dataframes)
joined_df.to_excel(file2Save, 'twitter_info', index = False)
