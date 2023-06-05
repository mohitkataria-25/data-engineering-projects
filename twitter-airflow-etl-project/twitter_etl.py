#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  3 22:05:12 2023

@author: mohitkataria
"""

import pandas as pd
import tweepy
import json
from datetime import datetime
import s3fs


def run_twitter_etl():

    access_key = "CnnbMXtraRxTVTGuZWRrzFvQP"
    access_secret = "gSKhiI4K1RxH65S6x2KaEwzn0KtPlALp0Q8dP91HJswniDm5UM"
    consumer_key = "1379759264378585097-YmNWQ9vNGdSzRwNomL5nhmmUVQKyHz"
    consumer_secret = "Jfvu8tbQayqchaoyWFZmiMXYZvbU8nJFZc0jzld99zKYQ"
    
    #Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)
    
    #Creating an API object
    api = tweepy.API(auth)
    
    tweets = api.user_timeline(screen_name = '@elonmusk',
                               count = 200,
                               include_rts = False,
                               tweet_mode='extended'
                               )
    
    tweet_list = []
    
    for tweet in tweets:
        text = tweet._json["full_text"]
        
        redefined_tweet = {"user": tweet.user.screen_name,
                           "text": text,
                           "favorite_count": tweet.favorite_count,
                           "retweet_count": tweet.retweet_count,
                           "created_date": tweet.created_at}
        
        tweet_list.append(redefined_tweet)
        
    
    df = pd.DataFrame(tweet_list)
    
    df.to_csv("s3://mo-se-version-2/elonmusk_twitterdata.csv")
    

