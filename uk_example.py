import numpy as np
import pandas as pd
import math
import glob
import gzip
import json
import datetime


def tweets_in_uk(file):
    count=0
    with gzip.open(file,"rt") as fh:#open the file
        for line in fh: #for each line
            tweet=json.loads(line)#load json
            tweet_text= tweet["text"]#get text of tweet
            if "United Kingdom" in tweet_text:
                count+=1#add 1 to count if "United Kingdom" is in the text
    return count


if __name__=="__main__":
    start=datetime.datetime.now()
    uk_tweets=0
    for file in glob.glob("../lectures/data/twitter-geo/2016-01-*.gz"):
        uk_tweets+=tweets_in_uk(file)
    end=datetime.datetime.now()
    print(uk_tweets)
