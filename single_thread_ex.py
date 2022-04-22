import multiprocessing
import numpy as np
import pandas as pd
import math
import glob
import gzip
import json
import pickle
import datetime
from load_countries import loadCountries

def process_file(file,countries):
    counts=np.zeros(len(countries),dtype="int") #This array should contain the counts of all countries in the same order as countries
    with gzip.open(file,"rt") as fh:#open the file
        for line in fh: #for each line
            tweet=json.loads(line)#load json
            tweet_text= tweet["text"]#get text of tweet
            for i in range(len(countries)):
                if countries[i] in tweet_text:
                    counts[i]+=1
    return counts


def country_counts_single_thread(files,countries):
    counts=np.zeros(len(countries),dtype="int")
    for file in files:
        counts+=process_file(file,countries)
    return counts

if __name__=="__main__":
    countries=loadCountries()
    start=datetime.datetime.now()
    results_singlethreaded=country_counts_single_thread(glob.glob("../lectures/data/twitter-geo/2016-01-*.gz"),countries)
    end=datetime.datetime.now()

    print(results_singlethreaded[0:10])
    print("A total of {} country names were found.".format(results_singlethreaded.sum()))
    max_index=np.argmax(results_singlethreaded)
    print("The country appearing the most {} with {} occurences.".format(countries[int(max_index)],results_singlethreaded[max_index]))
    print("The execution time was {} seconds".format(end-start))
