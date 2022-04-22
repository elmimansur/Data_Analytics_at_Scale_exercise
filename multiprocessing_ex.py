import numpy as np
import pandas as pd
import multiprocessing
import glob
import gzip
import json
import pickle
import datetime
from functools import partial
from load_countries import loadCountries

from ex_singlethreaded import process_file

def country_counts_multiprocess(files,countries):
    process_file_fixed_countries=partial(process_file,countries=countries)
    #Start up a pool and run process_file for each file in the list files
    with multiprocessing.Pool() as pool:
        counts=np.zeros(len(countries),dtype="int")
        results=pool.map(process_file_fixed_countries, files,chunksize=1)
        for r in results:
            counts+=r
    return counts


if __name__=="__main__":
    countries=loadCountries()

    start=datetime.datetime.now()
    multiprocess_results=country_counts_multiprocess(glob.glob("../lectures/data/twitter-geo/2016-01-*.gz"),countries)
    end=datetime.datetime.now()
    print(f"Finished in {end-start}")
    
    df=pd.DataFrame({"country":countries,"count":multiprocess_results}).sort_values("count",ascending=False)
    print(df.head(10))   
 

