import glob
import gzip
import json
import pickle
import numpy as np
from load_countries import loadCountries

def map_func(rawtweet):
    tweet=json.loads(rawtweet)#Load it with json
    if "text" in tweet:
        for i in range(len(countries)):
            if countries[i] in tweet["text"]:
                output (i,1)
    
def reduce_func(key,values):
    return (key,sum(values))#key is an index from countries
    

if __name__=="__main__":
	countries=loadCountries()

	from collections import defaultdict
	map_out=defaultdict(list)
	def output(key,val):
		map_out[key].append(val)

	for file in glob.glob("../lectures/data/twitter-geo/2016-01-*.gz"):
		with gzip.open(file,"rt") as fh:
			for line in fh:
				map_func(line)					

	final_out={}
	for key,vals in map_out.items():
		k,v=reduce_func(key,vals)
		final_out[k]=v
	map_out=None
