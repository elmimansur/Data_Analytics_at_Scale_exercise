def loadCountries(filename="countryInfo.txt"):
	#Expected format is https://download.geonames.org/export/dump/countryInfo.txt
	countries=[]
	with open(filename) as fh:
		for line in fh:
			if line[0]=="#":
				continue
			else:
				countries.append(line.split("\t")[4].replace(".","").strip())
	return countries

if __name__=="__main__":
	print(loadCountries("countryInfo.txt"))
