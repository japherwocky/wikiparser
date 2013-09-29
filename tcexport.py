
import re


import pytc
hdb = pytc.HDB()
hdb.open( 'intcorpus.tch', pytc.BDBOWRITER | pytc.BDBOREADER | pytc.BDBOCREAT)

import pymongo
C = pymongo.Connection( 'localhost').corpusdb

def main():
	numbers = re.compile( r'\d')
	keys = open('keys').xreadlines()
	for key in keys:
		if not numbers.search( key):
			count = hdb.addint( key[:-1], 0)
			C.df.insert( {'term':key[:-1], 'count':count})

if __name__ == '__main__': main()
