from sys import stdin, stdout
stdout = open('mongoprogress.log', 'w')
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
from time import time
from collections import defaultdict

from stem import PorterStemmer
S = PorterStemmer()
def stem( word):
	try:
		return S.stem(word, 0, len(word)-1)
	except Exception, e:
		#import pdb;pdb.set_trace()
		#log it or something
		return ' '


"""
import pytc
hdb = pytc.HDB()
#hdb.tune(5000000, 4, 10, pytc.HDBTTCBS)
hdb.open( 'intcorpus.tch', pytc.BDBOWRITER | pytc.BDBOREADER | pytc.BDBOCREAT)
"""

import pymongo
C = pymongo.Connection().corpusdb

Corpus = defaultdict( lambda: 0)

class TextExtractor( ContentHandler):

	def startDocument( self):
		self.start = time()
		self.lap = time()
		self.buffer = []
		stdout.write('Starting at %f' % self.start)

	def startElement( self, name, attrs):
		if name == 'title' or name == 'text': 
			self.buffer = []

		

	def characters(self, content):
		if content:
			self.buffer.append(content)

	def endElement(self, name):
		global Corpus
		if name == 'title':
			#stdout.write( 'Parsing %s.. '% (unicode().join( self.buffer).encode(stdout.encoding)) )
			stdout.write( 'Parsing %s.. '% (unicode().join( self.buffer).encode('UTF8') ))
		elif name == 'text':
			words = unicode().join(self.buffer)
			for word in filter( words):
				if len( word) > 2:
					Corpus[word] += 1

			Corpus = self.flushCorpus( Corpus)

			stdout.write( 'Done in %f sec\n'%(time() - self.lap) )
			self.lap = time()
			
	def endDocument(self):
		stdout.write( 'Document complete in %f seconds' % (time() - self.start) )

	def flushCorpus(self, Corpus):
		for word in Corpus:
			entry = C.df.find_one( {'term':word})
			if entry:
				entry['count'] += 1
				C.df.save( entry)
			else:
				C.df.insert( {'term':word, 'count':1})
			

		return defaultdict( lambda: 0)

import re
nonalphanum = re.compile('\W')
digital = re.compile( '\d')

def filter( text):
	def spacer( matchobj): return ' '
	text = re.sub( nonalphanum, spacer, text)
	words = text.split()
	words = [word.lower() for word in words]
	words = [ stem(word) for word in words if not digital.search( word)]
	return words


from xml.sax.handler import feature_namespaces
def main():

	parser = make_parser()
	#parser.setFeature( feature_namespaces, 0)
	parser.setContentHandler( TextExtractor()) 

	def stdingenerator():
		for i in range(100):
			yield stdin.read( 1024)

	parser.parse( stdin)
	"""
	for chunk in stdingenerator():
		parser.parseString( chunk)
	"""

if __name__ == '__main__': main()
