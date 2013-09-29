from sys import stdin, stdout
stdout = open('optparse.log', 'w')
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
from time import time
from collections import defaultdict


import pytc
hdb = pytc.HDB()
#hdb.tune(5000000, 4, 10, pytc.HDBTTCBS)
hdb.open( 'optcorpus.tch', pytc.BDBOWRITER | pytc.BDBOREADER | pytc.BDBOCREAT)

Corpus = defaultdict( lambda: 0)

class TextExtractor( ContentHandler):
	doctotal = 0

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
			self.doctotal +=1
		elif name == 'text':
			words = unicode().join(self.buffer)
			for word in filter( words):
				if len( word) > 3:
					Corpus[word] += 1

			Corpus = self.flushCorpus( Corpus)

			stdout.write( 'Done in %f sec\n'%(time() - self.lap) )
			self.lap = time()
			if not self.doctotal % 500000:
				print 'Optimizing ', time()
				hdb.optimize()
			
	def endDocument(self):
		stdout.write( 'Document complete in %f seconds' % (time() - self.start) )

	def flushCorpus(self, Corpus):
		for word in Corpus:
			hdb.addint(word, Corpus[word])

		return defaultdict( lambda: 0)

import re
nonalphanum = re.compile('\W')

def filter( text):
	def spacer( matchobj): return ' '
	text = re.sub( nonalphanum, spacer, text)
	words = text.split()
	words = [word.lower() for word in words]
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
