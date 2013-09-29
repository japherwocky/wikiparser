from sys import stdin, stdout
stdout = open('corpus2.log', 'w')
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
from time import time
from collections import defaultdict

import pytc
db = pytc.HDB()
db.tune( 30000000, 4, 10, pytc.HDBTDEFLATE)
db.open( 'corpus2.tch', pytc.BDBOWRITER | pytc.BDBOREADER | pytc.BDBOCREAT)

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
			stdout.write( '(%f)%s:'% (time(),unicode().join( self.buffer).encode('UTF8') ))
			self.startarticle = time()
		elif name == 'text':
			words = unicode().join(self.buffer)

			for word in filter( words):
				if len( word) > 2:
					Corpus[word] += 1

			wordcount = len( Corpus)

			Corpus = self.flushCorpus( Corpus)

			elapsed = time() - self.lap
			avg = wordcount / elapsed

			stdout.write( '%d words in %f sec (%f wps)\n'%(wordcount, elapsed, avg) )
			self.lap = time()
			
	def endDocument(self):
		stdout.write( 'Document complete in %f seconds' % (time() - self.start) )

	def flushCorpus(self, Corpus):
		for word in Corpus:
			if db.has_key(word):
				count = int(db.get(word)) + Corpus[word]
			else: count = Corpus[word]
			db.put( word, str( count))

		return defaultdict( lambda: 0)

import re
nonalphanum = re.compile('[\W-]')

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
