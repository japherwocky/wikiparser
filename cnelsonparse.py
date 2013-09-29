from sys import stdin, stdout
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
from time import time
from collections import defaultdict


Corpus = defaultdict( lambda: 0)

class TextExtractor( ContentHandler):

	def startDocument( self):
		self.start = time()
		self.lap = time()
		self.buffer = []
		print time()
		self.doctotal = 0

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
			self.title = unicode().join( self.buffer).encode('UTF8') 
			self.doctotal +=1

		elif name == 'text':
			words = unicode().join(self.buffer)
			for word in filter( words):
				if word == 'cnelson': print self.title

			
	def endDocument(self):
		print time()
		print self.doctotal

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
