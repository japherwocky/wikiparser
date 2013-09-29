# generally run like  wget -O - -o /dev/null http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2 | bunzip2 | python wikiparse.py

from sys import stdin
from xml.sax.handler import ContentHandler
from xml.sax import make_parser
from time import time
from collections import defaultdict

import sqlite3
sqlite = sqlite3.Connection( 'corpus.db')
cursor = sqlite.cursor()

Corpus = defaultdict( lambda: 0)

class TextExtractor( ContentHandler):

    def startDocument( self):
        self.start = time()
        self.lap = time()
        self.buffer = []
        print 'Starting at %f' % self.start

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
            print 'Parsing %s.. '% (unicode().join( self.buffer).encode('UTF8') )
        elif name == 'text':
            words = unicode().join(self.buffer)
            for word in filter( words):
                if len( word) > 3:
                    Corpus[word] += 1

            Corpus = self.flushCorpus( Corpus)

            print 'Done in %f sec\n'%(time() - self.lap)
            self.lap = time()
            
    def endDocument(self):
        print 'Document complete in %f seconds' % (time() - self.start)

    def flushCorpus(self, Corpus):
        print 'Flushing corpus of %s words'%(len(Corpus.keys()))
        for word in Corpus:
            cursor.execute('SELECT tally FROM words WHERE word = ?',(word,))
            count = cursor.fetchone()
            if count:
                count = count[0]
                cursor.execute( 'UPDATE words SET tally = ? WHERE word = ?', (Corpus[word]+count,word))
            else:
                cursor.execute( 'INSERT INTO words VALUES (?,?)', (word, Corpus[word]))

        sqlite.commit()

        return defaultdict( lambda: 0)

import re, string
nonalphanum = re.compile('\W')

def filter( text):

    words = [w.encode('ascii', 'ignore').lower().translate( string.maketrans("",""), string.punctuation+string.digits) for w in text.split()]
    words = [w for w in words if w]


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

if __name__ == '__main__': main()
