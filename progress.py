from sys import stdin
from time import time
def main():
	start = 1261514647
	elapsed = (time() - start) / 60.0
	articles = int( stdin.read().split()[0] )
	avg = articles / elapsed
	complete = articles / 3200000.0

	print '%f min elapsed, avg %f articles / min, %f%% complete' % (elapsed, avg, complete)

if __name__=="__main__": main()
