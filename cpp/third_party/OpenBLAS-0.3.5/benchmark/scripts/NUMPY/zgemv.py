#!/usr/bin/python

import os
import sys
import time
import numpy
from numpy.random import randn

def run_zgemv(N,l):

	A = randn(N,N).astype('float64') + randn(N,N).astype('float64') * 1j;
	B = randn(N).astype('float64') + randn(N).astype('float64') * 1j;

	start = time.time();
	for i in range(0,l):
		ref = numpy.dot(A,B)
	end = time.time()
	
	timediff = (end -start) 
	mflops = ( 8*N*N) *l / timediff
	mflops *= 1e-6

	size = "%dx%d" % (N,N)
	print("%14s :\t%20f MFlops\t%20f sec" % (size,mflops,timediff))


if __name__ == "__main__":
	N=128
	NMAX=2048
	NINC=128
	LOOPS=1

	z=0
	for arg in sys.argv:
		if z == 1:
			N = int(arg)
		elif z == 2:
			NMAX = int(arg)
		elif z == 3:
			NINC = int(arg)
		elif z == 4:
			LOOPS = int(arg)

		z = z + 1

	if 'OPENBLAS_LOOPS' in os.environ:
		p = os.environ['OPENBLAS_LOOPS']
		if p:
			LOOPS = int(p);

	print("From: %d To: %d Step=%d Loops=%d" % (N, NMAX, NINC, LOOPS))
	print("\tSIZE\t\t\tFlops\t\t\t\t\tTime")

	for i in range (N,NMAX+NINC,NINC):
		run_zgemv(i,LOOPS)

