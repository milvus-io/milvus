include ../../make.inc

#######################################################################
#  This is the makefile to create a the variants libraries for LAPACK.
#  The files are organized as follows:
#       CHOLRL -- Right looking block version of the algorithm, calling Level 3 BLAS
#       CHOLTOP -- Top looking block version of the algorithm, calling Level 3 BLAS
#       LUCR -- Crout Level 3 BLAS version of LU factorization
#       LULL -- left-looking Level 3 BLAS version of LU factorization
#       QRLL -- left-looking Level 3 BLAS version of QR factorization
#       LUREC -- an iterative version of Sivan Toledo's recursive LU algorithm[1].
#       For square matrices, this iterative versions should
#       be within a factor of two of the optimum number of memory transfers.
#
# [1] Toledo, S. 1997. Locality of Reference in LU Decomposition with
#  Partial Pivoting. SIAM J. Matrix Anal. Appl. 18, 4 (Oct. 1997),
#  1065-1081. http://dx.doi.org/10.1137/S0895479896297744
#######################################################################

CHOLRL = cholesky/RL/cpotrf.o cholesky/RL/dpotrf.o cholesky/RL/spotrf.o cholesky/RL/zpotrf.o

CHOLTOP = cholesky/TOP/cpotrf.o cholesky/TOP/dpotrf.o cholesky/TOP/spotrf.o cholesky/TOP/zpotrf.o

LUCR = lu/CR/cgetrf.o lu/CR/dgetrf.o lu/CR/sgetrf.o lu/CR/zgetrf.o

LULL = lu/LL/cgetrf.o lu/LL/dgetrf.o lu/LL/sgetrf.o lu/LL/zgetrf.o

LUREC = lu/REC/cgetrf.o lu/REC/dgetrf.o lu/REC/sgetrf.o lu/REC/zgetrf.o

QRLL = qr/LL/cgeqrf.o qr/LL/dgeqrf.o qr/LL/sgeqrf.o qr/LL/zgeqrf.o qr/LL/sceil.o


all: cholrl.a choltop.a lucr.a lull.a lurec.a qrll.a

cholrl.a: $(CHOLRL)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

choltop.a: $(CHOLTOP)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

lucr.a: $(LUCR)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

lull.a: $(LULL)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

lurec.a: $(LUREC)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

qrll.a: $(QRLL)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

clean: cleanobj cleanlib
cleanobj:
	rm -f $(CHOLRL) $(CHOLTOP) $(LUCR) $(LULL) $(LUREC) $(QRLL)
cleanlib:
	rm -f *.a

.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<
