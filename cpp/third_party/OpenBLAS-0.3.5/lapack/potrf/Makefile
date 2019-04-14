TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = spotrf_U_single.$(SUFFIX) spotrf_L_single.$(SUFFIX)
DBLASOBJS = dpotrf_U_single.$(SUFFIX) dpotrf_L_single.$(SUFFIX)
QBLASOBJS = qpotrf_U_single.$(SUFFIX) qpotrf_L_single.$(SUFFIX)
CBLASOBJS = cpotrf_U_single.$(SUFFIX) cpotrf_L_single.$(SUFFIX)
ZBLASOBJS = zpotrf_U_single.$(SUFFIX) zpotrf_L_single.$(SUFFIX)
XBLASOBJS = xpotrf_U_single.$(SUFFIX) xpotrf_L_single.$(SUFFIX)

ifdef SMP
SBLASOBJS += spotrf_U_parallel.$(SUFFIX) spotrf_L_parallel.$(SUFFIX)
DBLASOBJS += dpotrf_U_parallel.$(SUFFIX) dpotrf_L_parallel.$(SUFFIX)
QBLASOBJS += qpotrf_U_parallel.$(SUFFIX) qpotrf_L_parallel.$(SUFFIX)
CBLASOBJS += cpotrf_U_parallel.$(SUFFIX) cpotrf_L_parallel.$(SUFFIX)
ZBLASOBJS += zpotrf_U_parallel.$(SUFFIX) zpotrf_L_parallel.$(SUFFIX)
XBLASOBJS += xpotrf_U_parallel.$(SUFFIX) xpotrf_L_parallel.$(SUFFIX)
endif

spotrf_U_single.$(SUFFIX) : potrf_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotrf_L_single.$(SUFFIX) : potrf_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotrf_U_parallel.$(SUFFIX) : potrf_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotrf_L_parallel.$(SUFFIX) : potrf_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dpotrf_U_single.$(SUFFIX) : potrf_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotrf_L_single.$(SUFFIX) : potrf_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotrf_U_parallel.$(SUFFIX) : potrf_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotrf_L_parallel.$(SUFFIX) : potrf_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qpotrf_U_single.$(SUFFIX) : potrf_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotrf_L_single.$(SUFFIX) : potrf_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotrf_U_parallel.$(SUFFIX) : potrf_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotrf_L_parallel.$(SUFFIX) : potrf_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

cpotrf_U_single.$(SUFFIX) : potrf_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotrf_L_single.$(SUFFIX) : potrf_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotrf_U_parallel.$(SUFFIX) : potrf_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotrf_L_parallel.$(SUFFIX) : potrf_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zpotrf_U_single.$(SUFFIX) : potrf_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotrf_L_single.$(SUFFIX) : potrf_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotrf_U_parallel.$(SUFFIX) : potrf_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotrf_L_parallel.$(SUFFIX) : potrf_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xpotrf_U_single.$(SUFFIX) : potrf_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotrf_L_single.$(SUFFIX) : potrf_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotrf_U_parallel.$(SUFFIX) : potrf_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotrf_L_parallel.$(SUFFIX) : potrf_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

spotrf_U_single.$(PSUFFIX) : potrf_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotrf_L_single.$(PSUFFIX) : potrf_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotrf_U_parallel.$(PSUFFIX) : potrf_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotrf_L_parallel.$(PSUFFIX) : potrf_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dpotrf_U_single.$(PSUFFIX) : potrf_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotrf_L_single.$(PSUFFIX) : potrf_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotrf_U_parallel.$(PSUFFIX) : potrf_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotrf_L_parallel.$(PSUFFIX) : potrf_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qpotrf_U_single.$(PSUFFIX) : potrf_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotrf_L_single.$(PSUFFIX) : potrf_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotrf_U_parallel.$(PSUFFIX) : potrf_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotrf_L_parallel.$(PSUFFIX) : potrf_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

cpotrf_U_single.$(PSUFFIX) : potrf_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotrf_L_single.$(PSUFFIX) : potrf_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotrf_U_parallel.$(PSUFFIX) : potrf_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotrf_L_parallel.$(PSUFFIX) : potrf_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zpotrf_U_single.$(PSUFFIX) : potrf_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotrf_L_single.$(PSUFFIX) : potrf_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotrf_U_parallel.$(PSUFFIX) : potrf_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotrf_L_parallel.$(PSUFFIX) : potrf_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xpotrf_U_single.$(PSUFFIX) : potrf_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotrf_L_single.$(PSUFFIX) : potrf_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotrf_U_parallel.$(PSUFFIX) : potrf_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotrf_L_parallel.$(PSUFFIX) : potrf_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

include ../../Makefile.tail
