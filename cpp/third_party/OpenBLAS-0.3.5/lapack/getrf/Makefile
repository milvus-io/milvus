TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = sgetrf_single.$(SUFFIX)
DBLASOBJS = dgetrf_single.$(SUFFIX)
QBLASOBJS = qgetrf_single.$(SUFFIX)
CBLASOBJS = cgetrf_single.$(SUFFIX)
ZBLASOBJS = zgetrf_single.$(SUFFIX)
XBLASOBJS = xgetrf_single.$(SUFFIX)

ifdef SMP
SBLASOBJS += sgetrf_parallel.$(SUFFIX)
DBLASOBJS += dgetrf_parallel.$(SUFFIX)
QBLASOBJS += qgetrf_parallel.$(SUFFIX)
CBLASOBJS += cgetrf_parallel.$(SUFFIX)
ZBLASOBJS += zgetrf_parallel.$(SUFFIX)
XBLASOBJS += xgetrf_parallel.$(SUFFIX)
endif

ifeq ($(USE_OPENMP), 1)
GETRF_SRC = getrf_parallel_omp.c
else
GETRF_SRC = getrf_parallel.c
endif

sgetrf_single.$(SUFFIX) : getrf_single.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

dgetrf_single.$(SUFFIX) : getrf_single.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

qgetrf_single.$(SUFFIX) : getrf_single.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

cgetrf_single.$(SUFFIX) : getrf_single.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

zgetrf_single.$(SUFFIX) : getrf_single.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

xgetrf_single.$(SUFFIX) : getrf_single.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

sgetrf_parallel.$(SUFFIX) : $(GETRF_SRC) ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

dgetrf_parallel.$(SUFFIX) : $(GETRF_SRC) ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

qgetrf_parallel.$(SUFFIX) : $(GETRF_SRC) ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

cgetrf_parallel.$(SUFFIX) : $(GETRF_SRC) ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

zgetrf_parallel.$(SUFFIX) : $(GETRF_SRC) ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

xgetrf_parallel.$(SUFFIX) : $(GETRF_SRC) ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

sgetrf_single.$(PSUFFIX) : getrf_single.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

dgetrf_single.$(PSUFFIX) : getrf_single.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

qgetrf_single.$(PSUFFIX) : getrf_single.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

cgetrf_single.$(PSUFFIX) : getrf_single.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

zgetrf_single.$(PSUFFIX) : getrf_single.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

xgetrf_single.$(PSUFFIX) : getrf_single.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

sgetrf_parallel.$(PSUFFIX) : $(GETRF_SRC)
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

dgetrf_parallel.$(PSUFFIX) : $(GETRF_SRC)
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

qgetrf_parallel.$(PSUFFIX) : $(GETRF_SRC)
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DUNIT $< -o $(@F)

cgetrf_parallel.$(PSUFFIX) : $(GETRF_SRC)
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

zgetrf_parallel.$(PSUFFIX) : $(GETRF_SRC)
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

xgetrf_parallel.$(PSUFFIX) : $(GETRF_SRC)
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DUNIT $< -o $(@F)

include ../../Makefile.tail
