TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = spotf2_U.$(SUFFIX) spotf2_L.$(SUFFIX)
DBLASOBJS = dpotf2_U.$(SUFFIX) dpotf2_L.$(SUFFIX)
QBLASOBJS = qpotf2_U.$(SUFFIX) qpotf2_L.$(SUFFIX)
CBLASOBJS = cpotf2_U.$(SUFFIX) cpotf2_L.$(SUFFIX)
ZBLASOBJS = zpotf2_U.$(SUFFIX) zpotf2_L.$(SUFFIX)
XBLASOBJS = xpotf2_U.$(SUFFIX) xpotf2_L.$(SUFFIX)

spotf2_U.$(SUFFIX) : potf2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotf2_L.$(SUFFIX) : potf2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dpotf2_U.$(SUFFIX) : potf2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotf2_L.$(SUFFIX) : potf2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qpotf2_U.$(SUFFIX) : potf2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotf2_L.$(SUFFIX) : potf2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

cpotf2_U.$(SUFFIX) : zpotf2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotf2_L.$(SUFFIX) : zpotf2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zpotf2_U.$(SUFFIX) : zpotf2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotf2_L.$(SUFFIX) : zpotf2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xpotf2_U.$(SUFFIX) : zpotf2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotf2_L.$(SUFFIX) : zpotf2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

spotf2_U.$(PSUFFIX) : potf2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

spotf2_L.$(PSUFFIX) : potf2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dpotf2_U.$(PSUFFIX) : potf2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dpotf2_L.$(PSUFFIX) : potf2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qpotf2_U.$(PSUFFIX) : potf2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qpotf2_L.$(PSUFFIX) : potf2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

cpotf2_U.$(PSUFFIX) : zpotf2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

cpotf2_L.$(PSUFFIX) : zpotf2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zpotf2_U.$(PSUFFIX) : zpotf2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zpotf2_L.$(PSUFFIX) : zpotf2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xpotf2_U.$(PSUFFIX) : zpotf2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xpotf2_L.$(PSUFFIX) : zpotf2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

include ../../Makefile.tail
