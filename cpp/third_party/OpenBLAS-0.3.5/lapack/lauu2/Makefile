TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = slauu2_U.$(SUFFIX) slauu2_L.$(SUFFIX)
DBLASOBJS = dlauu2_U.$(SUFFIX) dlauu2_L.$(SUFFIX)
QBLASOBJS = qlauu2_U.$(SUFFIX) qlauu2_L.$(SUFFIX)
CBLASOBJS = clauu2_U.$(SUFFIX) clauu2_L.$(SUFFIX)
ZBLASOBJS = zlauu2_U.$(SUFFIX) zlauu2_L.$(SUFFIX)
XBLASOBJS = xlauu2_U.$(SUFFIX) xlauu2_L.$(SUFFIX)

slauu2_U.$(SUFFIX) : lauu2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauu2_L.$(SUFFIX) : lauu2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dlauu2_U.$(SUFFIX) : lauu2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauu2_L.$(SUFFIX) : lauu2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qlauu2_U.$(SUFFIX) : lauu2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauu2_L.$(SUFFIX) : lauu2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

clauu2_U.$(SUFFIX) : zlauu2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauu2_L.$(SUFFIX) : zlauu2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zlauu2_U.$(SUFFIX) : zlauu2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauu2_L.$(SUFFIX) : zlauu2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xlauu2_U.$(SUFFIX) : zlauu2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauu2_L.$(SUFFIX) : zlauu2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

slauu2_U.$(PSUFFIX) : lauu2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauu2_L.$(PSUFFIX) : lauu2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dlauu2_U.$(PSUFFIX) : lauu2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauu2_L.$(PSUFFIX) : lauu2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qlauu2_U.$(PSUFFIX) : lauu2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauu2_L.$(PSUFFIX) : lauu2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

clauu2_U.$(PSUFFIX) : zlauu2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauu2_L.$(PSUFFIX) : zlauu2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zlauu2_U.$(PSUFFIX) : zlauu2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauu2_L.$(PSUFFIX) : zlauu2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xlauu2_U.$(PSUFFIX) : zlauu2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauu2_L.$(PSUFFIX) : zlauu2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

include ../../Makefile.tail
