TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = strti2_UU.$(SUFFIX) strti2_UN.$(SUFFIX) strti2_LU.$(SUFFIX) strti2_LN.$(SUFFIX)
DBLASOBJS = dtrti2_UU.$(SUFFIX) dtrti2_UN.$(SUFFIX) dtrti2_LU.$(SUFFIX) dtrti2_LN.$(SUFFIX)
QBLASOBJS = qtrti2_UU.$(SUFFIX) qtrti2_UN.$(SUFFIX) qtrti2_LU.$(SUFFIX) qtrti2_LN.$(SUFFIX)
CBLASOBJS = ctrti2_UU.$(SUFFIX) ctrti2_UN.$(SUFFIX) ctrti2_LU.$(SUFFIX) ctrti2_LN.$(SUFFIX)
ZBLASOBJS = ztrti2_UU.$(SUFFIX) ztrti2_UN.$(SUFFIX) ztrti2_LU.$(SUFFIX) ztrti2_LN.$(SUFFIX)
XBLASOBJS = xtrti2_UU.$(SUFFIX) xtrti2_UN.$(SUFFIX) xtrti2_LU.$(SUFFIX) xtrti2_LN.$(SUFFIX)

strti2_UU.$(SUFFIX) : trti2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strti2_UN.$(SUFFIX) : trti2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strti2_LU.$(SUFFIX) : trti2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strti2_LN.$(SUFFIX) : trti2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

dtrti2_UU.$(SUFFIX) : trti2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrti2_UN.$(SUFFIX) : trti2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrti2_LU.$(SUFFIX) : trti2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrti2_LN.$(SUFFIX) : trti2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

qtrti2_UU.$(SUFFIX) : trti2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrti2_UN.$(SUFFIX) : trti2_U.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrti2_LU.$(SUFFIX) : trti2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrti2_LN.$(SUFFIX) : trti2_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

ctrti2_UU.$(SUFFIX) : ztrti2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrti2_UN.$(SUFFIX) : ztrti2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrti2_LU.$(SUFFIX) : ztrti2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrti2_LN.$(SUFFIX) : ztrti2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ztrti2_UU.$(SUFFIX) : ztrti2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrti2_UN.$(SUFFIX) : ztrti2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrti2_LU.$(SUFFIX) : ztrti2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrti2_LN.$(SUFFIX) : ztrti2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

xtrti2_UU.$(SUFFIX) : ztrti2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrti2_UN.$(SUFFIX) : ztrti2_U.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrti2_LU.$(SUFFIX) : ztrti2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrti2_LN.$(SUFFIX) : ztrti2_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

strti2_UU.$(PSUFFIX) : trti2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strti2_UN.$(PSUFFIX) : trti2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strti2_LU.$(PSUFFIX) : trti2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strti2_LN.$(PSUFFIX) : trti2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

dtrti2_UU.$(PSUFFIX) : trti2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrti2_UN.$(PSUFFIX) : trti2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrti2_LU.$(PSUFFIX) : trti2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrti2_LN.$(PSUFFIX) : trti2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

qtrti2_UU.$(PSUFFIX) : trti2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrti2_UN.$(PSUFFIX) : trti2_U.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrti2_LU.$(PSUFFIX) : trti2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrti2_LN.$(PSUFFIX) : trti2_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

ctrti2_UU.$(PSUFFIX) : ztrti2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrti2_UN.$(PSUFFIX) : ztrti2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrti2_LU.$(PSUFFIX) : ztrti2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrti2_LN.$(PSUFFIX) : ztrti2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ztrti2_UU.$(PSUFFIX) : ztrti2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrti2_UN.$(PSUFFIX) : ztrti2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrti2_LU.$(PSUFFIX) : ztrti2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrti2_LN.$(PSUFFIX) : ztrti2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

xtrti2_UU.$(PSUFFIX) : ztrti2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrti2_UN.$(PSUFFIX) : ztrti2_U.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrti2_LU.$(PSUFFIX) : ztrti2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrti2_LN.$(PSUFFIX) : ztrti2_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

include ../../Makefile.tail
