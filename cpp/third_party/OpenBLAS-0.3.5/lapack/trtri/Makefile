TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = strtri_UU_single.$(SUFFIX) strtri_UN_single.$(SUFFIX) strtri_LU_single.$(SUFFIX) strtri_LN_single.$(SUFFIX)

DBLASOBJS = dtrtri_UU_single.$(SUFFIX) dtrtri_UN_single.$(SUFFIX) dtrtri_LU_single.$(SUFFIX) dtrtri_LN_single.$(SUFFIX)

QBLASOBJS = qtrtri_UU_single.$(SUFFIX) qtrtri_UN_single.$(SUFFIX) qtrtri_LU_single.$(SUFFIX) qtrtri_LN_single.$(SUFFIX)

CBLASOBJS = ctrtri_UU_single.$(SUFFIX) ctrtri_UN_single.$(SUFFIX) ctrtri_LU_single.$(SUFFIX) ctrtri_LN_single.$(SUFFIX)

ZBLASOBJS = ztrtri_UU_single.$(SUFFIX) ztrtri_UN_single.$(SUFFIX) ztrtri_LU_single.$(SUFFIX) ztrtri_LN_single.$(SUFFIX)

XBLASOBJS = xtrtri_UU_single.$(SUFFIX) xtrtri_UN_single.$(SUFFIX) xtrtri_LU_single.$(SUFFIX) xtrtri_LN_single.$(SUFFIX)


ifdef SMP
SBLASOBJS += strtri_UU_parallel.$(SUFFIX) strtri_UN_parallel.$(SUFFIX) strtri_LU_parallel.$(SUFFIX) strtri_LN_parallel.$(SUFFIX)
DBLASOBJS += dtrtri_UU_parallel.$(SUFFIX) dtrtri_UN_parallel.$(SUFFIX) dtrtri_LU_parallel.$(SUFFIX) dtrtri_LN_parallel.$(SUFFIX)
QBLASOBJS += qtrtri_UU_parallel.$(SUFFIX) qtrtri_UN_parallel.$(SUFFIX) qtrtri_LU_parallel.$(SUFFIX) qtrtri_LN_parallel.$(SUFFIX)
CBLASOBJS += ctrtri_UU_parallel.$(SUFFIX) ctrtri_UN_parallel.$(SUFFIX) ctrtri_LU_parallel.$(SUFFIX) ctrtri_LN_parallel.$(SUFFIX)
ZBLASOBJS += ztrtri_UU_parallel.$(SUFFIX) ztrtri_UN_parallel.$(SUFFIX) ztrtri_LU_parallel.$(SUFFIX) ztrtri_LN_parallel.$(SUFFIX)
XBLASOBJS += xtrtri_UU_parallel.$(SUFFIX) xtrtri_UN_parallel.$(SUFFIX) xtrtri_LU_parallel.$(SUFFIX) xtrtri_LN_parallel.$(SUFFIX)
endif

strtri_UU_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_UN_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strtri_LU_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_LN_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strtri_UU_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_UN_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strtri_LU_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_LN_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

dtrtri_UU_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_UN_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrtri_LU_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_LN_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrtri_UU_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_UN_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrtri_LU_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_LN_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

qtrtri_UU_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_UN_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrtri_LU_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_LN_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrtri_UU_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_UN_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrtri_LU_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_LN_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

ctrtri_UU_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_UN_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrtri_LU_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_LN_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrtri_UU_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_UN_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrtri_LU_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_LN_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ztrtri_UU_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_UN_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrtri_LU_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_LN_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrtri_UU_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_UN_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrtri_LU_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_LN_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

xtrtri_UU_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_UN_single.$(SUFFIX) : trtri_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrtri_LU_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_LN_single.$(SUFFIX) : trtri_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrtri_UU_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_UN_parallel.$(SUFFIX) : trtri_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrtri_LU_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_LN_parallel.$(SUFFIX) : trtri_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

strtri_UU_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_UN_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strtri_LU_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_LN_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strtri_UU_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_UN_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

strtri_LU_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

strtri_LN_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

dtrtri_UU_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_UN_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrtri_LU_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_LN_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrtri_UU_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_UN_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

dtrtri_LU_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

dtrtri_LN_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

qtrtri_UU_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_UN_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrtri_LU_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_LN_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrtri_UU_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_UN_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

qtrtri_LU_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

qtrtri_LN_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

ctrtri_UU_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_UN_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrtri_LU_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_LN_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrtri_UU_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_UN_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ctrtri_LU_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DUNIT $< -o $(@F)

ctrtri_LN_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UUNIT $< -o $(@F)

ztrtri_UU_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_UN_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrtri_LU_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_LN_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrtri_UU_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_UN_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

ztrtri_LU_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DUNIT $< -o $(@F)

ztrtri_LN_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UUNIT $< -o $(@F)

xtrtri_UU_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_UN_single.$(PSUFFIX) : trtri_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrtri_LU_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_LN_single.$(PSUFFIX) : trtri_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrtri_UU_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_UN_parallel.$(PSUFFIX) : trtri_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

xtrtri_LU_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DUNIT $< -o $(@F)

xtrtri_LN_parallel.$(PSUFFIX) : trtri_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UUNIT $< -o $(@F)

include ../../Makefile.tail
