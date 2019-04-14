TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = slauum_U_single.$(SUFFIX) slauum_L_single.$(SUFFIX)
DBLASOBJS = dlauum_U_single.$(SUFFIX) dlauum_L_single.$(SUFFIX)
QBLASOBJS = qlauum_U_single.$(SUFFIX) qlauum_L_single.$(SUFFIX)
CBLASOBJS = clauum_U_single.$(SUFFIX) clauum_L_single.$(SUFFIX)
ZBLASOBJS = zlauum_U_single.$(SUFFIX) zlauum_L_single.$(SUFFIX)
XBLASOBJS = xlauum_U_single.$(SUFFIX) xlauum_L_single.$(SUFFIX)

ifdef SMP
SBLASOBJS += slauum_U_parallel.$(SUFFIX) slauum_L_parallel.$(SUFFIX)
DBLASOBJS += dlauum_U_parallel.$(SUFFIX) dlauum_L_parallel.$(SUFFIX)
QBLASOBJS += qlauum_U_parallel.$(SUFFIX) qlauum_L_parallel.$(SUFFIX)
CBLASOBJS += clauum_U_parallel.$(SUFFIX) clauum_L_parallel.$(SUFFIX)
ZBLASOBJS += zlauum_U_parallel.$(SUFFIX) zlauum_L_parallel.$(SUFFIX)
XBLASOBJS += xlauum_U_parallel.$(SUFFIX) xlauum_L_parallel.$(SUFFIX)
endif

slauum_U_single.$(SUFFIX) : lauum_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauum_L_single.$(SUFFIX) : lauum_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauum_U_parallel.$(SUFFIX) : lauum_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauum_L_parallel.$(SUFFIX) : lauum_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dlauum_U_single.$(SUFFIX) : lauum_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauum_L_single.$(SUFFIX) : lauum_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauum_U_parallel.$(SUFFIX) : lauum_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauum_L_parallel.$(SUFFIX) : lauum_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qlauum_U_single.$(SUFFIX) : lauum_U_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauum_L_single.$(SUFFIX) : lauum_L_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauum_U_parallel.$(SUFFIX) : lauum_U_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauum_L_parallel.$(SUFFIX) : lauum_L_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

clauum_U_single.$(SUFFIX) : lauum_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauum_L_single.$(SUFFIX) : lauum_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauum_U_parallel.$(SUFFIX) : lauum_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauum_L_parallel.$(SUFFIX) : lauum_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zlauum_U_single.$(SUFFIX) : lauum_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauum_L_single.$(SUFFIX) : lauum_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauum_U_parallel.$(SUFFIX) : lauum_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauum_L_parallel.$(SUFFIX) : lauum_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xlauum_U_single.$(SUFFIX) : lauum_U_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauum_L_single.$(SUFFIX) : lauum_L_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauum_U_parallel.$(SUFFIX) : lauum_U_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauum_L_parallel.$(SUFFIX) : lauum_L_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

slauum_U_single.$(PSUFFIX) : lauum_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauum_L_single.$(PSUFFIX) : lauum_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauum_U_parallel.$(PSUFFIX) : lauum_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

slauum_L_parallel.$(PSUFFIX) : lauum_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE $< -o $(@F)

dlauum_U_single.$(PSUFFIX) : lauum_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauum_L_single.$(PSUFFIX) : lauum_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauum_U_parallel.$(PSUFFIX) : lauum_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

dlauum_L_parallel.$(PSUFFIX) : lauum_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE $< -o $(@F)

qlauum_U_single.$(PSUFFIX) : lauum_U_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauum_L_single.$(PSUFFIX) : lauum_L_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauum_U_parallel.$(PSUFFIX) : lauum_U_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

qlauum_L_parallel.$(PSUFFIX) : lauum_L_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE $< -o $(@F)

clauum_U_single.$(PSUFFIX) : lauum_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauum_L_single.$(PSUFFIX) : lauum_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauum_U_parallel.$(PSUFFIX) : lauum_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

clauum_L_parallel.$(PSUFFIX) : lauum_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE $< -o $(@F)

zlauum_U_single.$(PSUFFIX) : lauum_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauum_L_single.$(PSUFFIX) : lauum_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauum_U_parallel.$(PSUFFIX) : lauum_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

zlauum_L_parallel.$(PSUFFIX) : lauum_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE $< -o $(@F)

xlauum_U_single.$(PSUFFIX) : lauum_U_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauum_L_single.$(PSUFFIX) : lauum_L_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauum_U_parallel.$(PSUFFIX) : lauum_U_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

xlauum_L_parallel.$(PSUFFIX) : lauum_L_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE $< -o $(@F)

include ../../Makefile.tail
