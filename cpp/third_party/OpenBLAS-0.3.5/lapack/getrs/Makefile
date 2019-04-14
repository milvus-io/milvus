TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = sgetrs_N_single.$(SUFFIX) sgetrs_T_single.$(SUFFIX)
DBLASOBJS = dgetrs_N_single.$(SUFFIX) dgetrs_T_single.$(SUFFIX)
QBLASOBJS = qgetrs_N_single.$(SUFFIX) qgetrs_T_single.$(SUFFIX)
CBLASOBJS = cgetrs_N_single.$(SUFFIX) cgetrs_T_single.$(SUFFIX) cgetrs_R_single.$(SUFFIX) cgetrs_C_single.$(SUFFIX)
ZBLASOBJS = zgetrs_N_single.$(SUFFIX) zgetrs_T_single.$(SUFFIX) zgetrs_R_single.$(SUFFIX) zgetrs_C_single.$(SUFFIX)
XBLASOBJS = xgetrs_N_single.$(SUFFIX) xgetrs_T_single.$(SUFFIX) xgetrs_R_single.$(SUFFIX) xgetrs_C_single.$(SUFFIX)

ifdef SMP
SBLASOBJS += sgetrs_N_parallel.$(SUFFIX) sgetrs_T_parallel.$(SUFFIX)
DBLASOBJS += dgetrs_N_parallel.$(SUFFIX) dgetrs_T_parallel.$(SUFFIX)
QBLASOBJS += qgetrs_N_parallel.$(SUFFIX) qgetrs_T_parallel.$(SUFFIX)
CBLASOBJS += cgetrs_N_parallel.$(SUFFIX) cgetrs_T_parallel.$(SUFFIX) cgetrs_R_parallel.$(SUFFIX) cgetrs_C_parallel.$(SUFFIX)
ZBLASOBJS += zgetrs_N_parallel.$(SUFFIX) zgetrs_T_parallel.$(SUFFIX) zgetrs_R_parallel.$(SUFFIX) zgetrs_C_parallel.$(SUFFIX)
XBLASOBJS += xgetrs_N_parallel.$(SUFFIX) xgetrs_T_parallel.$(SUFFIX) xgetrs_R_parallel.$(SUFFIX) xgetrs_C_parallel.$(SUFFIX)
endif

sgetrs_N_single.$(SUFFIX) : getrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANS $< -o $(@F)

sgetrs_T_single.$(SUFFIX) : getrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANS $< -o $(@F)

sgetrs_N_parallel.$(SUFFIX) : getrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANS $< -o $(@F)

sgetrs_T_parallel.$(SUFFIX) : getrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANS $< -o $(@F)

dgetrs_N_single.$(SUFFIX) : getrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANS $< -o $(@F)

dgetrs_T_single.$(SUFFIX) : getrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANS $< -o $(@F)

dgetrs_N_parallel.$(SUFFIX) : getrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANS $< -o $(@F)

dgetrs_T_parallel.$(SUFFIX) : getrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANS $< -o $(@F)

qgetrs_N_single.$(SUFFIX) : getrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANS $< -o $(@F)

qgetrs_T_single.$(SUFFIX) : getrs_single.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANS $< -o $(@F)

qgetrs_N_parallel.$(SUFFIX) : getrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANS $< -o $(@F)

qgetrs_T_parallel.$(SUFFIX) : getrs_parallel.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANS $< -o $(@F)

cgetrs_N_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=1 $< -o $(@F)

cgetrs_T_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=2 $< -o $(@F)

cgetrs_R_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=3 $< -o $(@F)

cgetrs_C_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=4 $< -o $(@F)

cgetrs_N_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=1 $< -o $(@F)

cgetrs_T_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=2 $< -o $(@F)

cgetrs_R_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=3 $< -o $(@F)

cgetrs_C_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=4 $< -o $(@F)

zgetrs_N_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=1 $< -o $(@F)

zgetrs_T_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=2 $< -o $(@F)

zgetrs_R_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=3 $< -o $(@F)

zgetrs_C_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=4 $< -o $(@F)

zgetrs_N_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=1 $< -o $(@F)

zgetrs_T_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=2 $< -o $(@F)

zgetrs_R_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=3 $< -o $(@F)

zgetrs_C_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=4 $< -o $(@F)

xgetrs_N_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=1 $< -o $(@F)

xgetrs_T_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=2 $< -o $(@F)

xgetrs_R_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=3 $< -o $(@F)

xgetrs_C_single.$(SUFFIX) : zgetrs_single.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=4 $< -o $(@F)

xgetrs_N_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=1 $< -o $(@F)

xgetrs_T_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=2 $< -o $(@F)

xgetrs_R_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=3 $< -o $(@F)

xgetrs_C_parallel.$(SUFFIX) : zgetrs_parallel.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=4 $< -o $(@F)

sgetrs_N_single.$(PSUFFIX) : getrs_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANS $< -o $(@F)

sgetrs_T_single.$(PSUFFIX) : getrs_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANS $< -o $(@F)

sgetrs_N_parallel.$(PSUFFIX) : getrs_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANS $< -o $(@F)

sgetrs_T_parallel.$(PSUFFIX) : getrs_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANS $< -o $(@F)

dgetrs_N_single.$(PSUFFIX) : getrs_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANS $< -o $(@F)

dgetrs_T_single.$(PSUFFIX) : getrs_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANS $< -o $(@F)

dgetrs_N_parallel.$(PSUFFIX) : getrs_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANS $< -o $(@F)

dgetrs_T_parallel.$(PSUFFIX) : getrs_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANS $< -o $(@F)

qgetrs_N_single.$(PSUFFIX) : getrs_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANS $< -o $(@F)

qgetrs_T_single.$(PSUFFIX) : getrs_single.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANS $< -o $(@F)

qgetrs_N_parallel.$(PSUFFIX) : getrs_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANS $< -o $(@F)

qgetrs_T_parallel.$(PSUFFIX) : getrs_parallel.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANS $< -o $(@F)

cgetrs_N_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=1 $< -o $(@F)

cgetrs_T_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=2 $< -o $(@F)

cgetrs_R_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=3 $< -o $(@F)

cgetrs_C_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=4 $< -o $(@F)

cgetrs_N_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=1 $< -o $(@F)

cgetrs_T_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=2 $< -o $(@F)

cgetrs_R_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=3 $< -o $(@F)

cgetrs_C_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANS=4 $< -o $(@F)

zgetrs_N_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=1 $< -o $(@F)

zgetrs_T_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=2 $< -o $(@F)

zgetrs_R_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=3 $< -o $(@F)

zgetrs_C_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=4 $< -o $(@F)

zgetrs_N_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=1 $< -o $(@F)

zgetrs_T_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=2 $< -o $(@F)

zgetrs_R_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=3 $< -o $(@F)

zgetrs_C_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANS=4 $< -o $(@F)

xgetrs_N_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=1 $< -o $(@F)

xgetrs_T_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=2 $< -o $(@F)

xgetrs_R_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=3 $< -o $(@F)

xgetrs_C_single.$(PSUFFIX) : zgetrs_single.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=4 $< -o $(@F)

xgetrs_N_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=1 $< -o $(@F)

xgetrs_T_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=2 $< -o $(@F)

xgetrs_R_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=3 $< -o $(@F)

xgetrs_C_parallel.$(PSUFFIX) : zgetrs_parallel.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANS=4 $< -o $(@F)

include ../../Makefile.tail
