TOPDIR	= ../..
include ../../Makefile.system

COMMONOBJS	 = memory.$(SUFFIX) xerbla.$(SUFFIX) c_abs.$(SUFFIX) z_abs.$(SUFFIX) openblas_set_num_threads.$(SUFFIX) openblas_get_num_threads.$(SUFFIX) openblas_get_num_procs.$(SUFFIX) openblas_get_config.$(SUFFIX) openblas_get_parallel.$(SUFFIX) openblas_error_handle.$(SUFFIX) openblas_env.$(SUFFIX)

#COMMONOBJS	+= slamch.$(SUFFIX) slamc3.$(SUFFIX) dlamch.$(SUFFIX)  dlamc3.$(SUFFIX)

ifdef SMP
COMMONOBJS	+= blas_server.$(SUFFIX) divtable.$(SUFFIX) blasL1thread.$(SUFFIX)
ifndef NO_AFFINITY
COMMONOBJS	+= init.$(SUFFIX)
endif
endif

# COMMONOBJS	+=  info.$(SUFFIX)

ifeq ($(DYNAMIC_ARCH), 1)
ifeq ($(ARCH),arm64)
COMMONOBJS	+=  dynamic_arm64.$(SUFFIX)
else
COMMONOBJS	+=  dynamic.$(SUFFIX)
endif
else
COMMONOBJS	+=  parameter.$(SUFFIX)
endif

ifdef EXPRECISION
COMMONOBJS	+= x_abs.$(SUFFIX) qlamch.$(SUFFIX) qlamc3.$(SUFFIX)
endif

ifdef QUAD_PRECISION
COMMONOBJS	+= addx.$(SUFFIX) mulx.$(SUFFIX)
endif

ifeq ($(OSNAME), CYGWIN_NT)
ifeq ($(C_COMPILER), PGI)
# COMMONOBJS	+= __builtin_stinit.$(SUFFIX)
endif
endif

ifdef USE_CUDA
COMMONOBJS	+= cuda_init.$(SUFFIX)
endif

ifdef FUNCTION_PROFILE
COMMONOBJS	+= profile.$(SUFFIX)
endif

LIBOTHERS = libothers.$(LIBSUFFIX)

ifeq ($(CORE), PPC440)
MEMORY = memory_qalloc.c
endif

ifndef MEMORY
MEMORY = memory.c
endif

ifeq ($(USE_OPENMP), 1)
BLAS_SERVER = blas_server_omp.c
else
ifeq ($(OSNAME), WINNT)
BLAS_SERVER = blas_server_win32.c
endif
ifeq ($(OSNAME), CYGWIN_NT)
BLAS_SERVER = blas_server_win32.c
endif
ifeq ($(OSNAME), Interix)
BLAS_SERVER = blas_server_win32.c
endif
endif

ifndef BLAS_SERVER
BLAS_SERVER = blas_server.c
endif

ifeq ($(DYNAMIC_ARCH), 1)
ifeq ($(ARCH),arm64)
HPLOBJS = memory.$(SUFFIX) xerbla.$(SUFFIX) dynamic_arm64.$(SUFFIX)
else
HPLOBJS = memory.$(SUFFIX) xerbla.$(SUFFIX) dynamic.$(SUFFIX)
endif
else
HPLOBJS = memory.$(SUFFIX) xerbla.$(SUFFIX) parameter.$(SUFFIX)
endif

xerbla.$(SUFFIX) : xerbla.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dynamic.$(SUFFIX) : dynamic.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dynamic.$(PSUFFIX) : dynamic.c
	$(CC) $(PFLAGS) -c $< -o $(@F)

parameter.$(SUFFIX) : parameter.c ../../param.h
	$(CC) $(CFLAGS) -c $< -o $(@F)

init.$(SUFFIX) : init.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

profile.$(SUFFIX) : profile.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

memory.$(SUFFIX) : $(MEMORY) ../../common.h ../../param.h
	$(CC) $(CFLAGS) -c $< -o $(@F)

blas_server.$(SUFFIX) : $(BLAS_SERVER) ../../common.h ../../common_thread.h ../../param.h
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_set_num_threads.$(SUFFIX) : openblas_set_num_threads.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_get_num_threads.$(SUFFIX) : openblas_get_num_threads.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_get_num_procs.$(SUFFIX) : openblas_get_num_procs.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_get_config.$(SUFFIX) : openblas_get_config.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_get_parallel.$(SUFFIX) : openblas_get_parallel.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_error_handle.$(SUFFIX) : openblas_error_handle.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

openblas_env.$(SUFFIX) : openblas_env.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

blasL1thread.$(SUFFIX) : blas_l1_thread.c ../../common.h ../../common_thread.h
	$(CC) $(CFLAGS) -c $< -o $(@F)

cuda_init.$(SUFFIX) : cuda_init.c
	$(CUCC) $(COMMON_OPT) -I$(TOPDIR) $(CUFLAGS) -DCNAME=$(*F) -c $< -o $(@F)

c_abs.$(SUFFIX) : abs.c
	$(CC) $(CFLAGS) -c -UDOUBLE $< -o $(@F)

z_abs.$(SUFFIX) : abs.c
	$(CC) $(CFLAGS) -c -DDOUBLE $< -o $(@F)

x_abs.$(SUFFIX) : abs.c
	$(CC) $(CFLAGS) -c -DXDOUBLE $< -o $(@F)

slamch.$(SUFFIX) : lamch.c
	$(CC) $(CFLAGS) -c -UDOUBLE $< -o $(@F)

dlamch.$(SUFFIX) : lamch.c
	$(CC) $(CFLAGS) -c -DDOUBLE $< -o $(@F)

qlamch.$(SUFFIX) : lamch.c
	$(CC) $(CFLAGS) -c -DXDOUBLE $< -o $(@F)

slamc3.$(SUFFIX) : lamc3.c
	$(CC) $(CFLAGS) -c -UDOUBLE $< -o $(@F)

dlamc3.$(SUFFIX) : lamc3.c
	$(CC) $(CFLAGS) -c -DDOUBLE $< -o $(@F)

qlamc3.$(SUFFIX) : lamc3.c
	$(CC) $(CFLAGS) -c -DXDOUBLE $< -o $(@F)

divtable.$(SUFFIX) : divtable.c
	$(CC) $(CFLAGS) -c -UDOUBLE $< -o $(@F)

__builtin_stinit.$(SUFFIX) : $(ARCH)/builtin_stinit.S
	$(CC) $(CFLAGS) -c -UDOUBLE $< -o $(@F)

addx.$(SUFFIX) : $(ARCH)/addx.c
	$(CC) $(CFLAGS) -c -DXDOUBLE -UCOMPLEX $< -o $(@F)

mulx.$(SUFFIX) : $(ARCH)/mulx.c
	$(CC) $(CFLAGS) -c -DXDOUBLE -UCOMPLEX $< -o $(@F)

xerbla.$(PSUFFIX) : xerbla.c
	$(CC) $(PFLAGS) -c $< -o $(@F)

parameter.$(PSUFFIX) : parameter.c ../../param.h
	$(CC) $(PFLAGS) -c $< -o $(@F)

init.$(PSUFFIX) : init.c
	$(CC) $(PFLAGS) -c $< -o $(@F)

profile.$(PSUFFIX) : profile.c
	$(CC) $(PFLAGS) -c $< -o $(@F)

memory.$(PSUFFIX) : $(MEMORY) ../../common.h ../../param.h
	$(CC) $(PFLAGS) -c $< -o $(@F)

blas_server.$(PSUFFIX) : $(BLAS_SERVER) ../../common.h ../../common_thread.h
	$(CC) $(PFLAGS) -c $< -o $(@F)

blasL1thread.$(PSUFFIX) : blas_l1_thread.c ../../common.h ../../common_thread.h
	$(CC) $(PFLAGS) -c $< -o $(@F)

cuda_init.$(PSUFFIX) : cuda_init.c
	$(CUCC) $(COMMON_OPT) -I$(TOPDIR) $(CUFLAGS) -DCNAME=$(*F) -c $< -o $(@F)

c_abs.$(PSUFFIX) : abs.c
	$(CC) $(PFLAGS) -c -UDOUBLE $< -o $(@F)

z_abs.$(PSUFFIX) : abs.c
	$(CC) $(PFLAGS) -c -DDOUBLE $< -o $(@F)

x_abs.$(PSUFFIX) : abs.c
	$(CC) $(PFLAGS) -c -DXDOUBLE $< -o $(@F)

slamch.$(PUFFIX) : lamch.c
	$(CC) $(PFLAGS) -c -UDOUBLE $< -o $(@F)

dlamch.$(PUFFIX) : lamch.c
	$(CC) $(PFLAGS) -c -DDOUBLE $< -o $(@F)

qlamch.$(PUFFIX) : lamch.c
	$(CC) $(PFLAGS) -c -DXDOUBLE $< -o $(@F)

slamc3.$(PUFFIX) : lamc3.c
	$(CC) $(PFLAGS) -c -UDOUBLE $< -o $(@F)

dlamc3.$(PUFFIX) : lamc3.c
	$(CC) $(PFLAGS) -c -DDOUBLE $< -o $(@F)

qlamc3.$(PUFFIX) : lamc3.c
	$(CC) $(PFLAGS) -c -DXDOUBLE $< -o $(@F)

divtable.$(PSUFFIX) : divtable.c
	$(CC) $(PFLAGS) -c -UDOUBLE $< -o $(@F)

__builtin_stinit.$(PPSUFFIX) : $(ARCH)/builtin_stinit.S
	$(CC) $(PFLAGS) -c -UDOUBLE $< -o $(@F)

addx.$(PSUFFIX) : $(ARCH)/addx.c
	$(CC) $(PFLAGS) -c -DXDOUBLE -UCOMPLEX $< -o $(@F)

mulx.$(PSUFFIX) : $(ARCH)/mulx.c
	$(CC) $(PFLAGS) -c -DXDOUBLE -UCOMPLEX $< -o $(@F)

info.$(SUFFIX) : info.c info.h ../../common.h ../../param.h
	$(CC) $(CFLAGS) -c $< -o $(@F)


hpl   : override CFLAGS += -DHPL
hpl_p : override CFLAGS += -DHPL

include $(TOPDIR)/Makefile.tail
