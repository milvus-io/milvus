ifdef TARGET_CORE
TARGET = $(TARGET_CORE)
endif

TOPDIR	= ..
include $(TOPDIR)/Makefile.system

AVX2OPT = 
ifeq ($(C_COMPILER), GCC)
# AVX2 support was added in 4.7.0
  GCCVERSIONGTEQ4 := $(shell expr `$(CC) -dumpversion | cut -f1 -d.` \>= 4)
  GCCMINORVERSIONGTEQ7 := $(shell expr `$(CC) -dumpversion | cut -f2 -d.` \>= 7)
  ifeq ($(GCCVERSIONGTEQ4)$(GCCMINORVERSIONGTEQ7), 11)
   AVX2OPT = -mavx2
  endif
endif
ifeq ($(C_COMPILER), CLANG)
# Any clang posing as gcc 4.2 should be new enough (3.4 or later)
  GCCVERSIONGTEQ4 := $(shell expr `$(CC) -dumpversion | cut -f1 -d.` \>= 4)
  GCCMINORVERSIONGTEQ2 := $(shell expr `$(CC) -dumpversion | cut -f2 -d.` \>= 2)
  ifeq ($(GCCVERSIONGTEQ4)$(GCCMINORVERSIONGTEQ2), 11)
   AVX2OPT = -mavx2
  endif
endif
ifdef NO_AVX2
 AVX2OPT=
endif

ifdef TARGET_CORE
ifeq ($(TARGET_CORE), SKYLAKEX)
 override CFLAGS += -DBUILD_KERNEL -DTABLE_NAME=gotoblas_$(TARGET_CORE) -march=skylake-avx512
 ifeq ($(OSNAME), CYGWIN_NT)
  override CFLAGS += -fno-asynchronous-unwind-tables
 endif
 ifeq ($(OSNAME), WINNT)
  ifeq ($(C_COMPILER), GCC)
   override CFLAGS += -fno-asynchronous-unwind-tables
  endif
 endif
else ifeq ($(TARGET_CORE), HASWELL)
 override CFLAGS += -DBUILD_KERNEL -DTABLE_NAME=gotoblas_$(TARGET_CORE) $(AVX2OPT)
else
 override CFLAGS += -DBUILD_KERNEL -DTABLE_NAME=gotoblas_$(TARGET_CORE)
endif
BUILD_KERNEL = 1
KDIR =
TSUFFIX = _$(TARGET_CORE)
else
TARGET_CORE = $(CORE)
KDIR =
TSUFFIX =
endif

-include $(KERNELDIR)/KERNEL.$(TARGET_CORE)

include $(KERNELDIR)/KERNEL

include Makefile.L1

include Makefile.L2

include Makefile.L3

include Makefile.LA

HPLOBJS = \
	dgemm_kernel.$(SUFFIX) \
	$(DGEMMINCOPYOBJ) $(DGEMMITCOPYOBJ) \
	$(DGEMMONCOPYOBJ) $(DGEMMOTCOPYOBJ) \
	dtrsm_kernel_LN.$(SUFFIX) dtrsm_kernel_LT.$(SUFFIX) \
	dtrsm_kernel_RN.$(SUFFIX) dtrsm_kernel_RT.$(SUFFIX) \
	daxpy_k.$(SUFFIX) dcopy_k.$(SUFFIX) ddot_k.$(SUFFIX) \
	dger_k.$(SUFFIX)  dscal_k.$(SUFFIX) idamax_k.$(SUFFIX) \
	dgemv_n.$(SUFFIX) dgemv_t.$(SUFFIX) dgemm_beta.$(SUFFIX) \
	dtrsm_iunucopy.$(SUFFIX) dtrsm_iunncopy.$(SUFFIX) \
	dtrsm_ilnucopy.$(SUFFIX) dtrsm_ilnncopy.$(SUFFIX) \
	dtrsm_iutucopy.$(SUFFIX) dtrsm_iutncopy.$(SUFFIX) \
	dtrsm_iltucopy.$(SUFFIX) dtrsm_iltncopy.$(SUFFIX) \
	dtrsm_ounucopy.$(SUFFIX) dtrsm_ounncopy.$(SUFFIX) \
	dtrsm_olnucopy.$(SUFFIX) dtrsm_olnncopy.$(SUFFIX) \
	dtrsm_outucopy.$(SUFFIX) dtrsm_outncopy.$(SUFFIX) \
	dtrsm_oltucopy.$(SUFFIX) dtrsm_oltncopy.$(SUFFIX)

COMMONOBJS	+= lsame.$(SUFFIX) scabs1.$(SUFFIX) dcabs1.$(SUFFIX)

ifeq ($(DYNAMIC_ARCH), 1)
SBLASOBJS	+= setparam$(TSUFFIX).$(SUFFIX)
CCOMMON_OPT	+= -DTS=$(TSUFFIX)
endif

KERNEL_INTERFACE = ../common_level1.h ../common_level2.h ../common_level3.h
ifneq ($(NO_LAPACK), 1)
KERNEL_INTERFACE += ../common_lapack.h
endif

ifeq ($(ARCH), x86)
COMMONOBJS	+= cpuid.$(SUFFIX)
endif

ifdef EXPRECISION
COMMONOBJS	+= qconjg.$(SUFFIX) qcabs1.$(SUFFIX)
endif

ifdef QUAD_PRECISION
COMMONOBJS	+= qconjg.$(SUFFIX) qcabs1.$(SUFFIX)
endif

all : libs

scabs1.$(SUFFIX): $(KERNELDIR)/$(SCABS_KERNEL)
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DF_INTERFACE $< -o $(@F)

dcabs1.$(SUFFIX): $(KERNELDIR)/$(DCABS_KERNEL)
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DF_INTERFACE $< -o $(@F)

qcabs1.$(SUFFIX): $(KERNELDIR)/$(QCABS_KERNEL)
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DF_INTERFACE $< -o $(@F)

qconjg.$(SUFFIX): $(KERNELDIR)/qconjg.S
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DF_INTERFACE $< -o $(@F)

lsame.$(SUFFIX): $(KERNELDIR)/$(LSAME_KERNEL)
	$(CC) -c $(CFLAGS) -DF_INTERFACE $< -o $(@F)

setparam$(TSUFFIX).$(SUFFIX): setparam$(TSUFFIX).c kernel$(TSUFFIX).h
ifeq ($(USE_GEMM3M), 1)
	$(CC) -c $(CFLAGS) -DUSE_GEMM3M $< -o $@
else
	$(CC) -c $(CFLAGS) $< -o $@
endif

setparam$(TSUFFIX).c : setparam-ref.c
	sed 's/TS/$(TSUFFIX)/g' $< > $(@F)

kernel$(TSUFFIX).h : $(KERNEL_INTERFACE)
	sed 's/\ *(/$(TSUFFIX)(/g' $^ > $(@F)


cpuid.$(SUFFIX): $(KERNELDIR)/cpuid.S
	$(CC) -c $(CFLAGS) $< -o $(@F)

scabs1.$(PSUFFIX): $(KERNELDIR)/$(SCABS_KERNEL)
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DF_INTERFACE $< -o $(@F)

dcabs1.$(PSUFFIX): $(KERNELDIR)/$(DCABS_KERNEL)
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DF_INTERFACE $< -o $(@F)

qcabs1.$(PSUFFIX): $(KERNELDIR)/$(QCABS_KERNEL)
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DF_INTERFACE $< -o $(@F)

qconjg.$(PSUFFIX): $(KERNELDIR)/qconjg.S
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DF_INTERFACE $< -o $(@F)

lsame.$(PSUFFIX): $(KERNELDIR)/$(LSAME_KERNEL)
	$(CC) -c $(PFLAGS) -DF_INTERFACE $< -o $(@F)

cpuid.$(PSUFFIX): $(KERNELDIR)/cpuid.S
	$(CC) -c $(PFLAGS) $< -o $(@F)

#ifdef DYNAMIC_ARCH
clean ::
	@rm -f setparam_*.c kernel_*.h setparam.h kernel.h

#endif

include $(TOPDIR)/Makefile.tail
