#
# The Makefile compiles c wrappers and testers for CBLAS.
#

TOPDIR = ..
include $(TOPDIR)/Makefile.system

override CFLAGS += -DADD$(BU) -DCBLAS

LIB = $(TOPDIR)/$(LIBNAME)

stestl1o = c_sblas1.o

stestl2o = c_sblas2.o c_s2chke.o auxiliary.o c_xerbla.o constant.o

stestl3o = c_sblas3.o c_s3chke.o auxiliary.o c_xerbla.o constant.o

dtestl1o = c_dblas1.o

dtestl2o = c_dblas2.o c_d2chke.o auxiliary.o c_xerbla.o constant.o

dtestl3o = c_dblas3.o c_d3chke.o auxiliary.o c_xerbla.o constant.o

ctestl1o = c_cblas1.o

ctestl2o = c_cblas2.o c_c2chke.o auxiliary.o c_xerbla.o constant.o

ctestl3o = c_cblas3.o c_c3chke.o auxiliary.o c_xerbla.o constant.o

ctestl3o_3m = c_cblas3_3m.o c_c3chke_3m.o auxiliary.o c_xerbla.o constant.o

ztestl1o = c_zblas1.o

ztestl2o = c_zblas2.o c_z2chke.o auxiliary.o c_xerbla.o constant.o

ztestl3o = c_zblas3.o c_z3chke.o auxiliary.o c_xerbla.o constant.o

ztestl3o_3m = c_zblas3_3m.o c_z3chke_3m.o auxiliary.o c_xerbla.o constant.o



all :: all1 all2 all3

all1: xscblat1 xdcblat1 xccblat1 xzcblat1
ifndef CROSS
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./xscblat1
	OMP_NUM_THREADS=2 ./xdcblat1
	OMP_NUM_THREADS=2 ./xccblat1
	OMP_NUM_THREADS=2 ./xzcblat1
else
	OPENBLAS_NUM_THREADS=2 ./xscblat1
	OPENBLAS_NUM_THREADS=2 ./xdcblat1
	OPENBLAS_NUM_THREADS=2 ./xccblat1
	OPENBLAS_NUM_THREADS=2 ./xzcblat1
endif
endif

all2: xscblat2 xdcblat2 xccblat2 xzcblat2
ifndef CROSS
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./xscblat2 < sin2
	OMP_NUM_THREADS=2 ./xdcblat2 < din2
	OMP_NUM_THREADS=2 ./xccblat2 < cin2
	OMP_NUM_THREADS=2 ./xzcblat2 < zin2
else
	OPENBLAS_NUM_THREADS=2 ./xscblat2 < sin2
	OPENBLAS_NUM_THREADS=2 ./xdcblat2 < din2
	OPENBLAS_NUM_THREADS=2 ./xccblat2 < cin2
	OPENBLAS_NUM_THREADS=2 ./xzcblat2 < zin2
endif
endif

all3: xscblat3 xdcblat3 xccblat3 xzcblat3
ifndef CROSS
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./xscblat3 < sin3
	OMP_NUM_THREADS=2 ./xdcblat3 < din3
	OMP_NUM_THREADS=2 ./xccblat3 < cin3
	OMP_NUM_THREADS=2 ./xzcblat3 < zin3
else
	OPENBLAS_NUM_THREADS=2 ./xscblat3 < sin3
	OPENBLAS_NUM_THREADS=2 ./xdcblat3 < din3
	OPENBLAS_NUM_THREADS=2 ./xccblat3 < cin3
	OPENBLAS_NUM_THREADS=2 ./xzcblat3 < zin3
endif

all3_3m: xzcblat3_3m xccblat3_3m
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./xccblat3_3m < cin3_3m
	OMP_NUM_THREADS=2 ./xzcblat3_3m < zin3_3m
else
	OPENBLAS_NUM_THREADS=2 ./xccblat3_3m < cin3_3m
	OPENBLAS_NUM_THREADS=2 ./xzcblat3_3m < zin3_3m
endif
endif




clean ::
	rm -f x*

FLDFLAGS = $(FFLAGS:-fPIC=) $(LDFLAGS)
ifeq ($(USE_OPENMP), 1)
ifeq ($(F_COMPILER), GFORTRAN)
ifeq ($(C_COMPILER), CLANG)
CEXTRALIB = -lomp
endif
endif
endif

# Single real
xscblat1: $(stestl1o) c_sblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xscblat1 c_sblat1.o $(stestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xscblat2: $(stestl2o) c_sblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xscblat2 c_sblat2.o $(stestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xscblat3: $(stestl3o) c_sblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xscblat3 c_sblat3.o $(stestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
# Double real
xdcblat1: $(dtestl1o) c_dblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xdcblat1 c_dblat1.o $(dtestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xdcblat2: $(dtestl2o) c_dblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xdcblat2 c_dblat2.o $(dtestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xdcblat3: $(dtestl3o) c_dblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xdcblat3 c_dblat3.o $(dtestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)

# Single complex
xccblat1: $(ctestl1o) c_cblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat1 c_cblat1.o $(ctestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xccblat2: $(ctestl2o) c_cblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat2 c_cblat2.o $(ctestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xccblat3: $(ctestl3o) c_cblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat3 c_cblat3.o $(ctestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)

xccblat3_3m: $(ctestl3o_3m) c_cblat3_3m.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xccblat3_3m c_cblat3_3m.o $(ctestl3o_3m) $(LIB) $(EXTRALIB) $(CEXTRALIB)

# Double complex
xzcblat1: $(ztestl1o) c_zblat1.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat1 c_zblat1.o $(ztestl1o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xzcblat2: $(ztestl2o) c_zblat2.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat2 c_zblat2.o $(ztestl2o) $(LIB) $(EXTRALIB) $(CEXTRALIB)
xzcblat3: $(ztestl3o) c_zblat3.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat3 c_zblat3.o $(ztestl3o) $(LIB) $(EXTRALIB) $(CEXTRALIB)


xzcblat3_3m: $(ztestl3o_3m) c_zblat3_3m.o $(TOPDIR)/$(LIBNAME)
	$(FC) $(FLDFLAGS) -o xzcblat3_3m c_zblat3_3m.o $(ztestl3o_3m) $(LIB) $(EXTRALIB) $(CEXTRALIB)


include $(TOPDIR)/Makefile.tail
