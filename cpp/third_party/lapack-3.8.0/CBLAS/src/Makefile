# This Makefile compiles the CBLAS routines

include ../../make.inc

all: $(CBLASLIB)

# Error handling routines for level 2 & 3
errhand = cblas_globals.o cblas_xerbla.o xerbla.o

#
#
# CBLAS routines
#
# Level 1
#
#

# Files for level 1 single precision real
slev1 = cblas_srotg.o cblas_srotmg.o cblas_srot.o  cblas_srotm.o \
        cblas_sswap.o cblas_sscal.o  cblas_scopy.o cblas_saxpy.o \
        cblas_sdot.o  cblas_sdsdot.o cblas_snrm2.o cblas_sasum.o \
        cblas_isamax.o sdotsub.o sdsdotsub.o snrm2sub.o sasumsub.o \
        isamaxsub.o

# Files for level 1 double precision real
dlev1 = cblas_drotg.o cblas_drotmg.o cblas_drot.o  cblas_drotm.o \
        cblas_dswap.o cblas_dscal.o  cblas_dcopy.o cblas_daxpy.o \
        cblas_ddot.o  cblas_dsdot.o  cblas_dnrm2.o cblas_dasum.o \
        cblas_idamax.o ddotsub.o dsdotsub.o dnrm2sub.o \
        dasumsub.o idamaxsub.o

# Files for level 1 single precision complex
clev1 = cblas_cswap.o cblas_cscal.o cblas_csscal.o cblas_ccopy.o \
        cblas_caxpy.o cblas_cdotu_sub.o cblas_cdotc_sub.o \
        cblas_icamax.o cdotcsub.o cdotusub.o icamaxsub.o

# Files for level 1 double precision complex
zlev1 = cblas_zswap.o cblas_zscal.o cblas_zdscal.o cblas_zcopy.o \
        cblas_zaxpy.o cblas_zdotu_sub.o cblas_zdotc_sub.o cblas_dznrm2.o \
        cblas_dzasum.o cblas_izamax.o zdotcsub.o zdotusub.o \
        dzasumsub.o dznrm2sub.o izamaxsub.o

# Common files for level 1 single precision
sclev1 = cblas_scasum.o scasumsub.o cblas_scnrm2.o scnrm2sub.o

# Single precision real
slib1: $(slev1) $(sclev1)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Double precision real
dlib1: $(dlev1)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Single precision complex
clib1: $(clev1) $(sclev1)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Double precision complex
zlib1: $(zlev1)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

#
#
# CBLAS routines
#
# Level 2
#
#

# Files for level 2 single precision real
slev2 = cblas_sgemv.o cblas_sgbmv.o cblas_sger.o  cblas_ssbmv.o cblas_sspmv.o \
        cblas_sspr.o  cblas_sspr2.o cblas_ssymv.o cblas_ssyr.o  cblas_ssyr2.o \
        cblas_stbmv.o cblas_stbsv.o cblas_stpmv.o cblas_stpsv.o cblas_strmv.o \
        cblas_strsv.o

# Files for level 2 double precision real
dlev2 = cblas_dgemv.o cblas_dgbmv.o cblas_dger.o  cblas_dsbmv.o cblas_dspmv.o \
        cblas_dspr.o  cblas_dspr2.o cblas_dsymv.o cblas_dsyr.o  cblas_dsyr2.o \
        cblas_dtbmv.o cblas_dtbsv.o cblas_dtpmv.o cblas_dtpsv.o cblas_dtrmv.o \
        cblas_dtrsv.o

# Files for level 2 single precision complex
clev2 = cblas_cgemv.o cblas_cgbmv.o cblas_chemv.o cblas_chbmv.o cblas_chpmv.o \
        cblas_ctrmv.o cblas_ctbmv.o cblas_ctpmv.o cblas_ctrsv.o cblas_ctbsv.o \
        cblas_ctpsv.o cblas_cgeru.o cblas_cgerc.o cblas_cher.o  cblas_cher2.o \
        cblas_chpr.o  cblas_chpr2.o

# Files for level 2 double precision complex
zlev2 = cblas_zgemv.o cblas_zgbmv.o cblas_zhemv.o cblas_zhbmv.o cblas_zhpmv.o \
        cblas_ztrmv.o cblas_ztbmv.o cblas_ztpmv.o cblas_ztrsv.o cblas_ztbsv.o \
        cblas_ztpsv.o cblas_zgeru.o cblas_zgerc.o cblas_zher.o  cblas_zher2.o \
        cblas_zhpr.o  cblas_zhpr2.o

# Single precision real
slib2: $(slev2) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Double precision real
dlib2: $(dlev2) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Single precision complex
clib2: $(clev2) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Double precision complex
zlib2: $(zlev2) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

#
#
# CBLAS routines
#
# Level 3
#
#

# Files for level 3 single precision real
slev3 = cblas_sgemm.o cblas_ssymm.o cblas_ssyrk.o cblas_ssyr2k.o cblas_strmm.o \
        cblas_strsm.o

# Files for level 3 double precision real
dlev3 = cblas_dgemm.o cblas_dsymm.o cblas_dsyrk.o cblas_dsyr2k.o cblas_dtrmm.o \
        cblas_dtrsm.o

# Files for level 3 single precision complex
clev3 = cblas_cgemm.o cblas_csymm.o cblas_chemm.o cblas_cherk.o \
        cblas_cher2k.o cblas_ctrmm.o cblas_ctrsm.o cblas_csyrk.o \
        cblas_csyr2k.o

# Files for level 3 double precision complex
zlev3 = cblas_zgemm.o cblas_zsymm.o cblas_zhemm.o cblas_zherk.o \
        cblas_zher2k.o cblas_ztrmm.o cblas_ztrsm.o cblas_zsyrk.o \
        cblas_zsyr2k.o

# Single precision real
slib3: $(slev3) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Double precision real
dlib3: $(dlev3) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Single precision complex
clib3: $(clev3) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# Double precision complex
zlib3: $(zlev3) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)


alev1 = $(slev1) $(dlev1) $(clev1) $(zlev1) $(sclev1)
alev2 = $(slev2) $(dlev2) $(clev2) $(zlev2)
alev3 = $(slev3) $(dlev3) $(clev3) $(zlev3)

# All level 1
all1: $(alev1)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# All level 2
all2: $(alev2) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# All level 3
all3: $(alev3) $(errhand)
	$(ARCH) $(ARCHFLAGS) $(CBLASLIB) $^
	$(RANLIB) $(CBLASLIB)

# All levels and precisions
$(CBLASLIB): $(alev1) $(alev2) $(alev3) $(errhand)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

FRC:
	@FRC=$(FRC)

clean: cleanobj cleanlib
cleanobj:
	rm -f *.o
cleanlib:
	rm -f $(CBLASLIB)

.c.o:
	$(CC) $(CFLAGS) -I../include -c -o $@ $<
.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<
