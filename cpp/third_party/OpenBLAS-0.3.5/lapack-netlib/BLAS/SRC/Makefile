include ../../make.inc

#######################################################################
#  This is the makefile to create a library for the BLAS.
#  The files are grouped as follows:
#
#       SBLAS1 -- Single precision real BLAS routines
#       CBLAS1 -- Single precision complex BLAS routines
#       DBLAS1 -- Double precision real BLAS routines
#       ZBLAS1 -- Double precision complex BLAS routines
#
#       CB1AUX -- Real BLAS routines called by complex routines
#       ZB1AUX -- D.P. real BLAS routines called by d.p. complex
#                 routines
#
#      ALLBLAS -- Auxiliary routines for Level 2 and 3 BLAS
#
#       SBLAS2 -- Single precision real BLAS2 routines
#       CBLAS2 -- Single precision complex BLAS2 routines
#       DBLAS2 -- Double precision real BLAS2 routines
#       ZBLAS2 -- Double precision complex BLAS2 routines
#
#       SBLAS3 -- Single precision real BLAS3 routines
#       CBLAS3 -- Single precision complex BLAS3 routines
#       DBLAS3 -- Double precision real BLAS3 routines
#       ZBLAS3 -- Double precision complex BLAS3 routines
#
#  The library can be set up to include routines for any combination
#  of the four precisions.  To create or add to the library, enter make
#  followed by one or more of the precisions desired.  Some examples:
#       make single
#       make single complex
#       make single double complex complex16
#  Note that these commands are not safe for parallel builds.
#
#  Alternatively, the commands
#       make all
#  or
#       make
#  without any arguments creates a library of all four precisions.
#  The name of the library is held in BLASLIB, which is set in the
#  top-level make.inc
#
#  To remove the object files after the library is created, enter
#       make cleanobj
#  To force the source files to be recompiled, enter, for example,
#       make single FRC=FRC
#
#---------------------------------------------------------------------
#
#  Edward Anderson, University of Tennessee
#  March 26, 1990
#  Susan Ostrouchov, Last updated September 30, 1994
#  ejr, May 2006.
#
#######################################################################

all: $(BLASLIB)

#---------------------------------------------------------
#  Comment out the next 6 definitions if you already have
#  the Level 1 BLAS.
#---------------------------------------------------------
SBLAS1 = isamax.o sasum.o saxpy.o scopy.o sdot.o snrm2.o \
	srot.o srotg.o sscal.o sswap.o sdsdot.o srotmg.o srotm.o
$(SBLAS1): $(FRC)

CBLAS1 = scabs1.o scasum.o scnrm2.o icamax.o caxpy.o ccopy.o \
	cdotc.o cdotu.o csscal.o crotg.o cscal.o cswap.o csrot.o
$(CBLAS1): $(FRC)

DBLAS1 = idamax.o dasum.o daxpy.o dcopy.o ddot.o dnrm2.o \
	drot.o drotg.o dscal.o dsdot.o dswap.o drotmg.o drotm.o
$(DBLAS1): $(FRC)

ZBLAS1 = dcabs1.o dzasum.o dznrm2.o izamax.o zaxpy.o zcopy.o \
	zdotc.o zdotu.o zdscal.o zrotg.o zscal.o zswap.o zdrot.o
$(ZBLAS1): $(FRC)

CB1AUX = isamax.o sasum.o saxpy.o scopy.o snrm2.o sscal.o
$(CB1AUX): $(FRC)

ZB1AUX = idamax.o dasum.o daxpy.o dcopy.o dnrm2.o dscal.o
$(ZB1AUX): $(FRC)

#---------------------------------------------------------------------
#  The following line defines auxiliary routines needed by both the
#  Level 2 and Level 3 BLAS.  Comment it out only if you already have
#  both the Level 2 and 3 BLAS.
#---------------------------------------------------------------------
ALLBLAS = lsame.o xerbla.o xerbla_array.o
$(ALLBLAS): $(FRC)

#---------------------------------------------------------
#  Comment out the next 4 definitions if you already have
#  the Level 2 BLAS.
#---------------------------------------------------------
SBLAS2 = sgemv.o sgbmv.o ssymv.o ssbmv.o sspmv.o \
	strmv.o stbmv.o stpmv.o strsv.o stbsv.o stpsv.o \
	sger.o ssyr.o sspr.o ssyr2.o sspr2.o
$(SBLAS2): $(FRC)

CBLAS2 = cgemv.o cgbmv.o chemv.o chbmv.o chpmv.o \
	ctrmv.o ctbmv.o ctpmv.o ctrsv.o ctbsv.o ctpsv.o \
	cgerc.o cgeru.o cher.o chpr.o cher2.o chpr2.o
$(CBLAS2): $(FRC)

DBLAS2 = dgemv.o dgbmv.o dsymv.o dsbmv.o dspmv.o \
	dtrmv.o dtbmv.o dtpmv.o dtrsv.o dtbsv.o dtpsv.o \
	dger.o dsyr.o dspr.o dsyr2.o dspr2.o
$(DBLAS2): $(FRC)

ZBLAS2 = zgemv.o zgbmv.o zhemv.o zhbmv.o zhpmv.o \
	ztrmv.o ztbmv.o ztpmv.o ztrsv.o ztbsv.o ztpsv.o \
	zgerc.o zgeru.o zher.o zhpr.o zher2.o zhpr2.o
$(ZBLAS2): $(FRC)

#---------------------------------------------------------
#  Comment out the next 4 definitions if you already have
#  the Level 3 BLAS.
#---------------------------------------------------------
SBLAS3 = sgemm.o ssymm.o ssyrk.o ssyr2k.o strmm.o strsm.o
$(SBLAS3): $(FRC)

CBLAS3 = cgemm.o csymm.o csyrk.o csyr2k.o ctrmm.o ctrsm.o \
	chemm.o cherk.o cher2k.o
$(CBLAS3): $(FRC)

DBLAS3 = dgemm.o dsymm.o dsyrk.o dsyr2k.o dtrmm.o dtrsm.o
$(DBLAS3): $(FRC)

ZBLAS3 = zgemm.o zsymm.o zsyrk.o zsyr2k.o ztrmm.o ztrsm.o \
	zhemm.o zherk.o zher2k.o
$(ZBLAS3): $(FRC)

ALLOBJ = $(SBLAS1) $(SBLAS2) $(SBLAS3) $(DBLAS1) $(DBLAS2) $(DBLAS3) \
	$(CBLAS1) $(CBLAS2) $(CBLAS3) $(ZBLAS1) \
	$(ZBLAS2) $(ZBLAS3) $(ALLBLAS)

$(BLASLIB): $(ALLOBJ)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

single: $(SBLAS1) $(ALLBLAS) $(SBLAS2) $(SBLAS3)
	$(ARCH) $(ARCHFLAGS) $(BLASLIB) $^
	$(RANLIB) $(BLASLIB)

double: $(DBLAS1) $(ALLBLAS) $(DBLAS2) $(DBLAS3)
	$(ARCH) $(ARCHFLAGS) $(BLASLIB) $^
	$(RANLIB) $(BLASLIB)

complex: $(CBLAS1) $(CB1AUX) $(ALLBLAS) $(CBLAS2) $(CBLAS3)
	$(ARCH) $(ARCHFLAGS) $(BLASLIB) $^
	$(RANLIB) $(BLASLIB)

complex16: $(ZBLAS1) $(ZB1AUX) $(ALLBLAS) $(ZBLAS2) $(ZBLAS3)
	$(ARCH) $(ARCHFLAGS) $(BLASLIB) $^
	$(RANLIB) $(BLASLIB)

FRC:
	@FRC=$(FRC)

clean: cleanobj cleanlib
cleanobj:
	rm -f *.o
cleanlib:
	#rm -f $(BLASLIB)  # May point to a system lib, e.g. -lblas

.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<
