TOPDIR	= ..
include $(TOPDIR)/Makefile.system

# ACML standard
#ACML=/opt/acml5.3.1/gfortran64_mp/lib
#LIBACML = -fopenmp $(ACML)/libacml_mp.a -lgfortran -lm

# ACML custom
#ACML=/opt/pb/acml-5-3-1-gfortran-64bit/gfortran64_fma4_mp/lib
#LIBACML = -fopenmp $(ACML)/libacml_mp.a -lgfortran -lm

# ACML 6.1 custom
ACML=/home/saar/acml6.1/gfortran64_mp/lib
LIBACML = -fopenmp $(ACML)/libacml_mp.so -lgfortran -lm
 

# Atlas Ubuntu 
#ATLAS=/usr/lib/atlas-base
#LIBATLAS = -fopenmp $(ATLAS)/liblapack_atlas.a  $(ATLAS)/libptcblas.a  $(ATLAS)/libptf77blas.a  $(ATLAS)/libatlas.a -lgfortran -lm

# Atlas RHEL and Fedora
ATLAS=/usr/lib64/atlas
LIBATLAS = -fopenmp $(ATLAS)/liblapack.a  $(ATLAS)/libptcblas.a  $(ATLAS)/libptf77blas.a  $(ATLAS)/libatlas.a -lgfortran -lm

# Intel standard
# MKL=/opt/intel/mkl/lib/intel64
# LIBMKL = -L$(MKL) -lmkl_intel_lp64 -lmkl_gnu_thread -lmkl_core -lgomp -lpthread -lm

# Intel custom
MKL=/home/saar/intel_mkl
LIBMKL = -L$(MKL) -lmkl_intel_lp64 -lmkl_gnu_thread -lmkl_core -lgomp -lpthread -lm

# Apple vecLib
LIBVECLIB = -framework Accelerate

ESSL=/opt/ibm/lib
#LIBESSL = -lesslsmp $(ESSL)/libxlomp_ser.so.1 $(ESSL)/libxlf90_r.so.1 $(ESSL)/libxlfmath.so.1 $(ESSL)/libxlsmp.so.1 /opt/ibm/xlC/13.1.3/lib/libxl.a
LIBESSL = -lesslsmp  $(ESSL)/libxlf90_r.so.1 $(ESSL)/libxlfmath.so.1 $(ESSL)/libxlsmp.so.1 /opt/ibm/xlC/13.1.3/lib/libxl.a

ifneq ($(NO_LAPACK), 1)
GOTO_LAPACK_TARGETS=slinpack.goto dlinpack.goto clinpack.goto zlinpack.goto \
		    scholesky.goto dcholesky.goto ccholesky.goto zcholesky.goto \
		    sgesv.goto dgesv.goto cgesv.goto zgesv.goto \
		    sgeev.goto dgeev.goto cgeev.goto zgeev.goto \
		    csymv.goto zsymv.goto \
		    sgetri.goto dgetri.goto cgetri.goto zgetri.goto \
		    spotrf.goto dpotrf.goto cpotrf.goto zpotrf.goto
else
GOTO_LAPACK_TARGETS=
endif

ifeq ($(OSNAME), WINNT)

goto :: slinpack.goto dlinpack.goto clinpack.goto zlinpack.goto \
       scholesky.goto dcholesky.goto ccholesky.goto zcholesky.goto \
       sgemm.goto dgemm.goto cgemm.goto zgemm.goto \
       strmm.goto dtrmm.goto ctrmm.goto ztrmm.goto \
       strsm.goto dtrsm.goto ctrsm.goto ztrsm.goto \
       ssyrk.goto dsyrk.goto csyrk.goto zsyrk.goto \
       ssyr2k.goto dsyr2k.goto csyr2k.goto zsyr2k.goto \
       sger.goto dger.goto cger.goto zger.goto \
       sdot.goto ddot.goto \
       srot.goto drot.goto \
       saxpy.goto daxpy.goto caxpy.goto zaxpy.goto \
       scopy.goto dcopy.goto ccopy.goto zcopy.goto \
       sswap.goto dswap.goto cswap.goto zswap.goto \
       sscal.goto dscal.goto cscal.goto zscal.goto \
       sasum.goto dasum.goto casum.goto zasum.goto \
       ssymv.goto dsymv.goto csymv.goto zsymv.goto \
       chemv.goto zhemv.goto \
       chemm.goto zhemm.goto \
       cherk.goto zherk.goto \
       cher2k.goto zher2k.goto \
       sgemv.goto dgemv.goto cgemv.goto zgemv.goto \
       sgeev.goto dgeev.goto cgeev.goto zgeev.goto \
       sgesv.goto dgesv.goto cgesv.goto zgesv.goto \
       sgetri.goto dgetri.goto cgetri.goto zgetri.goto \
       spotrf.goto dpotrf.goto cpotrf.goto zpotrf.goto \
       ssymm.goto dsymm.goto csymm.goto zsymm.goto

acml :: slinpack.acml dlinpack.acml clinpack.acml zlinpack.acml \
       scholesky.acml dcholesky.acml ccholesky.acml zcholesky.acml \
       sgemm.acml dgemm.acml cgemm.acml zgemm.acml \
       strmm.acml dtrmm.acml ctrmm.acml ztrmm.acml \
       strsm.acml dtrsm.acml ctrsm.acml ztrsm.acml \
       ssyrk.acml dsyrk.acml csyrk.acml zsyrk.acml \
       ssyr2k.acml dsyr2k.acml csyr2k.acml zsyr2k.acml \
       sger.acml dger.acml cger.acml zger.acml \
       sdot.acml ddot.acml \
       saxpy.acml daxpy.acml caxpy.acml zaxpy.acml \
       scopy.acml dcopy.acml ccopy.acml zcopy.acml \
       sswap.acml dswap.acml cswap.acml zswap.acml \
       sscal.acml dscal.acml cscal.acml zscal.acml \
       sasum.acml dasum.acml casum.acml zasum.acml \
       ssymv.acml dsymv.acml csymv.acml zsymv.acml \
       chemv.acml zhemv.acml \
       chemm.acml zhemm.acml \
       cherk.acml zherk.acml \
       cher2k.acml zher2k.acml \
       sgemv.acml dgemv.acml cgemv.acml zgemv.acml \
       sgeev.acml dgeev.acml cgeev.acml zgeev.acml \
       sgesv.acml dgesv.acml cgesv.acml zgesv.acml \
       sgetri.acml dgetri.acml cgetri.acml zgetri.acml \
       spotrf.acml dpotrf.acml cpotrf.acml zpotrf.acml \
       ssymm.acml dsymm.acml csymm.acml zsymm.acml

atlas :: slinpack.atlas dlinpack.atlas clinpack.atlas zlinpack.atlas \
       scholesky.atlas dcholesky.atlas ccholesky.atlas zcholesky.atlas \
       sgemm.atlas dgemm.atlas cgemm.atlas zgemm.atlas \
       strmm.atlas dtrmm.atlas ctrmm.atlas ztrmm.atlas \
       strsm.atlas dtrsm.atlas ctrsm.atlas ztrsm.atlas \
       ssyrk.atlas dsyrk.atlas csyrk.atlas zsyrk.atlas \
       ssyr2k.atlas dsyr2k.atlas csyr2k.atlas zsyr2k.atlas \
       sger.atlas dger.atlas cger.atlas zger.atlas\
       sdot.atlas ddot.atlas \
       saxpy.atlas daxpy.atlas caxpy.atlas zaxpy.atlas \
       scopy.atlas dcopy.atlas ccopy.atlas zcopy.atlas \
       sswap.atlas dswap.atlas cswap.atlas zswap.atlas \
       sscal.atlas dscal.atlas cscal.atlas zscal.atlas \
       sasum.atlas dasum.atlas casum.atlas zasum.atlas \
       ssymv.atlas dsymv.atlas csymv.atlas zsymv.atlas \
       chemv.atlas zhemv.atlas \
       chemm.acml zhemm.acml \
       chemm.atlas zhemm.atlas \
       cherk.atlas zherk.atlas \
       cher2k.atlas zher2k.atlas \
       sgemv.atlas dgemv.atlas cgemv.atlas zgemv.atlas \
       sgeev.atlas dgeev.atlas cgeev.atlas zgeev.atlas \
       sgesv.atlas dgesv.atlas cgesv.atlas zgesv.atlas \
       sgetri.atlas dgetri.atlas cgetri.atlas zgetri.atlas \
       spotrf.atlas dpotrf.atlas cpotrf.atlas zpotrf.atlas \
       ssymm.atlas dsymm.atlas csymm.atlas zsymm.atlas

mkl :: slinpack.mkl dlinpack.mkl clinpack.mkl zlinpack.mkl \
       scholesky.mkl dcholesky.mkl ccholesky.mkl zcholesky.mkl \
       sgemm.mkl dgemm.mkl cgemm.mkl zgemm.mkl \
       strmm.mkl dtrmm.mkl ctrmm.mkl ztrmm.mkl \
       strsm.mkl dtrsm.mkl ctrsm.mkl ztrsm.mkl \
       ssyrk.mkl dsyrk.mkl csyrk.mkl zsyrk.mkl \
       ssyr2k.mkl dsyr2k.mkl csyr2k.mkl zsyr2k.mkl \
       sger.mkl dger.mkl cger.mkl zger.mkl \
       sdot.mkl ddot.mkl \
       saxpy.mkl daxpy.mkl caxpy.mkl zaxpy.mkl \
       scopy.mkl dcopy.mkl ccopy.mkl zcopy.mkl \
       sswap.mkl dswap.mkl cswap.mkl zswap.mkl \
       sscal.mkl dscal.mkl cscal.mkl zscal.mkl \
       sasum.mkl dasum.mkl casum.mkl zasum.mkl \
       ssymv.mkl dsymv.mkl csymv.mkl zsymv.mkl \
       chemv.mkl zhemv.mkl \
       chemm.mkl zhemm.mkl \
       cherk.mkl zherk.mkl \
       cher2k.mkl zher2k.mkl \
       sgemv.mkl dgemv.mkl cgemv.mkl zgemv.mkl \
       sgeev.mkl dgeev.mkl cgeev.mkl zgeev.mkl \
       sgesv.mkl dgesv.mkl cgesv.mkl zgesv.mkl \
       sgetri.mkl dgetri.mkl cgetri.mkl zgetri.mkl \
       spotrf.mkl dpotrf.mkl cpotrf.mkl zpotrf.mkl \
       ssymm.mkl dsymm.mkl csymm.mkl zsymm.mkl

else

goto :: sgemm.goto dgemm.goto cgemm.goto zgemm.goto \
       strmm.goto dtrmm.goto ctrmm.goto ztrmm.goto \
       strsm.goto dtrsm.goto ctrsm.goto ztrsm.goto \
       ssyrk.goto dsyrk.goto csyrk.goto zsyrk.goto \
       ssyr2k.goto dsyr2k.goto csyr2k.goto zsyr2k.goto \
       sger.goto dger.goto cger.goto zger.goto \
       sdot.goto ddot.goto cdot.goto zdot.goto \
       srot.goto drot.goto \
       saxpy.goto daxpy.goto caxpy.goto zaxpy.goto \
       scopy.goto dcopy.goto ccopy.goto zcopy.goto \
       sswap.goto dswap.goto cswap.goto zswap.goto \
       sscal.goto dscal.goto cscal.goto zscal.goto \
       sasum.goto dasum.goto casum.goto zasum.goto \
       ssymv.goto dsymv.goto \
       chemv.goto zhemv.goto \
       chemm.goto zhemm.goto \
       cherk.goto zherk.goto \
       cher2k.goto zher2k.goto \
       sgemv.goto dgemv.goto cgemv.goto zgemv.goto \
       ssymm.goto dsymm.goto csymm.goto zsymm.goto \
       smallscaling \
       isamax.goto idamax.goto icamax.goto izamax.goto \
       snrm2.goto dnrm2.goto scnrm2.goto dznrm2.goto $(GOTO_LAPACK_TARGETS)

acml :: slinpack.acml dlinpack.acml clinpack.acml zlinpack.acml \
       scholesky.acml dcholesky.acml ccholesky.acml zcholesky.acml \
       sgemm.acml dgemm.acml cgemm.acml zgemm.acml \
       strmm.acml dtrmm.acml ctrmm.acml ztrmm.acml \
       strsm.acml dtrsm.acml ctrsm.acml ztrsm.acml \
       ssyrk.acml dsyrk.acml csyrk.acml zsyrk.acml \
       ssyr2k.acml dsyr2k.acml csyr2k.acml zsyr2k.acml \
       sger.acml dger.acml cger.acml zger.acml \
       sdot.acml ddot.acml \
       saxpy.acml daxpy.acml caxpy.acml zaxpy.acml \
       scopy.acml dcopy.acml ccopy.acml zcopy.acml \
       sswap.acml dswap.acml cswap.acml zswap.acml \
       sscal.acml dscal.acml cscal.acml zscal.acml \
       sasum.acml dasum.acml casum.acml zasum.acml \
       ssymv.acml dsymv.acml csymv.acml zsymv.acml \
       chemv.acml zhemv.acml \
       chemm.acml zhemm.acml \
       cherk.acml zherk.acml \
       cher2k.acml zher2k.acml \
       sgemv.acml dgemv.acml cgemv.acml zgemv.acml \
       sgeev.acml dgeev.acml cgeev.acml zgeev.acml \
       sgesv.acml dgesv.acml cgesv.acml zgesv.acml \
       sgetri.acml dgetri.acml cgetri.acml zgetri.acml \
       spotrf.acml dpotrf.acml cpotrf.acml zpotrf.acml \
       ssymm.acml dsymm.acml csymm.acml zsymm.acml

atlas :: slinpack.atlas dlinpack.atlas clinpack.atlas zlinpack.atlas \
       scholesky.atlas dcholesky.atlas ccholesky.atlas zcholesky.atlas \
       sgemm.atlas dgemm.atlas cgemm.atlas zgemm.atlas \
       strmm.atlas dtrmm.atlas ctrmm.atlas ztrmm.atlas \
       strsm.atlas dtrsm.atlas ctrsm.atlas ztrsm.atlas \
       ssyrk.atlas dsyrk.atlas csyrk.atlas zsyrk.atlas \
       ssyr2k.atlas dsyr2k.atlas csyr2k.atlas zsyr2k.atlas \
       sger.atlas dger.atlas cger.atlas zger.atlas\
       sdot.atlas ddot.atlas \
       saxpy.atlas daxpy.atlas caxpy.atlas zaxpy.atlas \
       scopy.atlas dcopy.atlas ccopy.atlas zcopy.atlas \
       sswap.atlas dswap.atlas cswap.atlas zswap.atlas \
       sscal.atlas dscal.atlas cscal.atlas zscal.atlas \
       sasum.atlas dasum.atlas casum.atlas zasum.atlas \
       ssymv.atlas dsymv.atlas csymv.atlas zsymv.atlas \
       chemv.atlas zhemv.atlas \
       chemm.acml zhemm.acml \
       chemm.atlas zhemm.atlas \
       cherk.atlas zherk.atlas \
       cher2k.atlas zher2k.atlas \
       sgemv.atlas dgemv.atlas cgemv.atlas zgemv.atlas \
       sgeev.atlas dgeev.atlas cgeev.atlas zgeev.atlas \
       sgesv.atlas dgesv.atlas cgesv.atlas zgesv.atlas \
       sgetri.atlas dgetri.atlas cgetri.atlas zgetri.atlas \
       spotrf.atlas dpotrf.atlas cpotrf.atlas zpotrf.atlas \
       ssymm.atlas dsymm.atlas csymm.atlas zsymm.atlas \
       isamax.atlas idamax.atlas icamax.atlas izamax.atlas \
       snrm2.goto dnrm2.goto scnrm2.goto dznrm2.goto

mkl :: slinpack.mkl dlinpack.mkl clinpack.mkl zlinpack.mkl \
       scholesky.mkl dcholesky.mkl ccholesky.mkl zcholesky.mkl \
       sgemm.mkl dgemm.mkl cgemm.mkl zgemm.mkl \
       strmm.mkl dtrmm.mkl ctrmm.mkl ztrmm.mkl \
       strsm.mkl dtrsm.mkl ctrsm.mkl ztrsm.mkl \
       ssyrk.mkl dsyrk.mkl csyrk.mkl zsyrk.mkl \
       ssyr2k.mkl dsyr2k.mkl csyr2k.mkl zsyr2k.mkl \
       sger.mkl dger.mkl cger.mkl zger.mkl \
       sdot.mkl ddot.mkl cdot.mkl zdot.mkl \
       saxpy.mkl daxpy.mkl caxpy.mkl zaxpy.mkl \
       scopy.mkl dcopy.mkl ccopy.mkl zcopy.mkl \
       sswap.mkl dswap.mkl cswap.mkl zswap.mkl \
       sscal.mkl dscal.mkl cscal.mkl zscal.mkl \
       sasum.mkl dasum.mkl casum.mkl zasum.mkl \
       ssymv.mkl dsymv.mkl csymv.mkl zsymv.mkl \
       chemv.mkl zhemv.mkl \
       chemm.mkl zhemm.mkl \
       cherk.mkl zherk.mkl \
       cher2k.mkl zher2k.mkl \
       sgemv.mkl dgemv.mkl cgemv.mkl zgemv.mkl \
       sgeev.mkl dgeev.mkl cgeev.mkl zgeev.mkl \
       sgesv.mkl dgesv.mkl cgesv.mkl zgesv.mkl \
       sgetri.mkl dgetri.mkl cgetri.mkl zgetri.mkl \
       spotrf.mkl dpotrf.mkl cpotrf.mkl zpotrf.mkl \
       ssymm.mkl dsymm.mkl csymm.mkl zsymm.mkl




endif

essl :: sgemm.essl strmm.essl dgemm.essl dtrmm.essl  \
	cgemm.essl ctrmm.essl zgemm.essl ztrmm.essl  \
	slinpack.essl clinpack.essl dlinpack.essl zlinpack.essl \
	scholesky.essl ccholesky.essl dcholesky.essl zcholesky.essl \
	strsm.essl dtrsm.essl ctrsm.essl ztrsm.essl

veclib :: slinpack.veclib dlinpack.veclib clinpack.veclib zlinpack.veclib \
       scholesky.veclib dcholesky.veclib ccholesky.veclib zcholesky.veclib \
       sgemm.veclib dgemm.veclib cgemm.veclib zgemm.veclib \
       strmm.veclib dtrmm.veclib ctrmm.veclib ztrmm.veclib \
       strsm.veclib dtrsm.veclib ctrsm.veclib ztrsm.veclib \
       ssyrk.veclib dsyrk.veclib csyrk.veclib zsyrk.veclib \
       ssyr2k.veclib dsyr2k.veclib csyr2k.veclib zsyr2k.veclib \
       sger.veclib dger.veclib cger.veclib zger.veclib \
       sdot.veclib ddot.veclib cdot.veclib zdot.veclib \
       saxpy.veclib daxpy.veclib caxpy.veclib zaxpy.veclib \
       scopy.veclib dcopy.veclib ccopy.veclib zcopy.veclib \
       sswap.veclib dswap.veclib cswap.veclib zswap.veclib \
       sscal.veclib dscal.veclib cscal.veclib zscal.veclib \
       sasum.veclib dasum.veclib casum.veclib zasum.veclib \
       ssymv.veclib dsymv.veclib csymv.veclib zsymv.veclib \
       chemv.veclib zhemv.veclib \
       chemm.veclib zhemm.veclib \
       cherk.veclib zherk.veclib \
       cher2k.veclib zher2k.veclib \
       sgemv.veclib dgemv.veclib cgemv.veclib zgemv.veclib \
       sgeev.veclib dgeev.veclib cgeev.veclib zgeev.veclib \
       sgesv.veclib dgesv.veclib cgesv.veclib zgesv.veclib \
       sgetri.veclib dgetri.veclib cgetri.veclib zgetri.veclib \
       spotrf.veclib dpotrf.veclib cpotrf.veclib zpotrf.veclib \
       ssymm.veclib dsymm.veclib csymm.veclib zsymm.veclib

goto_3m :: cgemm3m.goto zgemm3m.goto

mkl_3m :: cgemm3m.mkl zgemm3m.mkl

all :: goto mkl atlas acml veclib

exe :
	@./Make_exe.sh

##################################### Slinpack ####################################################
slinpack.goto : slinpack.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

slinpack.acml : slinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

slinpack.atlas : slinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

slinpack.mkl : slinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

slinpack.veclib : slinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

slinpack.essl : slinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dlinpack ####################################################
dlinpack.goto : dlinpack.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dlinpack.acml : dlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dlinpack.atlas : dlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dlinpack.mkl : dlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dlinpack.veclib : dlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dlinpack.essl : dlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Clinpack ####################################################

clinpack.goto : clinpack.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

clinpack.acml : clinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

clinpack.atlas : clinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

clinpack.mkl : clinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

clinpack.veclib : clinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

clinpack.essl : clinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zlinpack ####################################################

zlinpack.goto : zlinpack.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zlinpack.acml : zlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zlinpack.atlas : zlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zlinpack.mkl : zlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zlinpack.veclib : zlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zlinpack.essl : zlinpack.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Scholesky ###################################################

scholesky.goto : scholesky.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

scholesky.acml : scholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scholesky.atlas : scholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scholesky.mkl : scholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scholesky.veclib : scholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scholesky.essl : scholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dcholesky ###################################################

dcholesky.goto : dcholesky.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dcholesky.acml : dcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcholesky.atlas : dcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcholesky.mkl : dcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcholesky.veclib : dcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcholesky.essl : dcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ccholesky ###################################################

ccholesky.goto : ccholesky.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ccholesky.acml : ccholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccholesky.atlas : ccholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccholesky.mkl : ccholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccholesky.veclib : ccholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccholesky.essl : ccholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)


##################################### Zcholesky ###################################################

zcholesky.goto : zcholesky.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zcholesky.acml : zcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcholesky.atlas : zcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcholesky.mkl : zcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcholesky.veclib : zcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcholesky.essl : zcholesky.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sgemm ####################################################
sgemm.goto : sgemm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sgemm.acml : sgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemm.atlas : sgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemm.mkl : sgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemm.veclib : sgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemm.essl : sgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dgemm ####################################################
dgemm.goto : dgemm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dgemm.acml : dgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemm.atlas : dgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemm.mkl : dgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemm.veclib : dgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemm.essl : dgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cgemm ####################################################

cgemm.goto : cgemm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cgemm.acml : cgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemm.atlas : cgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemm.mkl : cgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemm.veclib : cgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemm.essl : cgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zgemm ####################################################

zgemm.goto : zgemm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zgemm.acml : zgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemm.atlas : zgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemm.mkl : zgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemm.veclib : zgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemm.essl : zgemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ssymm ####################################################
ssymm.goto : ssymm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ssymm.acml : ssymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssymm.atlas : ssymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssymm.mkl : ssymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssymm.veclib : ssymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dsymm ####################################################
dsymm.goto : dsymm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dsymm.acml : dsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsymm.atlas : dsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsymm.mkl : dsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsymm.veclib : dsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Csymm ####################################################

csymm.goto : csymm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

csymm.acml : csymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csymm.atlas : csymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csymm.mkl : csymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csymm.veclib : csymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zsymm ####################################################

zsymm.goto : zsymm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zsymm.acml : zsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsymm.atlas : zsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsymm.mkl : zsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsymm.veclib : zsymm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Strmm ####################################################
strmm.goto : strmm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

strmm.acml : strmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strmm.atlas : strmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strmm.mkl : strmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strmm.veclib : strmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strmm.essl : strmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dtrmm ####################################################
dtrmm.goto : dtrmm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dtrmm.acml : dtrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrmm.atlas : dtrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrmm.mkl : dtrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrmm.veclib : dtrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrmm.essl : dtrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ctrmm ####################################################

ctrmm.goto : ctrmm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ctrmm.acml : ctrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrmm.atlas : ctrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrmm.mkl : ctrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrmm.veclib : ctrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrmm.essl : ctrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ztrmm ####################################################

ztrmm.goto : ztrmm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ztrmm.acml : ztrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrmm.atlas : ztrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrmm.mkl : ztrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrmm.veclib : ztrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrmm.essl : ztrmm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Strsm ####################################################
strsm.goto : strsm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

strsm.acml : strsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strsm.atlas : strsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strsm.mkl : strsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strsm.veclib : strsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

strsm.essl : strsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dtrsm ####################################################
dtrsm.goto : dtrsm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dtrsm.acml : dtrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrsm.atlas : dtrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrsm.mkl : dtrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrsm.veclib : dtrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dtrsm.essl : dtrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ctrsm ####################################################

ctrsm.goto : ctrsm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ctrsm.acml : ctrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrsm.atlas : ctrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrsm.mkl : ctrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrsm.veclib : ctrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ctrsm.essl : ctrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ztrsm ####################################################

ztrsm.goto : ztrsm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ztrsm.acml : ztrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrsm.atlas : ztrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrsm.mkl : ztrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrsm.veclib : ztrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ztrsm.essl : ztrsm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBESSL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ssyrk ####################################################
ssyrk.goto : ssyrk.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ssyrk.acml : ssyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssyrk.atlas : ssyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssyrk.mkl : ssyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssyrk.veclib : ssyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dsyrk ####################################################
dsyrk.goto : dsyrk.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dsyrk.acml : dsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsyrk.atlas : dsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsyrk.mkl : dsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsyrk.veclib : dsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Csyrk ####################################################

csyrk.goto : csyrk.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

csyrk.acml : csyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csyrk.atlas : csyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csyrk.mkl : csyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csyrk.veclib : csyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zsyrk ####################################################

zsyrk.goto : zsyrk.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zsyrk.acml : zsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsyrk.atlas : zsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsyrk.mkl : zsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsyrk.veclib : zsyrk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ssyr2k ####################################################
ssyr2k.goto : ssyr2k.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ssyr2k.acml : ssyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssyr2k.atlas : ssyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssyr2k.mkl : ssyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssyr2k.veclib : ssyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dsyr2k ####################################################
dsyr2k.goto : dsyr2k.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dsyr2k.acml : dsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsyr2k.atlas : dsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsyr2k.mkl : dsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsyr2k.veclib : dsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Csyr2k ####################################################

csyr2k.goto : csyr2k.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

csyr2k.acml : csyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csyr2k.atlas : csyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csyr2k.mkl : csyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csyr2k.veclib : csyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zsyr2k ####################################################

zsyr2k.goto : zsyr2k.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zsyr2k.acml : zsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsyr2k.atlas : zsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsyr2k.mkl : zsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsyr2k.veclib : zsyr2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Chemm ####################################################

chemm.goto : chemm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

chemm.acml : chemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

chemm.atlas : chemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

chemm.mkl : chemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

chemm.veclib : chemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zhemm ####################################################

zhemm.goto : zhemm.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zhemm.acml : zhemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zhemm.atlas : zhemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zhemm.mkl : zhemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zhemm.veclib : zhemm.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cherk ####################################################

cherk.goto : cherk.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cherk.acml : cherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cherk.atlas : cherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cherk.mkl : cherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cherk.veclib : cherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zherk ####################################################

zherk.goto : zherk.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zherk.acml : zherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zherk.atlas : zherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zherk.mkl : zherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zherk.veclib : zherk.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cher2k ####################################################

cher2k.goto : cher2k.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cher2k.acml : cher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cher2k.atlas : cher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cher2k.mkl : cher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cher2k.veclib : cher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zher2k ####################################################

zher2k.goto : zher2k.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zher2k.acml : zher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zher2k.atlas : zher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zher2k.mkl : zher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zher2k.veclib : zher2k.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sgemv ####################################################
sgemv.goto : sgemv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sgemv.acml : sgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemv.atlas : sgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemv.mkl : sgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgemv.veclib : sgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dgemv ####################################################
dgemv.goto : dgemv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dgemv.acml : dgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemv.atlas : dgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemv.mkl : dgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgemv.veclib : dgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cgemv ####################################################

cgemv.goto : cgemv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cgemv.acml : cgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemv.atlas : cgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemv.mkl : cgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemv.veclib : cgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zgemv ####################################################

zgemv.goto : zgemv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zgemv.acml : zgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemv.atlas : zgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemv.mkl : zgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemv.veclib : zgemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sger ####################################################
sger.goto : sger.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sger.acml : sger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sger.atlas : sger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sger.mkl : sger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sger.veclib : sger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dger ####################################################
dger.goto : dger.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dger.acml : dger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dger.atlas : dger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dger.mkl : dger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dger.veclib : dger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cger ####################################################
cger.goto : cger.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cger.acml : cger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cger.atlas : cger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cger.mkl : cger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cger.veclib : cger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zger ####################################################
zger.goto : zger.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zger.acml : zger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zger.atlas : zger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zger.mkl : zger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zger.veclib : zger.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ssymv ####################################################
ssymv.goto : ssymv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ssymv.acml : ssymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssymv.atlas : ssymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssymv.mkl : ssymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ssymv.veclib : ssymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dsymv ####################################################
dsymv.goto : dsymv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dsymv.acml : dsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsymv.atlas : dsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsymv.mkl : dsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dsymv.veclib : dsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Csymv ####################################################
csymv.goto : csymv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

csymv.acml : csymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csymv.atlas : csymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csymv.mkl : csymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

csymv.veclib : csymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dsymv ####################################################
zsymv.goto : zsymv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zsymv.acml : zsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsymv.atlas : zsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsymv.mkl : zsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zsymv.veclib : zsymv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sgeev ####################################################
sgeev.goto : sgeev.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sgeev.acml : sgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgeev.atlas : sgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgeev.mkl : sgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgeev.veclib : sgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dgeev ####################################################
dgeev.goto : dgeev.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dgeev.acml : dgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgeev.atlas : dgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgeev.mkl : dgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgeev.veclib : dgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cgeev ####################################################

cgeev.goto : cgeev.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cgeev.acml : cgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgeev.atlas : cgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgeev.mkl : cgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgeev.veclib : cgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zgeev ####################################################

zgeev.goto : zgeev.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zgeev.acml : zgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgeev.atlas : zgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgeev.mkl : zgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgeev.veclib : zgeev.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sgetri ####################################################
sgetri.goto : sgetri.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sgetri.acml : sgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgetri.atlas : sgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgetri.mkl : sgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgetri.veclib : sgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dgetri ####################################################
dgetri.goto : dgetri.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dgetri.acml : dgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgetri.atlas : dgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgetri.mkl : dgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgetri.veclib : dgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cgetri ####################################################

cgetri.goto : cgetri.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cgetri.acml : cgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgetri.atlas : cgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgetri.mkl : cgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgetri.veclib : cgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zgetri ####################################################

zgetri.goto : zgetri.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zgetri.acml : zgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgetri.atlas : zgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgetri.mkl : zgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgetri.veclib : zgetri.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Spotrf ####################################################
spotrf.goto : spotrf.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

spotrf.acml : spotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

spotrf.atlas : spotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

spotrf.mkl : spotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

spotrf.veclib : spotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dpotrf ####################################################
dpotrf.goto : dpotrf.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dpotrf.acml : dpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dpotrf.atlas : dpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dpotrf.mkl : dpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dpotrf.veclib : dpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cpotrf ####################################################

cpotrf.goto : cpotrf.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cpotrf.acml : cpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cpotrf.atlas : cpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cpotrf.mkl : cpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cpotrf.veclib : cpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zpotrf ####################################################

zpotrf.goto : zpotrf.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zpotrf.acml : zpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zpotrf.atlas : zpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zpotrf.mkl : zpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zpotrf.veclib : zpotrf.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Chemv ####################################################

chemv.goto : chemv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

chemv.acml : chemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

chemv.atlas : chemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

chemv.mkl : chemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

chemv.veclib : chemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zhemv ####################################################

zhemv.goto : zhemv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zhemv.acml : zhemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zhemv.atlas : zhemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zhemv.mkl : zhemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zhemv.veclib : zhemv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sdot ####################################################
sdot.goto : sdot.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sdot.acml : sdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sdot.atlas : sdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sdot.mkl : sdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sdot.veclib : sdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ddot ####################################################
ddot.goto : ddot.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ddot.acml : ddot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ddot.atlas : ddot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ddot.mkl : ddot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ddot.veclib : ddot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cdot ####################################################
cdot.goto : cdot.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cdot.acml : cdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cdot.atlas : cdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cdot.mkl : cdot-intel.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cdot.veclib : cdot-intel.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zdot ####################################################
zdot.goto : zdot.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zdot.acml : zdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zdot.atlas : zdot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zdot.mkl : zdot-intel.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zdot.veclib : zdot-intel.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Srot ####################################################
srot.goto : srot.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

srot.acml : srot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

srot.atlas : srot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

srot.mkl : srot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

srot.veclib : srot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Drot ####################################################
drot.goto : drot.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

drot.acml : drot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

drot.atlas : drot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

drot.mkl : drot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

drot.veclib : drot.$(SUFFIX)
	$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)


##################################### Saxpy ####################################################
saxpy.goto : saxpy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

saxpy.acml : saxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

saxpy.atlas : saxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

saxpy.mkl : saxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

saxpy.veclib : saxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Daxpy ####################################################
daxpy.goto : daxpy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

daxpy.acml : daxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

daxpy.atlas : daxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

daxpy.mkl : daxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

daxpy.veclib : daxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Caxpy ####################################################

caxpy.goto : caxpy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

caxpy.acml : caxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

caxpy.atlas : caxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

caxpy.mkl : caxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

caxpy.veclib : caxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zaxpy ####################################################

zaxpy.goto : zaxpy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zaxpy.acml : zaxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zaxpy.atlas : zaxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zaxpy.mkl : zaxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zaxpy.veclib : zaxpy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)


##################################### Scopy ####################################################
scopy.goto : scopy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

scopy.acml : scopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scopy.atlas : scopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scopy.mkl : scopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

scopy.veclib : scopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dcopy ####################################################
dcopy.goto : dcopy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dcopy.acml : dcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcopy.atlas : dcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcopy.mkl : dcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dcopy.veclib : dcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Ccopy ####################################################

ccopy.goto : ccopy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

ccopy.acml : ccopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccopy.atlas : ccopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccopy.mkl : ccopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

ccopy.veclib : ccopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zcopy ####################################################

zcopy.goto : zcopy.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zcopy.acml : zcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcopy.atlas : zcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcopy.mkl : zcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zcopy.veclib : zcopy.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sscal ####################################################
sscal.goto : sscal.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sscal.acml : sscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sscal.atlas : sscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sscal.mkl : sscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sscal.veclib : sscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dscal ####################################################
dscal.goto : dscal.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dscal.acml : dscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dscal.atlas : dscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dscal.mkl : dscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dscal.veclib : dscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cscal ####################################################

cscal.goto : cscal.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cscal.acml : cscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cscal.atlas : cscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cscal.mkl : cscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cscal.veclib : cscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zscal ####################################################

zscal.goto : zscal.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zscal.acml : zscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zscal.atlas : zscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zscal.mkl : zscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zscal.veclib : zscal.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sasum ####################################################
sasum.goto : sasum.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sasum.acml : sasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sasum.atlas : sasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sasum.mkl : sasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sasum.veclib : sasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dasum ####################################################
dasum.goto : dasum.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dasum.acml : dasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dasum.atlas : dasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dasum.mkl : dasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dasum.veclib : dasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Casum ####################################################

casum.goto : casum.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

casum.acml : casum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

casum.atlas : casum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

casum.mkl : casum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

casum.veclib : casum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zasum ####################################################

zasum.goto : zasum.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zasum.acml : zasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zasum.atlas : zasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zasum.mkl : zasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zasum.veclib : zasum.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Sswap ####################################################
sswap.goto : sswap.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sswap.acml : sswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sswap.atlas : sswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sswap.mkl : sswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sswap.veclib : sswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dswap ####################################################
dswap.goto : dswap.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dswap.acml : dswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dswap.atlas : dswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dswap.mkl : dswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dswap.veclib : dswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cswap ####################################################

cswap.goto : cswap.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cswap.acml : cswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cswap.atlas : cswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cswap.mkl : cswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cswap.veclib : cswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zswap ####################################################

zswap.goto : zswap.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zswap.acml : zswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zswap.atlas : zswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zswap.mkl : zswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zswap.veclib : zswap.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)


##################################### Sgesv ####################################################
sgesv.goto : sgesv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

sgesv.acml : sgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgesv.atlas : sgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgesv.mkl : sgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

sgesv.veclib : sgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Dgesv ####################################################
dgesv.goto : dgesv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dgesv.acml : dgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgesv.atlas : dgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgesv.mkl : dgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

dgesv.veclib : dgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Cgesv ####################################################

cgesv.goto : cgesv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cgesv.acml : cgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgesv.atlas : cgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgesv.mkl : cgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgesv.veclib : cgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zgesv ####################################################

zgesv.goto : zgesv.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zgesv.acml : zgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBACML) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgesv.atlas : zgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgesv.mkl : zgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgesv.veclib : zgesv.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)


##################################### Cgemm3m ####################################################

cgemm3m.goto : cgemm3m.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

cgemm3m.mkl : cgemm3m.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

cgemm3m.veclib : cgemm3m.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

##################################### Zgemm3m ####################################################

zgemm3m.goto : zgemm3m.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

zgemm3m.mkl : zgemm3m.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBMKL) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

zgemm3m.veclib : zgemm3m.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBVECLIB) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## ISAMAX ##############################################
isamax.goto : isamax.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

isamax.atlas : isamax.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## IDAMAX ##############################################
idamax.goto : idamax.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

idamax.atlas : idamax.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## ICAMAX ##############################################
icamax.goto : icamax.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

icamax.atlas : icamax.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## IZAMAX ##############################################
izamax.goto : izamax.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

izamax.atlas : izamax.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## SNRM2 ##############################################
snrm2.goto : snrm2.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

snrm2.atlas : snrm2.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## DNRM2 ##############################################
dnrm2.goto : dnrm2.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dnrm2.atlas : dnrm2.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## Sscnrm2 ##############################################
scnrm2.goto : scnrm2.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

scnrm2.atlas : scnrm2.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)

############################################## Ddznrm2 ##############################################
dznrm2.goto : dznrm2.$(SUFFIX) ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB) -lm

dznrm2.atlas : dznrm2.$(SUFFIX)
	-$(CC) $(CFLAGS) -o $(@F) $^ $(LIBATLAS) $(CEXTRALIB) $(EXTRALIB) $(FEXTRALIB)


###################################################################################################

slinpack.$(SUFFIX) : linpack.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dlinpack.$(SUFFIX) : linpack.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

clinpack.$(SUFFIX) : linpack.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zlinpack.$(SUFFIX) : linpack.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

scholesky.$(SUFFIX) : cholesky.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dcholesky.$(SUFFIX) : cholesky.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

ccholesky.$(SUFFIX) : cholesky.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zcholesky.$(SUFFIX) : cholesky.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sgemm.$(SUFFIX) : gemm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dgemm.$(SUFFIX) : gemm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cgemm.$(SUFFIX) : gemm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zgemm.$(SUFFIX) : gemm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

ssymm.$(SUFFIX) : symm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dsymm.$(SUFFIX) : symm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

csymm.$(SUFFIX) : symm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zsymm.$(SUFFIX) : symm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

strmm.$(SUFFIX) : trmm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dtrmm.$(SUFFIX) : trmm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

ctrmm.$(SUFFIX) : trmm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

ztrmm.$(SUFFIX) : trmm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

strsm.$(SUFFIX) : trsm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dtrsm.$(SUFFIX) : trsm.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

ctrsm.$(SUFFIX) : trsm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

ztrsm.$(SUFFIX) : trsm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

ssyrk.$(SUFFIX) : syrk.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dsyrk.$(SUFFIX) : syrk.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

csyrk.$(SUFFIX) : syrk.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zsyrk.$(SUFFIX) : syrk.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

ssyr2k.$(SUFFIX) : syr2k.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dsyr2k.$(SUFFIX) : syr2k.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

csyr2k.$(SUFFIX) : syr2k.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zsyr2k.$(SUFFIX) : syr2k.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

chemm.$(SUFFIX) : hemm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zhemm.$(SUFFIX) : hemm.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

cherk.$(SUFFIX) : herk.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zherk.$(SUFFIX) : herk.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

cher2k.$(SUFFIX) : her2k.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zher2k.$(SUFFIX) : her2k.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sgemv.$(SUFFIX) : gemv.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dgemv.$(SUFFIX) : gemv.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cgemv.$(SUFFIX) : gemv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zgemv.$(SUFFIX) : gemv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sger.$(SUFFIX) : ger.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dger.$(SUFFIX) : ger.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cger.$(SUFFIX) : ger.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zger.$(SUFFIX) : ger.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^


ssymv.$(SUFFIX) : symv.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dsymv.$(SUFFIX) : symv.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

csymv.$(SUFFIX) : symv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zsymv.$(SUFFIX) : symv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sgeev.$(SUFFIX) : geev.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dgeev.$(SUFFIX) : geev.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cgeev.$(SUFFIX) : geev.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zgeev.$(SUFFIX) : geev.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sgetri.$(SUFFIX) : getri.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dgetri.$(SUFFIX) : getri.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cgetri.$(SUFFIX) : getri.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zgetri.$(SUFFIX) : getri.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

spotrf.$(SUFFIX) : potrf.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dpotrf.$(SUFFIX) : potrf.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cpotrf.$(SUFFIX) : potrf.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zpotrf.$(SUFFIX) : potrf.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

chemv.$(SUFFIX) : hemv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zhemv.$(SUFFIX) : hemv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sdot.$(SUFFIX) : dot.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

ddot.$(SUFFIX) : dot.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cdot.$(SUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zdot.$(SUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

cdot-intel.$(SUFFIX) : zdot-intel.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zdot-intel.$(SUFFIX) : zdot-intel.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^



saxpy.$(SUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

daxpy.$(SUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

caxpy.$(SUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zaxpy.$(SUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

scopy.$(SUFFIX) : copy.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dcopy.$(SUFFIX) : copy.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

ccopy.$(SUFFIX) : copy.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zcopy.$(SUFFIX) : copy.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sswap.$(SUFFIX) : swap.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dswap.$(SUFFIX) : swap.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cswap.$(SUFFIX) : swap.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zswap.$(SUFFIX) : swap.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^



sscal.$(SUFFIX) : scal.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dscal.$(SUFFIX) : scal.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cscal.$(SUFFIX) : scal.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zscal.$(SUFFIX) : scal.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

sasum.$(SUFFIX) : asum.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dasum.$(SUFFIX) : asum.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

casum.$(SUFFIX) : asum.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zasum.$(SUFFIX) : asum.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^


sgesv.$(SUFFIX) : gesv.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dgesv.$(SUFFIX) : gesv.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

cgesv.$(SUFFIX) : gesv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zgesv.$(SUFFIX) : gesv.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^

srot.$(SUFFIX) : rot.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

drot.$(SUFFIX) : rot.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^





cgemm3m.$(SUFFIX) : gemm3m.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

zgemm3m.$(SUFFIX) : gemm3m.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^


isamax.$(SUFFIX) : iamax.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

idamax.$(SUFFIX) : iamax.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

icamax.$(SUFFIX) : iamax.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

izamax.$(SUFFIX) : iamax.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^


snrm2.$(SUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -UDOUBLE -o $(@F) $^

dnrm2.$(SUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c -UCOMPLEX -DDOUBLE -o $(@F) $^

scnrm2.$(SUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -UDOUBLE -o $(@F) $^

dznrm2.$(SUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c -DCOMPLEX -DDOUBLE -o $(@F) $^


smallscaling: smallscaling.c ../$(LIBNAME)
	$(CC) $(CFLAGS) -o $(@F) $^ $(EXTRALIB) -fopenmp -lm -lpthread

clean ::
	@rm -f *.goto *.mkl *.acml *.atlas *.veclib *.essl smallscaling

include $(TOPDIR)/Makefile.tail

