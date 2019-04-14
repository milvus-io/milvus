TOPDIR	= ..
include $(TOPDIR)/Makefile.system

SUPPORT_GEMM3M = 0

ifeq ($(ARCH), x86)
SUPPORT_GEMM3M = 1
endif

ifeq ($(ARCH), x86_64)
SUPPORT_GEMM3M = 1
endif

ifeq ($(ARCH), ia64)
SUPPORT_GEMM3M = 1
endif

ifeq ($(ARCH), MIPS)
SUPPORT_GEMM3M = 1
endif

ifndef NO_FBLAS

SBLAS1OBJS    = \
		saxpy.$(SUFFIX) sswap.$(SUFFIX) \
		scopy.$(SUFFIX) sscal.$(SUFFIX) \
		sdot.$(SUFFIX) sdsdot.$(SUFFIX) dsdot.$(SUFFIX) \
		sasum.$(SUFFIX) snrm2.$(SUFFIX) \
		smax.$(SUFFIX) samax.$(SUFFIX) ismax.$(SUFFIX) isamax.$(SUFFIX) \
		smin.$(SUFFIX) samin.$(SUFFIX) ismin.$(SUFFIX) isamin.$(SUFFIX) \
		srot.$(SUFFIX) srotg.$(SUFFIX) srotm.$(SUFFIX) srotmg.$(SUFFIX) \
		saxpby.$(SUFFIX)

SBLAS2OBJS    = \
		sgemv.$(SUFFIX) sger.$(SUFFIX) \
		strsv.$(SUFFIX) strmv.$(SUFFIX) ssymv.$(SUFFIX) \
		ssyr.$(SUFFIX)  ssyr2.$(SUFFIX) sgbmv.$(SUFFIX) \
		ssbmv.$(SUFFIX) sspmv.$(SUFFIX) \
		sspr.$(SUFFIX)  sspr2.$(SUFFIX) \
		stbsv.$(SUFFIX) stbmv.$(SUFFIX) \
		stpsv.$(SUFFIX) stpmv.$(SUFFIX)

SBLAS3OBJS    = \
		sgemm.$(SUFFIX) ssymm.$(SUFFIX) strmm.$(SUFFIX) \
		strsm.$(SUFFIX) ssyrk.$(SUFFIX) ssyr2k.$(SUFFIX) \
		somatcopy.$(SUFFIX) simatcopy.$(SUFFIX)\
		sgeadd.$(SUFFIX)


DBLAS1OBJS    = \
		daxpy.$(SUFFIX) dswap.$(SUFFIX) \
		dcopy.$(SUFFIX) dscal.$(SUFFIX) \
		ddot.$(SUFFIX) \
		dasum.$(SUFFIX) dnrm2.$(SUFFIX) \
		dmax.$(SUFFIX) damax.$(SUFFIX) idmax.$(SUFFIX) idamax.$(SUFFIX) \
		dmin.$(SUFFIX) damin.$(SUFFIX) idmin.$(SUFFIX) idamin.$(SUFFIX) \
		drot.$(SUFFIX) drotg.$(SUFFIX) drotm.$(SUFFIX) drotmg.$(SUFFIX) \
		daxpby.$(SUFFIX)

DBLAS2OBJS    = \
		dgemv.$(SUFFIX) dger.$(SUFFIX) \
		dtrsv.$(SUFFIX) dtrmv.$(SUFFIX) dsymv.$(SUFFIX) \
		dsyr.$(SUFFIX)  dsyr2.$(SUFFIX) dgbmv.$(SUFFIX) \
		dsbmv.$(SUFFIX) dspmv.$(SUFFIX) \
		dspr.$(SUFFIX)  dspr2.$(SUFFIX) \
		dtbsv.$(SUFFIX) dtbmv.$(SUFFIX) \
		dtpsv.$(SUFFIX) dtpmv.$(SUFFIX)

DBLAS3OBJS    = \
		dgemm.$(SUFFIX) dsymm.$(SUFFIX) dtrmm.$(SUFFIX) \
		dtrsm.$(SUFFIX) dsyrk.$(SUFFIX) dsyr2k.$(SUFFIX) \
		domatcopy.$(SUFFIX) dimatcopy.$(SUFFIX)\
		dgeadd.$(SUFFIX) 

CBLAS1OBJS    = \
		caxpy.$(SUFFIX) caxpyc.$(SUFFIX) cswap.$(SUFFIX) \
		ccopy.$(SUFFIX) cscal.$(SUFFIX) csscal.$(SUFFIX) \
		cdotc.$(SUFFIX)  cdotu.$(SUFFIX) \
		scasum.$(SUFFIX) scnrm2.$(SUFFIX) \
		scamax.$(SUFFIX) icamax.$(SUFFIX) \
		scamin.$(SUFFIX) icamin.$(SUFFIX) \
		csrot.$(SUFFIX) crotg.$(SUFFIX) \
		caxpby.$(SUFFIX)

CBLAS2OBJS    = \
		cgemv.$(SUFFIX) cgeru.$(SUFFIX) cgerc.$(SUFFIX) \
		ctrsv.$(SUFFIX) ctrmv.$(SUFFIX) \
		csyr2.$(SUFFIX) cgbmv.$(SUFFIX) \
		csbmv.$(SUFFIX) \
		cspr2.$(SUFFIX) \
		ctbsv.$(SUFFIX) ctbmv.$(SUFFIX) \
		ctpsv.$(SUFFIX) ctpmv.$(SUFFIX) \
		chemv.$(SUFFIX) chbmv.$(SUFFIX) \
		cher.$(SUFFIX)  cher2.$(SUFFIX) \
		chpmv.$(SUFFIX) chpr.$(SUFFIX)  chpr2.$(SUFFIX)

CBLAS3OBJS    = \
		cgemm.$(SUFFIX) csymm.$(SUFFIX) ctrmm.$(SUFFIX) \
		ctrsm.$(SUFFIX) csyrk.$(SUFFIX) csyr2k.$(SUFFIX) \
	       	chemm.$(SUFFIX) cherk.$(SUFFIX) cher2k.$(SUFFIX) \
		comatcopy.$(SUFFIX) cimatcopy.$(SUFFIX)\
		cgeadd.$(SUFFIX) 

ZBLAS1OBJS    = \
		zaxpy.$(SUFFIX) zaxpyc.$(SUFFIX) zswap.$(SUFFIX) \
		zcopy.$(SUFFIX) zscal.$(SUFFIX) zdscal.$(SUFFIX) \
		zdotc.$(SUFFIX)  zdotu.$(SUFFIX) \
		dzasum.$(SUFFIX)  dznrm2.$(SUFFIX) \
		dzamax.$(SUFFIX) izamax.$(SUFFIX) \
		dzamin.$(SUFFIX) izamin.$(SUFFIX) \
		zdrot.$(SUFFIX) zrotg.$(SUFFIX) \
		zaxpby.$(SUFFIX)

ZBLAS2OBJS    = \
		zgemv.$(SUFFIX) zgeru.$(SUFFIX) zgerc.$(SUFFIX) \
		ztrsv.$(SUFFIX) ztrmv.$(SUFFIX) \
		zsyr2.$(SUFFIX) zgbmv.$(SUFFIX) \
		zsbmv.$(SUFFIX) \
		zspr2.$(SUFFIX) \
		ztbsv.$(SUFFIX) ztbmv.$(SUFFIX) \
		ztpsv.$(SUFFIX) ztpmv.$(SUFFIX) \
		zhemv.$(SUFFIX) zhbmv.$(SUFFIX) \
		zher.$(SUFFIX)  zher2.$(SUFFIX) \
		zhpmv.$(SUFFIX) zhpr.$(SUFFIX)  zhpr2.$(SUFFIX)

ZBLAS3OBJS    = \
		zgemm.$(SUFFIX) zsymm.$(SUFFIX) ztrmm.$(SUFFIX) \
		ztrsm.$(SUFFIX) zsyrk.$(SUFFIX) zsyr2k.$(SUFFIX) \
	       	zhemm.$(SUFFIX) zherk.$(SUFFIX) zher2k.$(SUFFIX) \
		zomatcopy.$(SUFFIX) zimatcopy.$(SUFFIX)\
		zgeadd.$(SUFFIX) 

ifeq ($(SUPPORT_GEMM3M), 1)

# CBLAS3OBJS   +=  cgemm3m.$(SUFFIX) csymm3m.$(SUFFIX) chemm3m.$(SUFFIX)
CBLAS3OBJS   +=  cgemm3m.$(SUFFIX)

# ZBLAS3OBJS   +=  zgemm3m.$(SUFFIX) zsymm3m.$(SUFFIX) zhemm3m.$(SUFFIX)
ZBLAS3OBJS   +=  zgemm3m.$(SUFFIX)

endif

ifdef EXPRECISION

QBLAS1OBJS    = \
		qaxpy.$(SUFFIX) qswap.$(SUFFIX) \
		qcopy.$(SUFFIX) qscal.$(SUFFIX) \
		qdot.$(SUFFIX) \
		qasum.$(SUFFIX) qnrm2.$(SUFFIX) \
		qmax.$(SUFFIX) qamax.$(SUFFIX) iqmax.$(SUFFIX) iqamax.$(SUFFIX) \
		qmin.$(SUFFIX) qamin.$(SUFFIX) iqmin.$(SUFFIX) iqamin.$(SUFFIX) \
		qrot.$(SUFFIX) qrotg.$(SUFFIX) qrotm.$(SUFFIX) qrotmg.$(SUFFIX) \

QBLAS2OBJS    = \
		qgemv.$(SUFFIX) qger.$(SUFFIX) \
		qtrsv.$(SUFFIX) qtrmv.$(SUFFIX) qsymv.$(SUFFIX) \
		qsyr.$(SUFFIX)  qsyr2.$(SUFFIX) qgbmv.$(SUFFIX) \
		qsbmv.$(SUFFIX) qspmv.$(SUFFIX) \
		qspr.$(SUFFIX)  qspr2.$(SUFFIX) \
		qtbsv.$(SUFFIX) qtbmv.$(SUFFIX) \
		qtpsv.$(SUFFIX) qtpmv.$(SUFFIX)

QBLAS3OBJS    = \
		qgemm.$(SUFFIX) qsymm.$(SUFFIX) qtrmm.$(SUFFIX) \
		qtrsm.$(SUFFIX) qsyrk.$(SUFFIX) qsyr2k.$(SUFFIX)

XBLAS1OBJS    = \
		xaxpy.$(SUFFIX) xaxpyc.$(SUFFIX) xswap.$(SUFFIX) \
		xcopy.$(SUFFIX) xscal.$(SUFFIX) xqscal.$(SUFFIX) \
		xdotc.$(SUFFIX)  xdotu.$(SUFFIX) \
		qxasum.$(SUFFIX) qxnrm2.$(SUFFIX) \
		qxamax.$(SUFFIX) ixamax.$(SUFFIX) \
		qxamin.$(SUFFIX) ixamin.$(SUFFIX) \
		xqrot.$(SUFFIX) xrotg.$(SUFFIX) \

XBLAS2OBJS    = \
		xgemv.$(SUFFIX) xgeru.$(SUFFIX) xgerc.$(SUFFIX) \
		xtrsv.$(SUFFIX) xtrmv.$(SUFFIX) xsymv.$(SUFFIX) \
		xsyr.$(SUFFIX)  xsyr2.$(SUFFIX) xgbmv.$(SUFFIX) \
		xsbmv.$(SUFFIX) xspmv.$(SUFFIX) \
		xspr.$(SUFFIX)  xspr2.$(SUFFIX) \
		xtbsv.$(SUFFIX) xtbmv.$(SUFFIX) \
		xtpsv.$(SUFFIX) xtpmv.$(SUFFIX) \
		xhemv.$(SUFFIX) xhbmv.$(SUFFIX) \
		xher.$(SUFFIX)  xher2.$(SUFFIX) \
		xhpmv.$(SUFFIX) xhpr.$(SUFFIX)  xhpr2.$(SUFFIX)

XBLAS3OBJS    = \
		xgemm.$(SUFFIX) xsymm.$(SUFFIX) xtrmm.$(SUFFIX) \
		xtrsm.$(SUFFIX) xsyrk.$(SUFFIX) xsyr2k.$(SUFFIX) \
		xhemm.$(SUFFIX) xherk.$(SUFFIX) xher2k.$(SUFFIX)

ifeq ($(SUPPORT_GEMM3M), 1)

XBLAS3OBJS   +=  xgemm3m.$(SUFFIX) xsymm3m.$(SUFFIX) xhemm3m.$(SUFFIX)

endif

endif

ifdef QUAD_PRECISION

QBLAS1OBJS    = \
		qaxpy.$(SUFFIX) qswap.$(SUFFIX) \
		qcopy.$(SUFFIX) qscal.$(SUFFIX) \
		qasum.$(SUFFIX) qnrm2.$(SUFFIX) \
		qmax.$(SUFFIX) qamax.$(SUFFIX) iqmax.$(SUFFIX) iqamax.$(SUFFIX) \
		qmin.$(SUFFIX) qamin.$(SUFFIX) iqmin.$(SUFFIX) iqamin.$(SUFFIX) \
		qrot.$(SUFFIX) qrotg.$(SUFFIX) qrotm.$(SUFFIX) qrotmg.$(SUFFIX) \

QBLAS2OBJS    = \
		qgemv.$(SUFFIX) qger.$(SUFFIX) \
		qtrsv.$(SUFFIX) qtrmv.$(SUFFIX) qsymv.$(SUFFIX) \
		qsyr.$(SUFFIX)  qsyr2.$(SUFFIX) qgbmv.$(SUFFIX) \
		qsbmv.$(SUFFIX) qspmv.$(SUFFIX) \
		qspr.$(SUFFIX)  qspr2.$(SUFFIX) \
		qtbsv.$(SUFFIX) qtbmv.$(SUFFIX) \
		qtpsv.$(SUFFIX) qtpmv.$(SUFFIX)

QBLAS3OBJS    = \
		qgemm.$(SUFFIX) qsymm.$(SUFFIX) qtrmm.$(SUFFIX) \
		qtrsm.$(SUFFIX) qsyrk.$(SUFFIX) qsyr2k.$(SUFFIX)

XBLAS1OBJS    = \
		xaxpy.$(SUFFIX) xaxpyc.$(SUFFIX) xswap.$(SUFFIX) \
		xcopy.$(SUFFIX) xscal.$(SUFFIX) xqscal.$(SUFFIX) \
		qxasum.$(SUFFIX) qxnrm2.$(SUFFIX) \
		qxamax.$(SUFFIX) ixamax.$(SUFFIX) \
		qxamin.$(SUFFIX) ixamin.$(SUFFIX) \
		xqrot.$(SUFFIX) xrotg.$(SUFFIX) \

XBLAS2OBJS    = \
		xgemv.$(SUFFIX) xgeru.$(SUFFIX) xgerc.$(SUFFIX) \
		xtrsv.$(SUFFIX) xtrmv.$(SUFFIX) xsymv.$(SUFFIX) \
		xsyr.$(SUFFIX)  xsyr2.$(SUFFIX) xgbmv.$(SUFFIX) \
		xsbmv.$(SUFFIX) xspmv.$(SUFFIX) \
		xspr.$(SUFFIX)  xspr2.$(SUFFIX) \
		xtbsv.$(SUFFIX) xtbmv.$(SUFFIX) \
		xtpsv.$(SUFFIX) xtpmv.$(SUFFIX) \
		xhemv.$(SUFFIX) xhbmv.$(SUFFIX) \
		xher.$(SUFFIX)  xher2.$(SUFFIX) \
		xhpmv.$(SUFFIX) xhpr.$(SUFFIX)  xhpr2.$(SUFFIX)

XBLAS3OBJS    = \
		xgemm.$(SUFFIX) xsymm.$(SUFFIX) xtrmm.$(SUFFIX) \
		xtrsm.$(SUFFIX) xsyrk.$(SUFFIX) xsyr2k.$(SUFFIX) \
		xhemm.$(SUFFIX) xherk.$(SUFFIX) xher2k.$(SUFFIX)

ifeq ($(SUPPORT_GEMM3M), 1)

XBLAS3OBJS   +=  xgemm3m.$(SUFFIX) xsymm3m.$(SUFFIX) xhemm3m.$(SUFFIX)

endif
endif

endif

HPLOBJS = dgemm.$(SUFFIX) dtrsm.$(SUFFIX) \
	  dgemv.$(SUFFIX)  dtrsv.$(SUFFIX) dger.$(SUFFIX) \
	  idamax.$(SUFFIX) daxpy.$(SUFFIX) dcopy.$(SUFFIX) dscal.$(SUFFIX)

CSBLAS1OBJS   = \
	cblas_isamax.$(SUFFIX) cblas_isamin.$(SUFFIX) cblas_sasum.$(SUFFIX) cblas_saxpy.$(SUFFIX) \
	cblas_scopy.$(SUFFIX) cblas_sdot.$(SUFFIX) cblas_sdsdot.$(SUFFIX) cblas_dsdot.$(SUFFIX) \
	cblas_srot.$(SUFFIX) cblas_srotg.$(SUFFIX) cblas_srotm.$(SUFFIX) cblas_srotmg.$(SUFFIX) \
	cblas_sscal.$(SUFFIX) cblas_sswap.$(SUFFIX) cblas_snrm2.$(SUFFIX) cblas_saxpby.$(SUFFIX)

CSBLAS2OBJS   = \
	cblas_sgemv.$(SUFFIX) cblas_sger.$(SUFFIX) cblas_ssymv.$(SUFFIX) cblas_strmv.$(SUFFIX) \
	cblas_strsv.$(SUFFIX) cblas_ssyr.$(SUFFIX) cblas_ssyr2.$(SUFFIX) cblas_sgbmv.$(SUFFIX) \
	cblas_ssbmv.$(SUFFIX) cblas_sspmv.$(SUFFIX) cblas_sspr.$(SUFFIX) cblas_sspr2.$(SUFFIX) \
	cblas_stbmv.$(SUFFIX) cblas_stbsv.$(SUFFIX) cblas_stpmv.$(SUFFIX) cblas_stpsv.$(SUFFIX)

CSBLAS3OBJS   = \
	cblas_sgemm.$(SUFFIX) cblas_ssymm.$(SUFFIX) cblas_strmm.$(SUFFIX) cblas_strsm.$(SUFFIX) \
	cblas_ssyrk.$(SUFFIX) cblas_ssyr2k.$(SUFFIX) cblas_somatcopy.$(SUFFIX)  cblas_simatcopy.$(SUFFIX)\
	cblas_sgeadd.$(SUFFIX)

CDBLAS1OBJS   = \
	cblas_idamax.$(SUFFIX) cblas_idamin.$(SUFFIX) cblas_dasum.$(SUFFIX) cblas_daxpy.$(SUFFIX) \
	cblas_dcopy.$(SUFFIX) cblas_ddot.$(SUFFIX) \
	cblas_drot.$(SUFFIX) cblas_drotg.$(SUFFIX) cblas_drotm.$(SUFFIX) cblas_drotmg.$(SUFFIX) \
	cblas_dscal.$(SUFFIX) cblas_dswap.$(SUFFIX) cblas_dnrm2.$(SUFFIX) cblas_daxpby.$(SUFFIX)

CDBLAS2OBJS   = \
	cblas_dgemv.$(SUFFIX) cblas_dger.$(SUFFIX) cblas_dsymv.$(SUFFIX) cblas_dtrmv.$(SUFFIX) \
	cblas_dtrsv.$(SUFFIX) cblas_dsyr.$(SUFFIX) cblas_dsyr2.$(SUFFIX) cblas_dgbmv.$(SUFFIX) \
	cblas_dsbmv.$(SUFFIX) cblas_dspmv.$(SUFFIX) cblas_dspr.$(SUFFIX) cblas_dspr2.$(SUFFIX) \
	cblas_dtbmv.$(SUFFIX) cblas_dtbsv.$(SUFFIX) cblas_dtpmv.$(SUFFIX) cblas_dtpsv.$(SUFFIX)

CDBLAS3OBJS   += \
	cblas_dgemm.$(SUFFIX) cblas_dsymm.$(SUFFIX) cblas_dtrmm.$(SUFFIX) cblas_dtrsm.$(SUFFIX) \
	cblas_dsyrk.$(SUFFIX) cblas_dsyr2k.$(SUFFIX) cblas_domatcopy.$(SUFFIX)  cblas_dimatcopy.$(SUFFIX) \
        cblas_dgeadd.$(SUFFIX) 

CCBLAS1OBJS   = \
	cblas_icamax.$(SUFFIX) cblas_icamin.$(SUFFIX) cblas_scasum.$(SUFFIX)  cblas_caxpy.$(SUFFIX) \
	cblas_ccopy.$(SUFFIX) \
	cblas_cdotc.$(SUFFIX) cblas_cdotu.$(SUFFIX) \
	cblas_cdotc_sub.$(SUFFIX) cblas_cdotu_sub.$(SUFFIX) \
	cblas_cscal.$(SUFFIX) cblas_csscal.$(SUFFIX) \
	cblas_cswap.$(SUFFIX) cblas_scnrm2.$(SUFFIX) \
	cblas_caxpby.$(SUFFIX)

CCBLAS2OBJS   = \
	cblas_cgemv.$(SUFFIX) cblas_cgerc.$(SUFFIX) cblas_cgeru.$(SUFFIX) \
	cblas_cgbmv.$(SUFFIX) cblas_chbmv.$(SUFFIX) cblas_chemv.$(SUFFIX) \
	cblas_cher.$(SUFFIX) cblas_cher2.$(SUFFIX) cblas_chpmv.$(SUFFIX) \
	cblas_chpr.$(SUFFIX) cblas_chpr2.$(SUFFIX) cblas_ctbmv.$(SUFFIX) \
	cblas_ctbsv.$(SUFFIX) cblas_ctpmv.$(SUFFIX) cblas_ctpsv.$(SUFFIX) \
	cblas_ctrmv.$(SUFFIX) cblas_ctrsv.$(SUFFIX)

CCBLAS3OBJS   = \
	cblas_cgemm.$(SUFFIX) cblas_csymm.$(SUFFIX) cblas_ctrmm.$(SUFFIX) cblas_ctrsm.$(SUFFIX) \
	cblas_csyrk.$(SUFFIX) cblas_csyr2k.$(SUFFIX) \
	cblas_chemm.$(SUFFIX) cblas_cherk.$(SUFFIX) cblas_cher2k.$(SUFFIX) \
	cblas_comatcopy.$(SUFFIX) cblas_cimatcopy.$(SUFFIX)\
	cblas_cgeadd.$(SUFFIX) cblas_xerbla.$(SUFFIX)



CZBLAS1OBJS   = \
	cblas_izamax.$(SUFFIX) cblas_izamin.$(SUFFIX) cblas_dzasum.$(SUFFIX)  cblas_zaxpy.$(SUFFIX) \
	cblas_zcopy.$(SUFFIX) \
	cblas_zdotc.$(SUFFIX) cblas_zdotu.$(SUFFIX) \
	cblas_zdotc_sub.$(SUFFIX) cblas_zdotu_sub.$(SUFFIX) \
	cblas_zscal.$(SUFFIX) cblas_zdscal.$(SUFFIX) \
	cblas_zswap.$(SUFFIX) cblas_dznrm2.$(SUFFIX) \
	cblas_zaxpby.$(SUFFIX)

CZBLAS2OBJS   = \
	cblas_zgemv.$(SUFFIX) cblas_zgerc.$(SUFFIX) cblas_zgeru.$(SUFFIX) \
	cblas_zgbmv.$(SUFFIX) cblas_zhbmv.$(SUFFIX) cblas_zhemv.$(SUFFIX) \
	cblas_zher.$(SUFFIX) cblas_zher2.$(SUFFIX) cblas_zhpmv.$(SUFFIX) \
	cblas_zhpr.$(SUFFIX) cblas_zhpr2.$(SUFFIX) cblas_ztbmv.$(SUFFIX) \
	cblas_ztbsv.$(SUFFIX) cblas_ztpmv.$(SUFFIX) cblas_ztpsv.$(SUFFIX) \
	cblas_ztrmv.$(SUFFIX) cblas_ztrsv.$(SUFFIX)

CZBLAS3OBJS   = \
	cblas_zgemm.$(SUFFIX) cblas_zsymm.$(SUFFIX) cblas_ztrmm.$(SUFFIX) cblas_ztrsm.$(SUFFIX) \
	cblas_zsyrk.$(SUFFIX) cblas_zsyr2k.$(SUFFIX) \
	cblas_zhemm.$(SUFFIX) cblas_zherk.$(SUFFIX) cblas_zher2k.$(SUFFIX)\
	cblas_zomatcopy.$(SUFFIX) cblas_zimatcopy.$(SUFFIX) \
	cblas_zgeadd.$(SUFFIX)


ifeq ($(SUPPORT_GEMM3M), 1)

# CBLAS3OBJS   +=  cgemm3m.$(SUFFIX) csymm3m.$(SUFFIX) chemm3m.$(SUFFIX)
CCBLAS3OBJS   +=  cblas_cgemm3m.$(SUFFIX)

# ZBLAS3OBJS   +=  zgemm3m.$(SUFFIX) zsymm3m.$(SUFFIX) zhemm3m.$(SUFFIX)
CZBLAS3OBJS   +=  cblas_zgemm3m.$(SUFFIX)

endif


ifndef NO_CBLAS

override CFLAGS += -I.

SBLAS1OBJS   += $(CSBLAS1OBJS)
SBLAS2OBJS   += $(CSBLAS2OBJS)
SBLAS3OBJS   += $(CSBLAS3OBJS)
DBLAS1OBJS   += $(CDBLAS1OBJS)
DBLAS2OBJS   += $(CDBLAS2OBJS)
DBLAS3OBJS   += $(CDBLAS3OBJS)
CBLAS1OBJS   += $(CCBLAS1OBJS)
CBLAS2OBJS   += $(CCBLAS2OBJS)
CBLAS3OBJS   += $(CCBLAS3OBJS)
ZBLAS1OBJS   += $(CZBLAS1OBJS)
ZBLAS2OBJS   += $(CZBLAS2OBJS)
ZBLAS3OBJS   += $(CZBLAS3OBJS)

endif

SBLASOBJS    = $(SBLAS1OBJS) $(SBLAS2OBJS) $(SBLAS3OBJS)
DBLASOBJS    = $(DBLAS1OBJS) $(DBLAS2OBJS) $(DBLAS3OBJS)
QBLASOBJS    = $(QBLAS1OBJS) $(QBLAS2OBJS) $(QBLAS3OBJS)
CBLASOBJS    = $(CBLAS1OBJS) $(CBLAS2OBJS) $(CBLAS3OBJS)
ZBLASOBJS    = $(ZBLAS1OBJS) $(ZBLAS2OBJS) $(ZBLAS3OBJS)
XBLASOBJS    = $(XBLAS1OBJS) $(XBLAS2OBJS) $(XBLAS3OBJS)

#SLAPACKOBJS	= \
#	sgetrf.$(SUFFIX) sgetrs.$(SUFFIX) spotrf.$(SUFFIX) sgetf2.$(SUFFIX) \
#	spotf2.$(SUFFIX) slaswp.$(SUFFIX) sgesv.$(SUFFIX) slauu2.$(SUFFIX)  \
#	slauum.$(SUFFIX) strti2.$(SUFFIX) strtri.$(SUFFIX) spotri.$(SUFFIX)

SLAPACKOBJS	= \
	sgetrf.$(SUFFIX) sgetrs.$(SUFFIX) spotrf.$(SUFFIX) sgetf2.$(SUFFIX) \
	spotf2.$(SUFFIX) slaswp.$(SUFFIX) sgesv.$(SUFFIX) slauu2.$(SUFFIX)  \
	slauum.$(SUFFIX) strti2.$(SUFFIX) strtri.$(SUFFIX)


#DLAPACKOBJS	= \
#	dgetrf.$(SUFFIX) dgetrs.$(SUFFIX) dpotrf.$(SUFFIX) dgetf2.$(SUFFIX) \
#	dpotf2.$(SUFFIX) dlaswp.$(SUFFIX) dgesv.$(SUFFIX) dlauu2.$(SUFFIX)  \
#	dlauum.$(SUFFIX) dtrti2.$(SUFFIX) dtrtri.$(SUFFIX) dpotri.$(SUFFIX)

DLAPACKOBJS	= \
	dgetrf.$(SUFFIX) dgetrs.$(SUFFIX) dpotrf.$(SUFFIX) dgetf2.$(SUFFIX) \
	dpotf2.$(SUFFIX) dlaswp.$(SUFFIX) dgesv.$(SUFFIX) dlauu2.$(SUFFIX)  \
	dlauum.$(SUFFIX) dtrti2.$(SUFFIX) dtrtri.$(SUFFIX)


QLAPACKOBJS	= \
	qgetf2.$(SUFFIX) qgetrf.$(SUFFIX) qlauu2.$(SUFFIX) qlauum.$(SUFFIX) \
	qpotf2.$(SUFFIX) qpotrf.$(SUFFIX) qtrti2.$(SUFFIX) qtrtri.$(SUFFIX) \
	qlaswp.$(SUFFIX) qgetrs.$(SUFFIX) qgesv.$(SUFFIX)  qpotri.$(SUFFIX) \


#CLAPACKOBJS	= \
#	cgetrf.$(SUFFIX) cgetrs.$(SUFFIX) cpotrf.$(SUFFIX) cgetf2.$(SUFFIX) \
#	cpotf2.$(SUFFIX) claswp.$(SUFFIX) cgesv.$(SUFFIX) clauu2.$(SUFFIX) \
#	clauum.$(SUFFIX) ctrti2.$(SUFFIX) ctrtri.$(SUFFIX) cpotri.$(SUFFIX)


CLAPACKOBJS	= \
	cgetrf.$(SUFFIX) cgetrs.$(SUFFIX) cpotrf.$(SUFFIX) cgetf2.$(SUFFIX) \
	cpotf2.$(SUFFIX) claswp.$(SUFFIX) cgesv.$(SUFFIX) clauu2.$(SUFFIX) \
	clauum.$(SUFFIX) ctrti2.$(SUFFIX) ctrtri.$(SUFFIX) 


#ZLAPACKOBJS	= \
#	zgetrf.$(SUFFIX) zgetrs.$(SUFFIX) zpotrf.$(SUFFIX) zgetf2.$(SUFFIX) \
#	zpotf2.$(SUFFIX) zlaswp.$(SUFFIX) zgesv.$(SUFFIX)  zlauu2.$(SUFFIX) \
#	zlauum.$(SUFFIX) ztrti2.$(SUFFIX) ztrtri.$(SUFFIX) zpotri.$(SUFFIX)


ZLAPACKOBJS	= \
	zgetrf.$(SUFFIX) zgetrs.$(SUFFIX) zpotrf.$(SUFFIX) zgetf2.$(SUFFIX) \
	zpotf2.$(SUFFIX) zlaswp.$(SUFFIX) zgesv.$(SUFFIX)  zlauu2.$(SUFFIX) \
	zlauum.$(SUFFIX) ztrti2.$(SUFFIX) ztrtri.$(SUFFIX) 


XLAPACKOBJS	= \
	xgetf2.$(SUFFIX) xgetrf.$(SUFFIX) xlauu2.$(SUFFIX) xlauum.$(SUFFIX) \
	xpotf2.$(SUFFIX) xpotrf.$(SUFFIX) xtrti2.$(SUFFIX) xtrtri.$(SUFFIX) \
	xlaswp.$(SUFFIX) xgetrs.$(SUFFIX) xgesv.$(SUFFIX)  xpotri.$(SUFFIX) \

ifneq ($(NO_LAPACK), 1)
SBLASOBJS += $(SLAPACKOBJS)
DBLASOBJS += $(DLAPACKOBJS)
#QBLASOBJS += $(QLAPACKOBJS)
CBLASOBJS += $(CLAPACKOBJS)
ZBLASOBJS += $(ZLAPACKOBJS)
#XBLASOBJS += $(XLAPACKOBJS)

endif

FUNCOBJS    = $(SBLASOBJS) $(DBLASOBJS) $(CBLASOBJS) $(ZBLASOBJS)

ifdef EXPRECISION
FUNCOBJS   += $(QBLASOBJS) $(XBLASOBJS)
endif

ifdef QUAD_PRECISION
FUNCOBJS   += $(QBLASOBJS) $(XBLASOBJS)
endif

FUNCALLFILES = $(FUNCOBJS:.$(SUFFIX)=)

include $(TOPDIR)/Makefile.tail

all :: libs

ifdef FUNCTION_PROFILE
$(BLASOBJS) $(BLASOBJS_P) : functable.h
$(BLASOBJS) $(BLASOBJS_P) : override CFLAGS += -DPROFILE_FUNC_NAME=interface_$(*F)

functable.h : Makefile
	./create $(FUNCALLFILES) > functable.h

endif

clean ::
	@rm -f functable.h

level1 : $(SBLAS1OBJS) $(DBLAS1OBJS) $(QBLAS1OBJS) $(CBLAS1OBJS) $(ZBLAS1OBJS) $(XBLAS1OBJS)
	$(AR) $(ARFLAGS) -ru $(TOPDIR)/$(LIBNAME) $^

level2 : $(SBLAS2OBJS) $(DBLAS2OBJS) $(QBLAS2OBJS) $(CBLAS2OBJS) $(ZBLAS2OBJS) $(XBLAS2OBJS)
	$(AR) $(ARFLAGS) -ru $(TOPDIR)/$(LIBNAME) $^

level3 : $(SBLAS3OBJS) $(DBLAS3OBJS) $(QBLAS3OBJS) $(CBLAS3OBJS) $(ZBLAS3OBJS) $(XBLAS3OBJS)
	$(AR) $(ARFLAGS) -ru $(TOPDIR)/$(LIBNAME) $^

$(CSBLASOBJS) $(CSBLASOBJS_P) $(CDBLASOBJS) $(CDBLASOBJS_P) $(CQBLASOBJS) $(CQBLASOBJS_P) \
$(CCBLASOBJS) $(CCBLASOBJS_P) $(CZBLASOBJS) $(CZBLASOBJS_P) $(CXBLASOBJS) $(CXBLASOBJS_P) : override CFLAGS += -DCBLAS

srot.$(SUFFIX) srot.$(PSUFFIX) : rot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

drot.$(SUFFIX) drot.$(PSUFFIX) : rot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qrot.$(SUFFIX) qrot.$(PSUFFIX) : rot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

csrot.$(SUFFIX) csrot.$(PSUFFIX) : zrot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

zdrot.$(SUFFIX) zdrot.$(PSUFFIX) : zrot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

xqrot.$(SUFFIX) xqrot.$(PSUFFIX) : zrot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

srotm.$(SUFFIX) srotm.$(PSUFFIX): rotm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

drotm.$(SUFFIX) drotm.$(PSUFFIX): rotm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qrotm.$(SUFFIX) qrotm.$(PSUFFIX): rotm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

srotmg.$(SUFFIX) srotmg.$(PSUFFIX): rotmg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

drotmg.$(SUFFIX) drotmg.$(PSUFFIX): rotmg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qrotmg.$(SUFFIX) qrotmg.$(PSUFFIX): rotmg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

srotg.$(SUFFIX) srotg.$(PSUFFIX): rotg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

drotg.$(SUFFIX) drotg.$(PSUFFIX): rotg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qrotg.$(SUFFIX) qrotg.$(PSUFFIX): rotg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

crotg.$(SUFFIX) crotg.$(PSUFFIX): zrotg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zrotg.$(SUFFIX) zrotg.$(PSUFFIX): zrotg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xrotg.$(SUFFIX) xrotg.$(PSUFFIX): zrotg.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sasum.$(SUFFIX) sasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dasum.$(SUFFIX) dasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qasum.$(SUFFIX) qasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

scasum.$(SUFFIX) scasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dzasum.$(SUFFIX) dzasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qxasum.$(SUFFIX) qxasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

snrm2.$(SUFFIX) snrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dnrm2.$(SUFFIX) dnrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qnrm2.$(SUFFIX) qnrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

scnrm2.$(SUFFIX) scnrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dznrm2.$(SUFFIX) dznrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qxnrm2.$(SUFFIX) qxnrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

samax.$(SUFFIX) samax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

damax.$(SUFFIX) damax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

qamax.$(SUFFIX) qamax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

scamax.$(SUFFIX) scamax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

dzamax.$(SUFFIX) dzamax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

qxamax.$(SUFFIX) qxamax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

samin.$(SUFFIX) samin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

damin.$(SUFFIX) damin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

qamin.$(SUFFIX) qamin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

scamin.$(SUFFIX) scamin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

dzamin.$(SUFFIX) dzamin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

qxamin.$(SUFFIX) qxamin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

smax.$(SUFFIX) smax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

dmax.$(SUFFIX) dmax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

qmax.$(SUFFIX) qmax.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

smin.$(SUFFIX) smin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

dmin.$(SUFFIX) dmin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

qmin.$(SUFFIX) qmin.$(PSUFFIX) : max.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

isamax.$(SUFFIX) isamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

idamax.$(SUFFIX) idamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

iqamax.$(SUFFIX) iqamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

icamax.$(SUFFIX) icamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

izamax.$(SUFFIX) izamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

ixamax.$(SUFFIX) ixamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

isamin.$(SUFFIX) isamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

idamin.$(SUFFIX) idamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

iqamin.$(SUFFIX) iqamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

icamin.$(SUFFIX) icamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

izamin.$(SUFFIX) izamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

ixamin.$(SUFFIX) ixamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

ismax.$(SUFFIX) ismax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

idmax.$(SUFFIX) idmax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

iqmax.$(SUFFIX) iqmax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

ismin.$(SUFFIX) ismin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

idmin.$(SUFFIX) idmin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

iqmin.$(SUFFIX) iqmin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

sdsdot.$(SUFFIX) sdsdot.$(PSUFFIX) : sdsdot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dsdot.$(SUFFIX) dsdot.$(PSUFFIX) : dsdot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

sdot.$(SUFFIX) sdot.$(PSUFFIX) : dot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

ddot.$(SUFFIX) ddot.$(PSUFFIX) : dot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qdot.$(SUFFIX) qdot.$(PSUFFIX) : dot.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cdotu.$(SUFFIX) cdotu.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -UCONJ $< -o $(@F)

cdotc.$(SUFFIX) cdotc.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -DCONJ $< -o $(@F)

zdotu.$(SUFFIX) zdotu.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -UCONJ $< -o $(@F)

zdotc.$(SUFFIX) zdotc.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -DCONJ $< -o $(@F)

xdotu.$(SUFFIX) xdotu.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -UCONJ $< -o $(@F)

xdotc.$(SUFFIX) xdotc.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -c -DCONJ $< -o $(@F)

saxpy.$(SUFFIX) saxpy.$(PSUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

daxpy.$(SUFFIX) daxpy.$(PSUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qaxpy.$(SUFFIX) qaxpy.$(PSUFFIX) : axpy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

caxpy.$(SUFFIX) caxpy.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

zaxpy.$(SUFFIX) zaxpy.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

xaxpy.$(SUFFIX) xaxpy.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

caxpyc.$(SUFFIX) caxpyc.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -c -DCONJ $< -o $(@F)

zaxpyc.$(SUFFIX) zaxpyc.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -c -DCONJ $< -o $(@F)

xaxpyc.$(SUFFIX) xaxpyc.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -c -DCONJ $< -o $(@F)

sscal.$(SUFFIX) sscal.$(PSUFFIX) : scal.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dscal.$(SUFFIX) dscal.$(PSUFFIX) : scal.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qscal.$(SUFFIX) qscal.$(PSUFFIX) : scal.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cscal.$(SUFFIX) cscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

zscal.$(SUFFIX) zscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

xscal.$(SUFFIX) xscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

csscal.$(SUFFIX) csscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -c -DSSCAL $< -o $(@F)

zdscal.$(SUFFIX) zdscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -c -DSSCAL $< -o $(@F)

xqscal.$(SUFFIX) xqscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -c -DSSCAL $< -o $(@F)

scopy.$(SUFFIX) scopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dcopy.$(SUFFIX) dcopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qcopy.$(SUFFIX) qcopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

ccopy.$(SUFFIX) ccopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

zcopy.$(SUFFIX) zcopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

xcopy.$(SUFFIX) xcopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

sswap.$(SUFFIX) sswap.$(PSUFFIX) : swap.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

dswap.$(SUFFIX) dswap.$(PSUFFIX) : swap.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

qswap.$(SUFFIX) qswap.$(PSUFFIX) : swap.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cswap.$(SUFFIX) cswap.$(PSUFFIX) : zswap.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

zswap.$(SUFFIX) zswap.$(PSUFFIX) : zswap.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

xswap.$(SUFFIX) xswap.$(PSUFFIX) : zswap.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

sger.$(SUFFIX) sger.$(PSUFFIX) : ger.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dger.$(SUFFIX) dger.$(PSUFFIX) : ger.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qger.$(SUFFIX) qger.$(PSUFFIX) : ger.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgeru.$(SUFFIX) cgeru.$(PSUFFIX) : zger.c
	$(CC) -c $(CFLAGS) -UCONJ $< -o $(@F)

cgerc.$(SUFFIX) cgerc.$(PSUFFIX) : zger.c
	$(CC) -c $(CFLAGS) -DCONJ $< -o $(@F)

zgeru.$(SUFFIX) zgeru.$(PSUFFIX) : zger.c
	$(CC) -c $(CFLAGS) -UCONJ $< -o $(@F)

zgerc.$(SUFFIX) zgerc.$(PSUFFIX) : zger.c
	$(CC) -c $(CFLAGS) -DCONJ $< -o $(@F)

xgeru.$(SUFFIX) xgeru.$(PSUFFIX) : zger.c
	$(CC) -c $(CFLAGS) -UCONJ $< -o $(@F)

xgerc.$(SUFFIX) xgerc.$(PSUFFIX) : zger.c
	$(CC) -c $(CFLAGS) -DCONJ $< -o $(@F)

ifndef USE_NETLIB_GEMV
sgemv.$(SUFFIX) sgemv.$(PSUFFIX): gemv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

dgemv.$(SUFFIX) dgemv.$(PSUFFIX): gemv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<
else
sgemv.$(SUFFIX) sgemv.$(PSUFFIX): netlib/sgemv.f
	$(FC) -c $(FFLAGS) -o $(@F) $<

dgemv.$(SUFFIX) dgemv.$(PSUFFIX): netlib/dgemv.f
	$(FC) -c $(FFLAGS) -o $(@F) $<
endif

qgemv.$(SUFFIX) qgemv.$(PSUFFIX): gemv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

ifndef USE_NETLIB_GEMV
cgemv.$(SUFFIX) cgemv.$(PSUFFIX): zgemv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

zgemv.$(SUFFIX) zgemv.$(PSUFFIX): zgemv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<
else
cgemv.$(SUFFIX) cgemv.$(PSUFFIX): netlib/cgemv.f
	$(FC) -c $(FFLAGS) -o $(@F) $<

zgemv.$(SUFFIX) zgemv.$(PSUFFIX): netlib/zgemv.f
	$(FC) -c $(FFLAGS) -o $(@F) $<
endif

xgemv.$(SUFFIX) xgemv.$(PSUFFIX): zgemv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

strsv.$(SUFFIX) strsv.$(PSUFFIX) : trsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtrsv.$(SUFFIX) dtrsv.$(PSUFFIX) : trsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtrsv.$(SUFFIX) qtrsv.$(PSUFFIX) : trsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctrsv.$(SUFFIX) ctrsv.$(PSUFFIX) : ztrsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztrsv.$(SUFFIX) ztrsv.$(PSUFFIX) : ztrsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtrsv.$(SUFFIX) xtrsv.$(PSUFFIX) : ztrsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

strmv.$(SUFFIX) strmv.$(PSUFFIX) : trmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtrmv.$(SUFFIX) dtrmv.$(PSUFFIX) : trmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtrmv.$(SUFFIX) qtrmv.$(PSUFFIX) : trmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctrmv.$(SUFFIX) ctrmv.$(PSUFFIX) : ztrmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztrmv.$(SUFFIX) ztrmv.$(PSUFFIX) : ztrmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtrmv.$(SUFFIX) xtrmv.$(PSUFFIX) : ztrmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssymv.$(SUFFIX) ssymv.$(PSUFFIX) : symv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsymv.$(SUFFIX) dsymv.$(PSUFFIX) : symv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsymv.$(SUFFIX) qsymv.$(PSUFFIX) : symv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csymv.$(SUFFIX) csymv.$(PSUFFIX) : zsymv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsymv.$(SUFFIX) zsymv.$(PSUFFIX) : zsymv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsymv.$(SUFFIX) xsymv.$(PSUFFIX) : zsymv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssyr.$(SUFFIX) ssyr.$(PSUFFIX) : syr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsyr.$(SUFFIX) dsyr.$(PSUFFIX) : syr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsyr.$(SUFFIX) qsyr.$(PSUFFIX) : syr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csyr.$(SUFFIX) csyr.$(PSUFFIX) : zsyr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsyr.$(SUFFIX) zsyr.$(PSUFFIX) : zsyr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsyr.$(SUFFIX) xsyr.$(PSUFFIX) : zsyr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssyr2.$(SUFFIX) ssyr2.$(PSUFFIX) : syr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsyr2.$(SUFFIX) dsyr2.$(PSUFFIX) : syr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsyr2.$(SUFFIX) qsyr2.$(PSUFFIX) : syr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csyr2.$(SUFFIX) csyr2.$(PSUFFIX) : zsyr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsyr2.$(SUFFIX) zsyr2.$(PSUFFIX) : zsyr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsyr2.$(SUFFIX) xsyr2.$(PSUFFIX) : zsyr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sgbmv.$(SUFFIX) sgbmv.$(PSUFFIX): gbmv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

dgbmv.$(SUFFIX) dgbmv.$(PSUFFIX): gbmv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

qgbmv.$(SUFFIX) qgbmv.$(PSUFFIX): gbmv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

cgbmv.$(SUFFIX) cgbmv.$(PSUFFIX): zgbmv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

zgbmv.$(SUFFIX) zgbmv.$(PSUFFIX): zgbmv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

xgbmv.$(SUFFIX) xgbmv.$(PSUFFIX): zgbmv.c
	$(CC) -c $(CFLAGS) -o $(@F) $<

ssbmv.$(SUFFIX) ssbmv.$(PSUFFIX) : sbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsbmv.$(SUFFIX) dsbmv.$(PSUFFIX) : sbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsbmv.$(SUFFIX) qsbmv.$(PSUFFIX) : sbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csbmv.$(SUFFIX) csbmv.$(PSUFFIX) : zsbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsbmv.$(SUFFIX) zsbmv.$(PSUFFIX) : zsbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsbmv.$(SUFFIX) xsbmv.$(PSUFFIX) : zsbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sspmv.$(SUFFIX) sspmv.$(PSUFFIX) : spmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dspmv.$(SUFFIX) dspmv.$(PSUFFIX) : spmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qspmv.$(SUFFIX) qspmv.$(PSUFFIX) : spmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cspmv.$(SUFFIX) cspmv.$(PSUFFIX) : zspmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zspmv.$(SUFFIX) zspmv.$(PSUFFIX) : zspmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xspmv.$(SUFFIX) xspmv.$(PSUFFIX) : zspmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sspr.$(SUFFIX) sspr.$(PSUFFIX) : spr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dspr.$(SUFFIX) dspr.$(PSUFFIX) : spr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qspr.$(SUFFIX) qspr.$(PSUFFIX) : spr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cspr.$(SUFFIX) cspr.$(PSUFFIX) : zspr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zspr.$(SUFFIX) zspr.$(PSUFFIX) : zspr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xspr.$(SUFFIX) xspr.$(PSUFFIX) : zspr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sspr2.$(SUFFIX) sspr2.$(PSUFFIX) : spr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dspr2.$(SUFFIX) dspr2.$(PSUFFIX) : spr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qspr2.$(SUFFIX) qspr2.$(PSUFFIX) : spr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cspr2.$(SUFFIX) cspr2.$(PSUFFIX) : zspr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zspr2.$(SUFFIX) zspr2.$(PSUFFIX) : zspr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xspr2.$(SUFFIX) xspr2.$(PSUFFIX) : zspr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

stbmv.$(SUFFIX) stbmv.$(PSUFFIX) : tbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtbmv.$(SUFFIX) dtbmv.$(PSUFFIX) : tbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtbmv.$(SUFFIX) qtbmv.$(PSUFFIX) : tbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctbmv.$(SUFFIX) ctbmv.$(PSUFFIX) : ztbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztbmv.$(SUFFIX) ztbmv.$(PSUFFIX) : ztbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtbmv.$(SUFFIX) xtbmv.$(PSUFFIX) : ztbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

stbsv.$(SUFFIX) stbsv.$(PSUFFIX) : tbsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtbsv.$(SUFFIX) dtbsv.$(PSUFFIX) : tbsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtbsv.$(SUFFIX) qtbsv.$(PSUFFIX) : tbsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctbsv.$(SUFFIX) ctbsv.$(PSUFFIX) : ztbsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztbsv.$(SUFFIX) ztbsv.$(PSUFFIX) : ztbsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtbsv.$(SUFFIX) xtbsv.$(PSUFFIX) : ztbsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

stpsv.$(SUFFIX) stpsv.$(PSUFFIX) : tpsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtpsv.$(SUFFIX) dtpsv.$(PSUFFIX) : tpsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtpsv.$(SUFFIX) qtpsv.$(PSUFFIX) : tpsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctpsv.$(SUFFIX) ctpsv.$(PSUFFIX) : ztpsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztpsv.$(SUFFIX) ztpsv.$(PSUFFIX) : ztpsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtpsv.$(SUFFIX) xtpsv.$(PSUFFIX) : ztpsv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

stpmv.$(SUFFIX) stpmv.$(PSUFFIX) : tpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtpmv.$(SUFFIX) dtpmv.$(PSUFFIX) : tpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtpmv.$(SUFFIX) qtpmv.$(PSUFFIX) : tpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctpmv.$(SUFFIX) ctpmv.$(PSUFFIX) : ztpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztpmv.$(SUFFIX) ztpmv.$(PSUFFIX) : ztpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtpmv.$(SUFFIX) xtpmv.$(PSUFFIX) : ztpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

chemv.$(SUFFIX) chemv.$(PSUFFIX) : zhemv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zhemv.$(SUFFIX) zhemv.$(PSUFFIX) : zhemv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xhemv.$(SUFFIX) xhemv.$(PSUFFIX) : zhemv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

chbmv.$(SUFFIX) chbmv.$(PSUFFIX) : zhbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zhbmv.$(SUFFIX) zhbmv.$(PSUFFIX) : zhbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xhbmv.$(SUFFIX) xhbmv.$(PSUFFIX) : zhbmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cher.$(SUFFIX) cher.$(PSUFFIX) : zher.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zher.$(SUFFIX) zher.$(PSUFFIX) : zher.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xher.$(SUFFIX) xher.$(PSUFFIX) : zher.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cher2.$(SUFFIX) cher2.$(PSUFFIX) : zher2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zher2.$(SUFFIX) zher2.$(PSUFFIX) : zher2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xher2.$(SUFFIX) xher2.$(PSUFFIX) : zher2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

chpmv.$(SUFFIX) chpmv.$(PSUFFIX) : zhpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zhpmv.$(SUFFIX) zhpmv.$(PSUFFIX) : zhpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xhpmv.$(SUFFIX) xhpmv.$(PSUFFIX) : zhpmv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

chpr.$(SUFFIX) chpr.$(PSUFFIX) : zhpr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zhpr.$(SUFFIX) zhpr.$(PSUFFIX) : zhpr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xhpr.$(SUFFIX) xhpr.$(PSUFFIX) : zhpr.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

chpr2.$(SUFFIX) chpr2.$(PSUFFIX) : zhpr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zhpr2.$(SUFFIX) zhpr2.$(PSUFFIX) : zhpr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xhpr2.$(SUFFIX) xhpr2.$(PSUFFIX) : zhpr2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sgemm.$(SUFFIX) sgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

dgemm.$(SUFFIX) dgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

qgemm.$(SUFFIX) qgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgemm.$(SUFFIX) cgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

zgemm.$(SUFFIX) zgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

xgemm.$(SUFFIX) xgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssymm.$(SUFFIX) ssymm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsymm.$(SUFFIX) dsymm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsymm.$(SUFFIX) qsymm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csymm.$(SUFFIX) csymm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsymm.$(SUFFIX) zsymm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsymm.$(SUFFIX) xsymm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

strmm.$(SUFFIX) strmm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) -DTRMM $< -o $(@F)

dtrmm.$(SUFFIX) dtrmm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) -DTRMM $< -o $(@F)

qtrmm.$(SUFFIX) qtrmm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) -DTRMM $< -o $(@F)

ctrmm.$(SUFFIX) ctrmm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) -DTRMM $< -o $(@F)

ztrmm.$(SUFFIX) ztrmm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) -DTRMM $< -o $(@F)

xtrmm.$(SUFFIX) xtrmm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) -DTRMM $< -o $(@F)

strsm.$(SUFFIX) strsm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtrsm.$(SUFFIX) dtrsm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtrsm.$(SUFFIX) qtrsm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctrsm.$(SUFFIX) ctrsm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztrsm.$(SUFFIX) ztrsm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtrsm.$(SUFFIX) xtrsm.$(PSUFFIX) : trsm.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssyrk.$(SUFFIX) ssyrk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsyrk.$(SUFFIX) dsyrk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsyrk.$(SUFFIX) qsyrk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csyrk.$(SUFFIX) csyrk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsyrk.$(SUFFIX) zsyrk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsyrk.$(SUFFIX) xsyrk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssyr2k.$(SUFFIX) ssyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dsyr2k.$(SUFFIX) dsyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qsyr2k.$(SUFFIX) qsyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

csyr2k.$(SUFFIX) csyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zsyr2k.$(SUFFIX) zsyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xsyr2k.$(SUFFIX) xsyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

chemm.$(SUFFIX) chemm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

zhemm.$(SUFFIX) zhemm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

xhemm.$(SUFFIX) xhemm.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

cherk.$(SUFFIX) cherk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

zherk.$(SUFFIX) zherk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

xherk.$(SUFFIX) xherk.$(PSUFFIX) : syrk.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

cher2k.$(SUFFIX) cher2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

zher2k.$(SUFFIX) zher2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

xher2k.$(SUFFIX) xher2k.$(PSUFFIX) : syr2k.c
	$(CC) -c $(CFLAGS) -DHEMM $< -o $(@F)

cgemm3m.$(SUFFIX) cgemm3m.$(PSUFFIX) : gemm.c
	$(CC) -c $(CFLAGS) -DGEMM3M $< -o $(@F)

zgemm3m.$(SUFFIX) zgemm3m.$(PSUFFIX) : gemm.c
	$(CC) -c $(CFLAGS) -DGEMM3M $< -o $(@F)

xgemm3m.$(SUFFIX) xgemm3m.$(PSUFFIX) : gemm.c
	$(CC) -c $(CFLAGS) -DGEMM3M $< -o $(@F)

csymm3m.$(SUFFIX) csymm3m.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DGEMM3M $< -o $(@F)

zsymm3m.$(SUFFIX) zsymm3m.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DGEMM3M $< -o $(@F)

xsymm3m.$(SUFFIX) xsymm3m.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DGEMM3M $< -o $(@F)

chemm3m.$(SUFFIX) chemm3m.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DGEMM3M -DHEMM $< -o $(@F)

zhemm3m.$(SUFFIX) zhemm3m.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DGEMM3M -DHEMM $< -o $(@F)

xhemm3m.$(SUFFIX) xhemm3m.$(PSUFFIX) : symm.c
	$(CC) -c $(CFLAGS) -DGEMM3M -DHEMM $< -o $(@F)

cblas_isamax.$(SUFFIX) cblas_isamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

cblas_idamax.$(SUFFIX) cblas_idamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

cblas_icamax.$(SUFFIX) cblas_icamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

cblas_izamax.$(SUFFIX) cblas_izamax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -UUSE_MIN $< -o $(@F)

cblas_isamin.$(SUFFIX) cblas_isamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

cblas_idamin.$(SUFFIX) cblas_idamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

cblas_icamin.$(SUFFIX) cblas_icamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

cblas_izamin.$(SUFFIX) cblas_izamin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -DUSE_ABS -DUSE_MIN $< -o $(@F)

cblas_ismax.$(SUFFIX) cblas_ismax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

cblas_idmax.$(SUFFIX) cblas_idmax.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -UUSE_ABS -UUSE_MIN $< -o $(@F)

cblas_ismin.$(SUFFIX) cblas_ismin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

cblas_idmin.$(SUFFIX) cblas_idmin.$(PSUFFIX) : imax.c
	$(CC) $(CFLAGS) -DCBLAS -c -UUSE_ABS -DUSE_MIN $< -o $(@F)

cblas_sasum.$(SUFFIX) cblas_sasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dasum.$(SUFFIX) cblas_dasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_scasum.$(SUFFIX) cblas_scasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dzasum.$(SUFFIX) cblas_dzasum.$(PSUFFIX) : asum.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_sdsdot.$(SUFFIX) cblas_sdsdot.$(PSUFFIX) : sdsdot.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dsdot.$(SUFFIX) cblas_dsdot.$(PSUFFIX) : dsdot.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_sdot.$(SUFFIX) cblas_sdot.$(PSUFFIX) : dot.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_ddot.$(SUFFIX) cblas_ddot.$(PSUFFIX) : dot.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_cdotu.$(SUFFIX) cblas_cdotu.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -c -UCONJ $< -o $(@F)

cblas_cdotc.$(SUFFIX) cblas_cdotc.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -c -DCONJ $< -o $(@F)

cblas_zdotu.$(SUFFIX) cblas_zdotu.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -c -UCONJ $< -o $(@F)

cblas_zdotc.$(SUFFIX) cblas_zdotc.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -c -DCONJ $< -o $(@F)

cblas_cdotu_sub.$(SUFFIX) cblas_cdotu_sub.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -DFORCE_USE_STACK -c -UCONJ $< -o $(@F)

cblas_cdotc_sub.$(SUFFIX) cblas_cdotc_sub.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -DFORCE_USE_STACK -c -DCONJ $< -o $(@F)

cblas_zdotu_sub.$(SUFFIX) cblas_zdotu_sub.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -DFORCE_USE_STACK -c -UCONJ $< -o $(@F)

cblas_zdotc_sub.$(SUFFIX) cblas_zdotc_sub.$(PSUFFIX) : zdot.c
	$(CC) $(CFLAGS) -DCBLAS -DFORCE_USE_STACK -c -DCONJ $< -o $(@F)

cblas_snrm2.$(SUFFIX) cblas_snrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dnrm2.$(SUFFIX) cblas_dnrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_scnrm2.$(SUFFIX) cblas_scnrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dznrm2.$(SUFFIX) cblas_dznrm2.$(PSUFFIX) : nrm2.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_saxpy.$(SUFFIX) cblas_saxpy.$(PSUFFIX) : axpy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_daxpy.$(SUFFIX) cblas_daxpy.$(PSUFFIX) : axpy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_caxpy.$(SUFFIX) cblas_caxpy.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_zaxpy.$(SUFFIX) cblas_zaxpy.$(PSUFFIX) : zaxpy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_scopy.$(SUFFIX) cblas_scopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dcopy.$(SUFFIX) cblas_dcopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_ccopy.$(SUFFIX) cblas_ccopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_zcopy.$(SUFFIX) cblas_zcopy.$(PSUFFIX) : copy.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_sswap.$(SUFFIX) cblas_sswap.$(PSUFFIX) : swap.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dswap.$(SUFFIX) cblas_dswap.$(PSUFFIX) : swap.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_cswap.$(SUFFIX) cblas_cswap.$(PSUFFIX) : zswap.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_zswap.$(SUFFIX) cblas_zswap.$(PSUFFIX) : zswap.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_srot.$(SUFFIX) cblas_srot.$(PSUFFIX) : rot.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_drot.$(SUFFIX) cblas_drot.$(PSUFFIX) : rot.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_srotg.$(SUFFIX) cblas_srotg.$(PSUFFIX): rotg.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_drotg.$(SUFFIX) cblas_drotg.$(PSUFFIX): rotg.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_srotm.$(SUFFIX) cblas_srotm.$(PSUFFIX): rotm.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_drotm.$(SUFFIX) cblas_drotm.$(PSUFFIX): rotm.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_srotmg.$(SUFFIX) cblas_srotmg.$(PSUFFIX): rotmg.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_drotmg.$(SUFFIX) cblas_drotmg.$(PSUFFIX): rotmg.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_sscal.$(SUFFIX) cblas_sscal.$(PSUFFIX) : scal.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_dscal.$(SUFFIX) cblas_dscal.$(PSUFFIX) : scal.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_cscal.$(SUFFIX) cblas_cscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_zscal.$(SUFFIX) cblas_zscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

cblas_csscal.$(SUFFIX) cblas_csscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -DCBLAS -c -DSSCAL $< -o $(@F)

cblas_zdscal.$(SUFFIX) cblas_zdscal.$(PSUFFIX) : zscal.c
	$(CC) $(CFLAGS) -DCBLAS -c -DSSCAL $< -o $(@F)

cblas_sgemv.$(SUFFIX) cblas_sgemv.$(PSUFFIX): gemv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_dgemv.$(SUFFIX) cblas_dgemv.$(PSUFFIX): gemv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_cgemv.$(SUFFIX) cblas_cgemv.$(PSUFFIX): zgemv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_zgemv.$(SUFFIX) cblas_zgemv.$(PSUFFIX): zgemv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_sger.$(SUFFIX) cblas_sger.$(PSUFFIX) : ger.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dger.$(SUFFIX) cblas_dger.$(PSUFFIX) : ger.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_cgeru.$(SUFFIX) cblas_cgeru.$(PSUFFIX) : zger.c
	$(CC) -DCBLAS -c $(CFLAGS) -UCONJ $< -o $(@F)

cblas_cgerc.$(SUFFIX) cblas_cgerc.$(PSUFFIX) : zger.c
	$(CC) -DCBLAS -c $(CFLAGS) -DCONJ $< -o $(@F)

cblas_zgeru.$(SUFFIX) cblas_zgeru.$(PSUFFIX) : zger.c
	$(CC) -DCBLAS -c $(CFLAGS) -UCONJ $< -o $(@F)

cblas_zgerc.$(SUFFIX) cblas_zgerc.$(PSUFFIX) : zger.c
	$(CC) -DCBLAS -c $(CFLAGS) -DCONJ $< -o $(@F)

cblas_strsv.$(SUFFIX) cblas_strsv.$(PSUFFIX) : trsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtrsv.$(SUFFIX) cblas_dtrsv.$(PSUFFIX) : trsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctrsv.$(SUFFIX) cblas_ctrsv.$(PSUFFIX) : ztrsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztrsv.$(SUFFIX) cblas_ztrsv.$(PSUFFIX) : ztrsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_strmv.$(SUFFIX) cblas_strmv.$(PSUFFIX) : trmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtrmv.$(SUFFIX) cblas_dtrmv.$(PSUFFIX) : trmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctrmv.$(SUFFIX) cblas_ctrmv.$(PSUFFIX) : ztrmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztrmv.$(SUFFIX) cblas_ztrmv.$(PSUFFIX) : ztrmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ssyr.$(SUFFIX) cblas_ssyr.$(PSUFFIX) : syr.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsyr.$(SUFFIX) cblas_dsyr.$(PSUFFIX) : syr.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_cher.$(SUFFIX) cblas_cher.$(PSUFFIX) : zher.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zher.$(SUFFIX) cblas_zher.$(PSUFFIX) : zher.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ssyr2.$(SUFFIX) cblas_ssyr2.$(PSUFFIX) : syr2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsyr2.$(SUFFIX) cblas_dsyr2.$(PSUFFIX) : syr2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_cher2.$(SUFFIX) cblas_cher2.$(PSUFFIX) : zher2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zher2.$(SUFFIX) cblas_zher2.$(PSUFFIX) : zher2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_sgbmv.$(SUFFIX) cblas_sgbmv.$(PSUFFIX): gbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_dgbmv.$(SUFFIX) cblas_dgbmv.$(PSUFFIX): gbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_cgbmv.$(SUFFIX) cblas_cgbmv.$(PSUFFIX): zgbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_zgbmv.$(SUFFIX) cblas_zgbmv.$(PSUFFIX): zgbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) -o $(@F) $<

cblas_ssbmv.$(SUFFIX) cblas_ssbmv.$(PSUFFIX) : sbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsbmv.$(SUFFIX) cblas_dsbmv.$(PSUFFIX) : sbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_chbmv.$(SUFFIX) cblas_chbmv.$(PSUFFIX) : zhbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zhbmv.$(SUFFIX) cblas_zhbmv.$(PSUFFIX) : zhbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_sspmv.$(SUFFIX) cblas_sspmv.$(PSUFFIX) : spmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dspmv.$(SUFFIX) cblas_dspmv.$(PSUFFIX) : spmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_sspr.$(SUFFIX) cblas_sspr.$(PSUFFIX) : spr.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dspr.$(SUFFIX) cblas_dspr.$(PSUFFIX) : spr.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_chpr.$(SUFFIX) cblas_chpr.$(PSUFFIX) : zhpr.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zhpr.$(SUFFIX) cblas_zhpr.$(PSUFFIX) : zhpr.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_sspr2.$(SUFFIX) cblas_sspr2.$(PSUFFIX) : spr2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dspr2.$(SUFFIX) cblas_dspr2.$(PSUFFIX) : spr2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_chpr2.$(SUFFIX) cblas_chpr2.$(PSUFFIX) : zhpr2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zhpr2.$(SUFFIX) cblas_zhpr2.$(PSUFFIX) : zhpr2.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_stbmv.$(SUFFIX) cblas_stbmv.$(PSUFFIX) : tbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtbmv.$(SUFFIX) cblas_dtbmv.$(PSUFFIX) : tbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctbmv.$(SUFFIX) cblas_ctbmv.$(PSUFFIX) : ztbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztbmv.$(SUFFIX) cblas_ztbmv.$(PSUFFIX) : ztbmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_stbsv.$(SUFFIX) cblas_stbsv.$(PSUFFIX) : tbsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtbsv.$(SUFFIX) cblas_dtbsv.$(PSUFFIX) : tbsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctbsv.$(SUFFIX) cblas_ctbsv.$(PSUFFIX) : ztbsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztbsv.$(SUFFIX) cblas_ztbsv.$(PSUFFIX) : ztbsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_stpmv.$(SUFFIX) cblas_stpmv.$(PSUFFIX) : tpmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtpmv.$(SUFFIX) cblas_dtpmv.$(PSUFFIX) : tpmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctpmv.$(SUFFIX) cblas_ctpmv.$(PSUFFIX) : ztpmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztpmv.$(SUFFIX) cblas_ztpmv.$(PSUFFIX) : ztpmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_chpmv.$(SUFFIX) cblas_chpmv.$(PSUFFIX) : zhpmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zhpmv.$(SUFFIX) cblas_zhpmv.$(PSUFFIX) : zhpmv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_stpsv.$(SUFFIX) cblas_stpsv.$(PSUFFIX) : tpsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtpsv.$(SUFFIX) cblas_dtpsv.$(PSUFFIX) : tpsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctpsv.$(SUFFIX) cblas_ctpsv.$(PSUFFIX) : ztpsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztpsv.$(SUFFIX) cblas_ztpsv.$(PSUFFIX) : ztpsv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ssymv.$(SUFFIX) cblas_ssymv.$(PSUFFIX) : symv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsymv.$(SUFFIX) cblas_dsymv.$(PSUFFIX) : symv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_chemv.$(SUFFIX) cblas_chemv.$(PSUFFIX) : zhemv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zhemv.$(SUFFIX) cblas_zhemv.$(PSUFFIX) : zhemv.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_sgemm.$(SUFFIX) cblas_sgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dgemm.$(SUFFIX) cblas_dgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_cgemm.$(SUFFIX) cblas_cgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zgemm.$(SUFFIX) cblas_zgemm.$(PSUFFIX) : gemm.c ../param.h
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ssymm.$(SUFFIX) cblas_ssymm.$(PSUFFIX) : symm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsymm.$(SUFFIX) cblas_dsymm.$(PSUFFIX) : symm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_csymm.$(SUFFIX) cblas_csymm.$(PSUFFIX) : symm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zsymm.$(SUFFIX) cblas_zsymm.$(PSUFFIX) : symm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ssyrk.$(SUFFIX) cblas_ssyrk.$(PSUFFIX) : syrk.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsyrk.$(SUFFIX) cblas_dsyrk.$(PSUFFIX) : syrk.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_csyrk.$(SUFFIX) cblas_csyrk.$(PSUFFIX) : syrk.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zsyrk.$(SUFFIX) cblas_zsyrk.$(PSUFFIX) : syrk.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ssyr2k.$(SUFFIX) cblas_ssyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dsyr2k.$(SUFFIX) cblas_dsyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_csyr2k.$(SUFFIX) cblas_csyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_zsyr2k.$(SUFFIX) cblas_zsyr2k.$(PSUFFIX) : syr2k.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_strmm.$(SUFFIX) cblas_strmm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DTRMM $< -o $(@F)

cblas_dtrmm.$(SUFFIX) cblas_dtrmm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DTRMM $< -o $(@F)

cblas_ctrmm.$(SUFFIX) cblas_ctrmm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DTRMM $< -o $(@F)

cblas_ztrmm.$(SUFFIX) cblas_ztrmm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DTRMM $< -o $(@F)

cblas_strsm.$(SUFFIX) cblas_strsm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_dtrsm.$(SUFFIX) cblas_dtrsm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ctrsm.$(SUFFIX) cblas_ctrsm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_ztrsm.$(SUFFIX) cblas_ztrsm.$(PSUFFIX) : trsm.c
	$(CC) -DCBLAS -c $(CFLAGS) $< -o $(@F)

cblas_chemm.$(SUFFIX) cblas_chemm.$(PSUFFIX) : symm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DHEMM $< -o $(@F)

cblas_zhemm.$(SUFFIX) cblas_zhemm.$(PSUFFIX) : symm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DHEMM $< -o $(@F)

cblas_cherk.$(SUFFIX) cblas_cherk.$(PSUFFIX) : syrk.c
	$(CC) -DCBLAS -c $(CFLAGS) -DHEMM $< -o $(@F)

cblas_zherk.$(SUFFIX) cblas_zherk.$(PSUFFIX) : syrk.c
	$(CC) -DCBLAS -c $(CFLAGS) -DHEMM $< -o $(@F)

cblas_cher2k.$(SUFFIX) cblas_cher2k.$(PSUFFIX) : syr2k.c
	$(CC) -DCBLAS -c $(CFLAGS) -DHEMM $< -o $(@F)

cblas_zher2k.$(SUFFIX) cblas_zher2k.$(PSUFFIX) : syr2k.c
	$(CC) -DCBLAS -c $(CFLAGS) -DHEMM $< -o $(@F)

cblas_cgemm3m.$(SUFFIX) cblas_cgemm3m.$(PSUFFIX) : gemm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DGEMM3M $< -o $(@F)

cblas_zgemm3m.$(SUFFIX) cblas_zgemm3m.$(PSUFFIX) : gemm.c
	$(CC) -DCBLAS -c $(CFLAGS) -DGEMM3M $< -o $(@F)


sgetf2.$(SUFFIX) sgetf2.$(PSUFFIX) : lapack/getf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dgetf2.$(SUFFIX) dgetf2.$(PSUFFIX) : lapack/getf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qgetf2.$(SUFFIX) qgetf2.$(PSUFFIX) : getf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgetf2.$(SUFFIX) cgetf2.$(PSUFFIX) : lapack/zgetf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zgetf2.$(SUFFIX) zgetf2.$(PSUFFIX) : lapack/zgetf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xgetf2.$(SUFFIX) xgetf2.$(PSUFFIX) : zgetf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sgetrf.$(SUFFIX) sgetrf.$(PSUFFIX) : lapack/getrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dgetrf.$(SUFFIX) dgetrf.$(PSUFFIX) : lapack/getrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qgetrf.$(SUFFIX) qgetrf.$(PSUFFIX) : getrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgetrf.$(SUFFIX) cgetrf.$(PSUFFIX) : lapack/zgetrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zgetrf.$(SUFFIX) zgetrf.$(PSUFFIX) : lapack/zgetrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xgetrf.$(SUFFIX) xgetrf.$(PSUFFIX) : zgetrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

slauu2.$(SUFFIX) slauu2.$(PSUFFIX) : lapack/lauu2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dlauu2.$(SUFFIX) dlauu2.$(PSUFFIX) : lapack/lauu2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qlauu2.$(SUFFIX) qlauu2.$(PSUFFIX) : lauu2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

clauu2.$(SUFFIX) clauu2.$(PSUFFIX) : lapack/zlauu2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zlauu2.$(SUFFIX) zlauu2.$(PSUFFIX) : lapack/zlauu2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xlauu2.$(SUFFIX) xlauu2.$(PSUFFIX) : zlauu2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

slauum.$(SUFFIX) slauum.$(PSUFFIX) : lapack/lauum.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dlauum.$(SUFFIX) dlauum.$(PSUFFIX) : lapack/lauum.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qlauum.$(SUFFIX) qlauum.$(PSUFFIX) : lauum.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

clauum.$(SUFFIX) clauum.$(PSUFFIX) : lapack/zlauum.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zlauum.$(SUFFIX) zlauum.$(PSUFFIX) : lapack/zlauum.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xlauum.$(SUFFIX) xlauum.$(PSUFFIX) : zlauum.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

spotf2.$(SUFFIX) spotf2.$(PSUFFIX) : lapack/potf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dpotf2.$(SUFFIX) dpotf2.$(PSUFFIX) : lapack/potf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qpotf2.$(SUFFIX) qpotf2.$(PSUFFIX) : potf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cpotf2.$(SUFFIX) cpotf2.$(PSUFFIX) : lapack/zpotf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zpotf2.$(SUFFIX) zpotf2.$(PSUFFIX) : lapack/zpotf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xpotf2.$(SUFFIX) xpotf2.$(PSUFFIX) : zpotf2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

spotrf.$(SUFFIX) spotrf.$(PSUFFIX) : lapack/potrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dpotrf.$(SUFFIX) dpotrf.$(PSUFFIX) : lapack/potrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qpotrf.$(SUFFIX) qpotrf.$(PSUFFIX) : potrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cpotrf.$(SUFFIX) cpotrf.$(PSUFFIX) : lapack/zpotrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zpotrf.$(SUFFIX) zpotrf.$(PSUFFIX) : lapack/zpotrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xpotrf.$(SUFFIX) xpotrf.$(PSUFFIX) : zpotrf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

strti2.$(SUFFIX) strti2.$(PSUFFIX) : lapack/trti2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtrti2.$(SUFFIX) dtrti2.$(PSUFFIX) : lapack/trti2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtrti2.$(SUFFIX) qtrti2.$(PSUFFIX) : trti2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctrti2.$(SUFFIX) ctrti2.$(PSUFFIX) : lapack/ztrti2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztrti2.$(SUFFIX) ztrti2.$(PSUFFIX) : lapack/ztrti2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtrti2.$(SUFFIX) xtrti2.$(PSUFFIX) : ztrti2.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

strtri.$(SUFFIX) strtri.$(PSUFFIX) : lapack/trtri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dtrtri.$(SUFFIX) dtrtri.$(PSUFFIX) : lapack/trtri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qtrtri.$(SUFFIX) qtrtri.$(PSUFFIX) : trtri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ctrtri.$(SUFFIX) ctrtri.$(PSUFFIX) : lapack/ztrtri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

ztrtri.$(SUFFIX) ztrtri.$(PSUFFIX) : lapack/ztrtri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xtrtri.$(SUFFIX) xtrtri.$(PSUFFIX) : ztrtri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

slaswp.$(SUFFIX) slaswp.$(PSUFFIX) : lapack/laswp.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dlaswp.$(SUFFIX) dlaswp.$(PSUFFIX) : lapack/laswp.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qlaswp.$(SUFFIX) qlaswp.$(PSUFFIX) : laswp.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

claswp.$(SUFFIX) claswp.$(PSUFFIX) : lapack/zlaswp.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zlaswp.$(SUFFIX) zlaswp.$(PSUFFIX) : lapack/zlaswp.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xlaswp.$(SUFFIX) xlaswp.$(PSUFFIX) : zlaswp.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sgetrs.$(SUFFIX) sgetrs.$(PSUFFIX) : lapack/getrs.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dgetrs.$(SUFFIX) dgetrs.$(PSUFFIX) : lapack/getrs.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qgetrs.$(SUFFIX) qgetrs.$(PSUFFIX) : getrs.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgetrs.$(SUFFIX) cgetrs.$(PSUFFIX) : lapack/zgetrs.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zgetrs.$(SUFFIX) zgetrs.$(PSUFFIX) : lapack/zgetrs.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xgetrs.$(SUFFIX) xgetrs.$(PSUFFIX) : zgetrs.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

sgesv.$(SUFFIX) sgesv.$(PSUFFIX) : lapack/gesv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dgesv.$(SUFFIX) dgesv.$(PSUFFIX) : lapack/gesv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qgesv.$(SUFFIX) qgesv.$(PSUFFIX) : gesv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgesv.$(SUFFIX) cgesv.$(PSUFFIX) : lapack/gesv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zgesv.$(SUFFIX) zgesv.$(PSUFFIX) : lapack/gesv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xgesv.$(SUFFIX) xgesv.$(PSUFFIX) : gesv.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

spotri.$(SUFFIX) spotri.$(PSUFFIX) : lapack/potri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dpotri.$(SUFFIX) dpotri.$(PSUFFIX) : lapack/potri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qpotri.$(SUFFIX) qpotri.$(PSUFFIX) : potri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cpotri.$(SUFFIX) cpotri.$(PSUFFIX) : lapack/zpotri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zpotri.$(SUFFIX) zpotri.$(PSUFFIX) : lapack/zpotri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xpotri.$(SUFFIX) xpotri.$(PSUFFIX) : zpotri.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

slarf.$(SUFFIX) slarf.$(PSUFFIX) : larf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dlarf.$(SUFFIX) dlarf.$(PSUFFIX) : larf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

qlarf.$(SUFFIX) qlarf.$(PSUFFIX) : larf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

clarf.$(SUFFIX) clarf.$(PSUFFIX) : larf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zlarf.$(SUFFIX) zlarf.$(PSUFFIX) : larf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

xlarf.$(SUFFIX) xlarf.$(PSUFFIX) : larf.c
	$(CC) -c $(CFLAGS) $< -o $(@F)


############# BLAS EXTENSIONS #####################################

daxpby.$(SUFFIX) daxpby.$(PSUFFIX) : axpby.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cblas_daxpby.$(SUFFIX) cblas_daxpby.$(PSUFFIX) : axpby.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

saxpby.$(SUFFIX) saxpby.$(PSUFFIX) : axpby.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cblas_saxpby.$(SUFFIX) cblas_saxpby.$(PSUFFIX) : axpby.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

zaxpby.$(SUFFIX) zaxpby.$(PSUFFIX) : zaxpby.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cblas_zaxpby.$(SUFFIX) cblas_zaxpby.$(PSUFFIX) : zaxpby.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

caxpby.$(SUFFIX) caxpby.$(PSUFFIX) : zaxpby.c
	$(CC) $(CFLAGS) -c $< -o $(@F)

cblas_caxpby.$(SUFFIX) cblas_caxpby.$(PSUFFIX) : zaxpby.c
	$(CC) $(CFLAGS) -DCBLAS -c $< -o $(@F)

domatcopy.$(SUFFIX) domatcopy.$(PSUFFIX) : omatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_domatcopy.$(SUFFIX) cblas_domatcopy.$(PSUFFIX) : omatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F) 

somatcopy.$(SUFFIX) somatcopy.$(PSUFFIX) : omatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_somatcopy.$(SUFFIX) cblas_somatcopy.$(PSUFFIX) : omatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

comatcopy.$(SUFFIX) comatcopy.$(PSUFFIX) : zomatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_comatcopy.$(SUFFIX) cblas_comatcopy.$(PSUFFIX) : zomatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

zomatcopy.$(SUFFIX) zomatcopy.$(PSUFFIX) : zomatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_zomatcopy.$(SUFFIX) cblas_zomatcopy.$(PSUFFIX) : zomatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

dimatcopy.$(SUFFIX) dimatcopy.$(PSUFFIX) : imatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_dimatcopy.$(SUFFIX) cblas_dimatcopy.$(PSUFFIX) : imatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

simatcopy.$(SUFFIX) simatcopy.$(PSUFFIX) : imatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_simatcopy.$(SUFFIX) cblas_simatcopy.$(PSUFFIX) : imatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

cimatcopy.$(SUFFIX) cimatcopy.$(PSUFFIX) : zimatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_cimatcopy.$(SUFFIX) cblas_cimatcopy.$(PSUFFIX) : zimatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

zimatcopy.$(SUFFIX) zimatcopy.$(PSUFFIX) : zimatcopy.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_zimatcopy.$(SUFFIX) cblas_zimatcopy.$(PSUFFIX) : zimatcopy.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

sgeadd.$(SUFFIX) sgeadd.$(PSUFFIX) : geadd.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

dgeadd.$(SUFFIX) dgeadd.$(PSUFFIX) : geadd.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cgeadd.$(SUFFIX) cgeadd.$(PSUFFIX) : zgeadd.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

zgeadd.$(SUFFIX) zgeadd.$(PSUFFIX) : zgeadd.c
	$(CC) -c $(CFLAGS) $< -o $(@F)

cblas_sgeadd.$(SUFFIX) cblas_sgeadd.$(PSUFFIX) : geadd.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

cblas_dgeadd.$(SUFFIX) cblas_dgeadd.$(PSUFFIX) : geadd.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

cblas_cgeadd.$(SUFFIX) cblas_cgeadd.$(PSUFFIX) : zgeadd.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

cblas_zgeadd.$(SUFFIX) cblas_zgeadd.$(PSUFFIX) : zgeadd.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)

cblas_xerbla.$(SUFFIX) cblas_xerbla.$(PSUFFIX) : xerbla.c
	$(CC) -c $(CFLAGS) -DCBLAS $< -o $(@F)
