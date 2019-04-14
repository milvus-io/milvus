TOPDIR	= ..
include $(TOPDIR)/Makefile.system

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

SBLAS1OBJS    = \
		saxpyf.$(SUFFIX) sswapf.$(SUFFIX) \
		scopyf.$(SUFFIX) sscalf.$(SUFFIX) \
		sdotf.$(SUFFIX) sdsdotf.$(SUFFIX) dsdotf.$(SUFFIX) \
		sasumf.$(SUFFIX) snrm2f.$(SUFFIX) \
		smaxf.$(SUFFIX) samaxf.$(SUFFIX) ismaxf.$(SUFFIX) isamaxf.$(SUFFIX) \
		sminf.$(SUFFIX) saminf.$(SUFFIX) isminf.$(SUFFIX) isaminf.$(SUFFIX) \
		srotf.$(SUFFIX) srotgf.$(SUFFIX) srotmf.$(SUFFIX) srotmgf.$(SUFFIX) \

SBLAS2OBJS    = \
		sgemvf.$(SUFFIX) sgerf.$(SUFFIX) \
		strsvf.$(SUFFIX) strmvf.$(SUFFIX) ssymvf.$(SUFFIX) \
		ssyrf.$(SUFFIX)  ssyr2f.$(SUFFIX) sgbmvf.$(SUFFIX) \
		ssbmvf.$(SUFFIX) sspmvf.$(SUFFIX) \
		ssprf.$(SUFFIX)  sspr2f.$(SUFFIX) \
		stbsvf.$(SUFFIX) stbmvf.$(SUFFIX) \
		stpsvf.$(SUFFIX) stpmvf.$(SUFFIX)

SBLAS3OBJS    = \
		sgemmf.$(SUFFIX) ssymmf.$(SUFFIX) strmmf.$(SUFFIX) \
		strsmf.$(SUFFIX) ssyrkf.$(SUFFIX) ssyr2kf.$(SUFFIX)

DBLAS1OBJS    = \
		daxpyf.$(SUFFIX) dswapf.$(SUFFIX) \
		dcopyf.$(SUFFIX) dscalf.$(SUFFIX) \
		 ddotf.$(SUFFIX) \
		dasumf.$(SUFFIX) dnrm2f.$(SUFFIX) \
		dmaxf.$(SUFFIX) damaxf.$(SUFFIX) idmaxf.$(SUFFIX) idamaxf.$(SUFFIX) \
		dminf.$(SUFFIX) daminf.$(SUFFIX) idminf.$(SUFFIX) idaminf.$(SUFFIX) \
		drotf.$(SUFFIX) drotgf.$(SUFFIX) drotmf.$(SUFFIX) drotmgf.$(SUFFIX) \

DBLAS2OBJS    = \
		dgemvf.$(SUFFIX) dgerf.$(SUFFIX) \
		dtrsvf.$(SUFFIX) dtrmvf.$(SUFFIX) dsymvf.$(SUFFIX) \
		dsyrf.$(SUFFIX)  dsyr2f.$(SUFFIX) dgbmvf.$(SUFFIX) \
		dsbmvf.$(SUFFIX) dspmvf.$(SUFFIX) \
		dsprf.$(SUFFIX)  dspr2f.$(SUFFIX) \
		dtbsvf.$(SUFFIX) dtbmvf.$(SUFFIX) \
		dtpsvf.$(SUFFIX) dtpmvf.$(SUFFIX)

DBLAS3OBJS    = \
		dgemmf.$(SUFFIX) dsymmf.$(SUFFIX) dtrmmf.$(SUFFIX) \
		dtrsmf.$(SUFFIX) dsyrkf.$(SUFFIX) dsyr2kf.$(SUFFIX)

CBLAS1OBJS    = \
		caxpyf.$(SUFFIX) caxpycf.$(SUFFIX) cswapf.$(SUFFIX) \
		ccopyf.$(SUFFIX) cscalf.$(SUFFIX) csscalf.$(SUFFIX) \
		cdotcf.$(SUFFIX)  cdotuf.$(SUFFIX) \
		scasumf.$(SUFFIX) scnrm2f.$(SUFFIX) \
		scamaxf.$(SUFFIX) icamaxf.$(SUFFIX) \
		scaminf.$(SUFFIX) icaminf.$(SUFFIX) \
		csrotf.$(SUFFIX) crotgf.$(SUFFIX) \

CBLAS2OBJS    = \
		cgemvf.$(SUFFIX) cgeruf.$(SUFFIX) cgercf.$(SUFFIX) \
		ctrsvf.$(SUFFIX) ctrmvf.$(SUFFIX) csymvf.$(SUFFIX) \
		csyrf.$(SUFFIX)  csyr2f.$(SUFFIX) cgbmvf.$(SUFFIX) \
		csbmvf.$(SUFFIX) cspmvf.$(SUFFIX) \
		csprf.$(SUFFIX)  cspr2f.$(SUFFIX) \
		ctbsvf.$(SUFFIX) ctbmvf.$(SUFFIX) \
		ctpsvf.$(SUFFIX) ctpmvf.$(SUFFIX) \
		chemvf.$(SUFFIX) chbmvf.$(SUFFIX) \
		cherf.$(SUFFIX)  cher2f.$(SUFFIX) \
		chpmvf.$(SUFFIX) chprf.$(SUFFIX)  chpr2f.$(SUFFIX)

CBLAS3OBJS    = \
		cgemmf.$(SUFFIX) csymmf.$(SUFFIX) ctrmmf.$(SUFFIX) \
		ctrsmf.$(SUFFIX) csyrkf.$(SUFFIX) csyr2kf.$(SUFFIX) \
	       	chemmf.$(SUFFIX) cherkf.$(SUFFIX) cher2kf.$(SUFFIX)

ZBLAS1OBJS    = \
		zaxpyf.$(SUFFIX) zaxpycf.$(SUFFIX) zswapf.$(SUFFIX) \
		zcopyf.$(SUFFIX) zscalf.$(SUFFIX) zdscalf.$(SUFFIX) \
		zdotcf.$(SUFFIX)  zdotuf.$(SUFFIX) \
		dzasumf.$(SUFFIX)  dznrm2f.$(SUFFIX) \
		dzamaxf.$(SUFFIX) izamaxf.$(SUFFIX) \
		dzaminf.$(SUFFIX) izaminf.$(SUFFIX) \
		zdrotf.$(SUFFIX) zrotgf.$(SUFFIX) \

ZBLAS2OBJS    = \
		zgemvf.$(SUFFIX) zgeruf.$(SUFFIX) zgercf.$(SUFFIX) \
		ztrsvf.$(SUFFIX) ztrmvf.$(SUFFIX) zsymvf.$(SUFFIX) \
		zsyrf.$(SUFFIX)  zsyr2f.$(SUFFIX) zgbmvf.$(SUFFIX) \
		zsbmvf.$(SUFFIX) zspmvf.$(SUFFIX) \
		zsprf.$(SUFFIX)  zspr2f.$(SUFFIX) \
		ztbsvf.$(SUFFIX) ztbmvf.$(SUFFIX) \
		ztpsvf.$(SUFFIX) ztpmvf.$(SUFFIX) \
		zhemvf.$(SUFFIX) zhbmvf.$(SUFFIX) \
		zherf.$(SUFFIX)  zher2f.$(SUFFIX) \
		zhpmvf.$(SUFFIX) zhprf.$(SUFFIX)  zhpr2f.$(SUFFIX)

ZBLAS3OBJS    = \
		zgemmf.$(SUFFIX) zsymmf.$(SUFFIX) ztrmmf.$(SUFFIX) \
		ztrsmf.$(SUFFIX) zsyrkf.$(SUFFIX) zsyr2kf.$(SUFFIX) \
	       	zhemmf.$(SUFFIX) zherkf.$(SUFFIX) zher2kf.$(SUFFIX)

ifdef SUPPORT_GEMM3M

CBLAS3OBJS   +=  cgemm3mf.$(SUFFIX) csymm3mf.$(SUFFIX) chemm3mf.$(SUFFIX)

ZBLAS3OBJS   +=  zgemm3mf.$(SUFFIX) zsymm3mf.$(SUFFIX) zhemm3mf.$(SUFFIX)

endif

SBLASOBJS    = $(SBLAS1OBJS) $(SBLAS2OBJS) $(SBLAS3OBJS)
DBLASOBJS    = $(DBLAS1OBJS) $(DBLAS2OBJS) $(DBLAS3OBJS)
QBLASOBJS    = $(QBLAS1OBJS) $(QBLAS2OBJS) $(QBLAS3OBJS)
CBLASOBJS    = $(CBLAS1OBJS) $(CBLAS2OBJS) $(CBLAS3OBJS)
ZBLASOBJS    = $(ZBLAS1OBJS) $(ZBLAS2OBJS) $(ZBLAS3OBJS)
XBLASOBJS    = $(XBLAS1OBJS) $(XBLAS2OBJS) $(XBLAS3OBJS)

ifneq ($(NO_LAPACK), 1)

SBLASOBJS	+= \
	sgetf2f.$(SUFFIX) sgetrff.$(SUFFIX) slauu2f.$(SUFFIX) slauumf.$(SUFFIX) \
	spotf2f.$(SUFFIX) spotrff.$(SUFFIX) strti2f.$(SUFFIX) strtrif.$(SUFFIX) \
	slaswpf.$(SUFFIX) sgetrsf.$(SUFFIX) sgesvf.$(SUFFIX)  spotrif.$(SUFFIX) \

DBLASOBJS	+= \
	dgetf2f.$(SUFFIX) dgetrff.$(SUFFIX) dlauu2f.$(SUFFIX) dlauumf.$(SUFFIX) \
	dpotf2f.$(SUFFIX) dpotrff.$(SUFFIX) dtrti2f.$(SUFFIX) dtrtrif.$(SUFFIX) \
	dlaswpf.$(SUFFIX) dgetrsf.$(SUFFIX) dgesvf.$(SUFFIX)  dpotrif.$(SUFFIX) \

QBLASOBJS	+=
#	\
	qgetf2f.$(SUFFIX) qgetrff.$(SUFFIX) qlauu2f.$(SUFFIX) qlauumf.$(SUFFIX) \
	qpotf2f.$(SUFFIX) qpotrff.$(SUFFIX) qtrti2f.$(SUFFIX) qtrtrif.$(SUFFIX) \
	qlaswpf.$(SUFFIX) qgetrsf.$(SUFFIX) qgesvf.$(SUFFIX)  qpotrif.$(SUFFIX) \

CBLASOBJS	+= \
	cgetf2f.$(SUFFIX) cgetrff.$(SUFFIX) clauu2f.$(SUFFIX) clauumf.$(SUFFIX) \
	cpotf2f.$(SUFFIX) cpotrff.$(SUFFIX) ctrti2f.$(SUFFIX) ctrtrif.$(SUFFIX) \
	claswpf.$(SUFFIX) cgetrsf.$(SUFFIX) cgesvf.$(SUFFIX)  cpotrif.$(SUFFIX) \

ZBLASOBJS	+= \
	zgetf2f.$(SUFFIX) zgetrff.$(SUFFIX) zlauu2f.$(SUFFIX) zlauumf.$(SUFFIX) \
	zpotf2f.$(SUFFIX) zpotrff.$(SUFFIX) ztrti2f.$(SUFFIX) ztrtrif.$(SUFFIX) \
	zlaswpf.$(SUFFIX) zgetrsf.$(SUFFIX) zgesvf.$(SUFFIX)  zpotrif.$(SUFFIX) \

XBLASOBJS	+=
#	\
	xgetf2f.$(SUFFIX) xgetrff.$(SUFFIX) xlauu2f.$(SUFFIX) xlauumf.$(SUFFIX) \
	xpotf2f.$(SUFFIX) xpotrff.$(SUFFIX) xtrti2f.$(SUFFIX) xtrtrif.$(SUFFIX) \
	xlaswpf.$(SUFFIX) xgetrsf.$(SUFFIX) xgesvf.$(SUFFIX)  xpotrif.$(SUFFIX) \

endif

include $(TOPDIR)/Makefile.tail

all :: libs

clean ::

level1 : $(SBLAS1OBJS) $(DBLAS1OBJS) $(QBLAS1OBJS) $(CBLAS1OBJS) $(ZBLAS1OBJS) $(XBLAS1OBJS)
	$(AR) $(ARFLAGS) -ru $(TOPDIR)/$(LIBNAME) $^

level2 : $(SBLAS2OBJS) $(DBLAS2OBJS) $(QBLAS2OBJS) $(CBLAS2OBJS) $(ZBLAS2OBJS) $(XBLAS2OBJS)
	$(AR) $(ARFLAGS) -ru $(TOPDIR)/$(LIBNAME) $^

level3 : $(SBLAS3OBJS) $(DBLAS3OBJS) $(QBLAS3OBJS) $(CBLAS3OBJS) $(ZBLAS3OBJS) $(XBLAS3OBJS)
	$(AR) $(ARFLAGS) -ru $(TOPDIR)/$(LIBNAME) $^

