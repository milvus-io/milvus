TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = \
	sgbmv_n.$(SUFFIX) sgbmv_t.$(SUFFIX) \
	ssbmv_U.$(SUFFIX) ssbmv_L.$(SUFFIX) sspmv_U.$(SUFFIX) sspmv_L.$(SUFFIX) \
	sspr_U.$(SUFFIX) sspr_L.$(SUFFIX) sspr2_U.$(SUFFIX) sspr2_L.$(SUFFIX) \
	ssyr_U.$(SUFFIX) ssyr_L.$(SUFFIX) ssyr2_U.$(SUFFIX) ssyr2_L.$(SUFFIX) \
	stbmv_NUU.$(SUFFIX) stbmv_NUN.$(SUFFIX) stbmv_NLU.$(SUFFIX) stbmv_NLN.$(SUFFIX) \
	stbmv_TUU.$(SUFFIX) stbmv_TUN.$(SUFFIX) stbmv_TLU.$(SUFFIX) stbmv_TLN.$(SUFFIX) \
	stbsv_NUU.$(SUFFIX) stbsv_NUN.$(SUFFIX) stbsv_NLU.$(SUFFIX) stbsv_NLN.$(SUFFIX) \
	stbsv_TUU.$(SUFFIX) stbsv_TUN.$(SUFFIX) stbsv_TLU.$(SUFFIX) stbsv_TLN.$(SUFFIX) \
	stpmv_NUU.$(SUFFIX) stpmv_NUN.$(SUFFIX) stpmv_NLU.$(SUFFIX) stpmv_NLN.$(SUFFIX) \
	stpmv_TUU.$(SUFFIX) stpmv_TUN.$(SUFFIX) stpmv_TLU.$(SUFFIX) stpmv_TLN.$(SUFFIX) \
	stpsv_NUU.$(SUFFIX) stpsv_NUN.$(SUFFIX) stpsv_NLU.$(SUFFIX) stpsv_NLN.$(SUFFIX) \
	stpsv_TUU.$(SUFFIX) stpsv_TUN.$(SUFFIX) stpsv_TLU.$(SUFFIX) stpsv_TLN.$(SUFFIX) \
	strmv_NUU.$(SUFFIX) strmv_NUN.$(SUFFIX) strmv_NLU.$(SUFFIX) strmv_NLN.$(SUFFIX) \
	strmv_TUU.$(SUFFIX) strmv_TUN.$(SUFFIX) strmv_TLU.$(SUFFIX) strmv_TLN.$(SUFFIX) \
	strsv_NUU.$(SUFFIX) strsv_NUN.$(SUFFIX) strsv_NLU.$(SUFFIX) strsv_NLN.$(SUFFIX) \
	strsv_TUU.$(SUFFIX) strsv_TUN.$(SUFFIX) strsv_TLU.$(SUFFIX) strsv_TLN.$(SUFFIX)

DBLASOBJS = \
	dgbmv_n.$(SUFFIX) dgbmv_t.$(SUFFIX) \
	dsbmv_U.$(SUFFIX) dsbmv_L.$(SUFFIX) dspmv_U.$(SUFFIX) dspmv_L.$(SUFFIX) \
	dspr_U.$(SUFFIX) dspr_L.$(SUFFIX) dspr2_U.$(SUFFIX) dspr2_L.$(SUFFIX) \
	dsyr_U.$(SUFFIX) dsyr_L.$(SUFFIX) dsyr2_U.$(SUFFIX) dsyr2_L.$(SUFFIX) \
	dtbmv_NUU.$(SUFFIX) dtbmv_NUN.$(SUFFIX) dtbmv_NLU.$(SUFFIX) dtbmv_NLN.$(SUFFIX) \
	dtbmv_TUU.$(SUFFIX) dtbmv_TUN.$(SUFFIX) dtbmv_TLU.$(SUFFIX) dtbmv_TLN.$(SUFFIX) \
	dtbsv_NUU.$(SUFFIX) dtbsv_NUN.$(SUFFIX) dtbsv_NLU.$(SUFFIX) dtbsv_NLN.$(SUFFIX) \
	dtbsv_TUU.$(SUFFIX) dtbsv_TUN.$(SUFFIX) dtbsv_TLU.$(SUFFIX) dtbsv_TLN.$(SUFFIX) \
	dtpmv_NUU.$(SUFFIX) dtpmv_NUN.$(SUFFIX) dtpmv_NLU.$(SUFFIX) dtpmv_NLN.$(SUFFIX) \
	dtpmv_TUU.$(SUFFIX) dtpmv_TUN.$(SUFFIX) dtpmv_TLU.$(SUFFIX) dtpmv_TLN.$(SUFFIX) \
	dtpsv_NUU.$(SUFFIX) dtpsv_NUN.$(SUFFIX) dtpsv_NLU.$(SUFFIX) dtpsv_NLN.$(SUFFIX) \
	dtpsv_TUU.$(SUFFIX) dtpsv_TUN.$(SUFFIX) dtpsv_TLU.$(SUFFIX) dtpsv_TLN.$(SUFFIX) \
	dtrmv_NUU.$(SUFFIX) dtrmv_NUN.$(SUFFIX) dtrmv_NLU.$(SUFFIX) dtrmv_NLN.$(SUFFIX) \
	dtrmv_TUU.$(SUFFIX) dtrmv_TUN.$(SUFFIX) dtrmv_TLU.$(SUFFIX) dtrmv_TLN.$(SUFFIX) \
	dtrsv_NUU.$(SUFFIX) dtrsv_NUN.$(SUFFIX) dtrsv_NLU.$(SUFFIX) dtrsv_NLN.$(SUFFIX) \
	dtrsv_TUU.$(SUFFIX) dtrsv_TUN.$(SUFFIX) dtrsv_TLU.$(SUFFIX) dtrsv_TLN.$(SUFFIX)

QBLASOBJS = \
	qgbmv_n.$(SUFFIX) qgbmv_t.$(SUFFIX) \
	qsbmv_U.$(SUFFIX) qsbmv_L.$(SUFFIX) qspmv_U.$(SUFFIX) qspmv_L.$(SUFFIX) \
	qspr_U.$(SUFFIX) qspr_L.$(SUFFIX) qspr2_U.$(SUFFIX) qspr2_L.$(SUFFIX) \
	qsyr_U.$(SUFFIX) qsyr_L.$(SUFFIX) qsyr2_U.$(SUFFIX) qsyr2_L.$(SUFFIX) \
	qtbmv_NUU.$(SUFFIX) qtbmv_NUN.$(SUFFIX) qtbmv_NLU.$(SUFFIX) qtbmv_NLN.$(SUFFIX) \
	qtbmv_TUU.$(SUFFIX) qtbmv_TUN.$(SUFFIX) qtbmv_TLU.$(SUFFIX) qtbmv_TLN.$(SUFFIX) \
	qtbsv_NUU.$(SUFFIX) qtbsv_NUN.$(SUFFIX) qtbsv_NLU.$(SUFFIX) qtbsv_NLN.$(SUFFIX) \
	qtbsv_TUU.$(SUFFIX) qtbsv_TUN.$(SUFFIX) qtbsv_TLU.$(SUFFIX) qtbsv_TLN.$(SUFFIX) \
	qtpmv_NUU.$(SUFFIX) qtpmv_NUN.$(SUFFIX) qtpmv_NLU.$(SUFFIX) qtpmv_NLN.$(SUFFIX) \
	qtpmv_TUU.$(SUFFIX) qtpmv_TUN.$(SUFFIX) qtpmv_TLU.$(SUFFIX) qtpmv_TLN.$(SUFFIX) \
	qtpsv_NUU.$(SUFFIX) qtpsv_NUN.$(SUFFIX) qtpsv_NLU.$(SUFFIX) qtpsv_NLN.$(SUFFIX) \
	qtpsv_TUU.$(SUFFIX) qtpsv_TUN.$(SUFFIX) qtpsv_TLU.$(SUFFIX) qtpsv_TLN.$(SUFFIX) \
	qtrmv_NUU.$(SUFFIX) qtrmv_NUN.$(SUFFIX) qtrmv_NLU.$(SUFFIX) qtrmv_NLN.$(SUFFIX) \
	qtrmv_TUU.$(SUFFIX) qtrmv_TUN.$(SUFFIX) qtrmv_TLU.$(SUFFIX) qtrmv_TLN.$(SUFFIX) \
	qtrsv_NUU.$(SUFFIX) qtrsv_NUN.$(SUFFIX) qtrsv_NLU.$(SUFFIX) qtrsv_NLN.$(SUFFIX) \
	qtrsv_TUU.$(SUFFIX) qtrsv_TUN.$(SUFFIX) qtrsv_TLU.$(SUFFIX) qtrsv_TLN.$(SUFFIX)

CBLASOBJS += \
	cgbmv_n.$(SUFFIX) cgbmv_t.$(SUFFIX) cgbmv_r.$(SUFFIX) cgbmv_c.$(SUFFIX) \
	cgbmv_o.$(SUFFIX) cgbmv_u.$(SUFFIX) cgbmv_s.$(SUFFIX) cgbmv_d.$(SUFFIX) \
	chbmv_U.$(SUFFIX) chbmv_L.$(SUFFIX) chbmv_V.$(SUFFIX) chbmv_M.$(SUFFIX) \
	cher_U.$(SUFFIX)  cher_L.$(SUFFIX)  cher_V.$(SUFFIX)  cher_M.$(SUFFIX) \
	cher2_U.$(SUFFIX) cher2_L.$(SUFFIX) cher2_V.$(SUFFIX) cher2_M.$(SUFFIX) \
	chpmv_U.$(SUFFIX) chpmv_L.$(SUFFIX) chpmv_V.$(SUFFIX) chpmv_M.$(SUFFIX) \
	chpr_U.$(SUFFIX)  chpr_L.$(SUFFIX)  chpr_V.$(SUFFIX)  chpr_M.$(SUFFIX) \
	chpr2_U.$(SUFFIX) chpr2_L.$(SUFFIX) chpr2_V.$(SUFFIX) chpr2_M.$(SUFFIX) \
	csbmv_U.$(SUFFIX) csbmv_L.$(SUFFIX) cspmv_U.$(SUFFIX) cspmv_L.$(SUFFIX) \
	cspr_U.$(SUFFIX)  cspr_L.$(SUFFIX)  cspr2_U.$(SUFFIX) cspr2_L.$(SUFFIX) \
	csyr_U.$(SUFFIX)  csyr_L.$(SUFFIX)  csyr2_U.$(SUFFIX) csyr2_L.$(SUFFIX) \
	ctbmv_NUU.$(SUFFIX) ctbmv_NUN.$(SUFFIX) ctbmv_NLU.$(SUFFIX) ctbmv_NLN.$(SUFFIX) \
	ctbmv_TUU.$(SUFFIX) ctbmv_TUN.$(SUFFIX) ctbmv_TLU.$(SUFFIX) ctbmv_TLN.$(SUFFIX) \
	ctbmv_RUU.$(SUFFIX) ctbmv_RUN.$(SUFFIX) ctbmv_RLU.$(SUFFIX) ctbmv_RLN.$(SUFFIX) \
	ctbmv_CUU.$(SUFFIX) ctbmv_CUN.$(SUFFIX) ctbmv_CLU.$(SUFFIX) ctbmv_CLN.$(SUFFIX) \
	ctbsv_NUU.$(SUFFIX) ctbsv_NUN.$(SUFFIX) ctbsv_NLU.$(SUFFIX) ctbsv_NLN.$(SUFFIX) \
	ctbsv_TUU.$(SUFFIX) ctbsv_TUN.$(SUFFIX) ctbsv_TLU.$(SUFFIX) ctbsv_TLN.$(SUFFIX) \
	ctbsv_RUU.$(SUFFIX) ctbsv_RUN.$(SUFFIX) ctbsv_RLU.$(SUFFIX) ctbsv_RLN.$(SUFFIX) \
	ctbsv_CUU.$(SUFFIX) ctbsv_CUN.$(SUFFIX) ctbsv_CLU.$(SUFFIX) ctbsv_CLN.$(SUFFIX) \
	ctpmv_NUU.$(SUFFIX) ctpmv_NUN.$(SUFFIX) ctpmv_NLU.$(SUFFIX) ctpmv_NLN.$(SUFFIX) \
	ctpmv_TUU.$(SUFFIX) ctpmv_TUN.$(SUFFIX) ctpmv_TLU.$(SUFFIX) ctpmv_TLN.$(SUFFIX) \
	ctpmv_RUU.$(SUFFIX) ctpmv_RUN.$(SUFFIX) ctpmv_RLU.$(SUFFIX) ctpmv_RLN.$(SUFFIX) \
	ctpmv_CUU.$(SUFFIX) ctpmv_CUN.$(SUFFIX) ctpmv_CLU.$(SUFFIX) ctpmv_CLN.$(SUFFIX) \
	ctpsv_NUU.$(SUFFIX) ctpsv_NUN.$(SUFFIX) ctpsv_NLU.$(SUFFIX) ctpsv_NLN.$(SUFFIX) \
	ctpsv_TUU.$(SUFFIX) ctpsv_TUN.$(SUFFIX) ctpsv_TLU.$(SUFFIX) ctpsv_TLN.$(SUFFIX) \
	ctpsv_RUU.$(SUFFIX) ctpsv_RUN.$(SUFFIX) ctpsv_RLU.$(SUFFIX) ctpsv_RLN.$(SUFFIX) \
	ctpsv_CUU.$(SUFFIX) ctpsv_CUN.$(SUFFIX) ctpsv_CLU.$(SUFFIX) ctpsv_CLN.$(SUFFIX) \
	ctrmv_NUU.$(SUFFIX) ctrmv_NUN.$(SUFFIX) ctrmv_NLU.$(SUFFIX) ctrmv_NLN.$(SUFFIX) \
	ctrmv_TUU.$(SUFFIX) ctrmv_TUN.$(SUFFIX) ctrmv_TLU.$(SUFFIX) ctrmv_TLN.$(SUFFIX) \
	ctrmv_RUU.$(SUFFIX) ctrmv_RUN.$(SUFFIX) ctrmv_RLU.$(SUFFIX) ctrmv_RLN.$(SUFFIX) \
	ctrmv_CUU.$(SUFFIX) ctrmv_CUN.$(SUFFIX) ctrmv_CLU.$(SUFFIX) ctrmv_CLN.$(SUFFIX) \
	ctrsv_NUU.$(SUFFIX) ctrsv_NUN.$(SUFFIX) ctrsv_NLU.$(SUFFIX) ctrsv_NLN.$(SUFFIX) \
	ctrsv_TUU.$(SUFFIX) ctrsv_TUN.$(SUFFIX) ctrsv_TLU.$(SUFFIX) ctrsv_TLN.$(SUFFIX) \
	ctrsv_RUU.$(SUFFIX) ctrsv_RUN.$(SUFFIX) ctrsv_RLU.$(SUFFIX) ctrsv_RLN.$(SUFFIX) \
	ctrsv_CUU.$(SUFFIX) ctrsv_CUN.$(SUFFIX) ctrsv_CLU.$(SUFFIX) ctrsv_CLN.$(SUFFIX)

ZBLASOBJS += \
	zgbmv_n.$(SUFFIX) zgbmv_t.$(SUFFIX) zgbmv_r.$(SUFFIX) zgbmv_c.$(SUFFIX) \
	zgbmv_o.$(SUFFIX) zgbmv_u.$(SUFFIX) zgbmv_s.$(SUFFIX) zgbmv_d.$(SUFFIX) \
	zhbmv_U.$(SUFFIX) zhbmv_L.$(SUFFIX) zhbmv_V.$(SUFFIX) zhbmv_M.$(SUFFIX) \
	zher_U.$(SUFFIX)  zher_L.$(SUFFIX)  zher_V.$(SUFFIX)  zher_M.$(SUFFIX) \
	zher2_U.$(SUFFIX) zher2_L.$(SUFFIX) zher2_V.$(SUFFIX) zher2_M.$(SUFFIX) \
	zhpmv_U.$(SUFFIX) zhpmv_L.$(SUFFIX) zhpmv_V.$(SUFFIX) zhpmv_M.$(SUFFIX) \
	zhpr_U.$(SUFFIX)  zhpr_L.$(SUFFIX)  zhpr_V.$(SUFFIX)  zhpr_M.$(SUFFIX) \
	zhpr2_U.$(SUFFIX) zhpr2_L.$(SUFFIX) zhpr2_V.$(SUFFIX) zhpr2_M.$(SUFFIX) \
	zsbmv_U.$(SUFFIX) zsbmv_L.$(SUFFIX) zspmv_U.$(SUFFIX) zspmv_L.$(SUFFIX) \
	zspr_U.$(SUFFIX)  zspr_L.$(SUFFIX)  zspr2_U.$(SUFFIX) zspr2_L.$(SUFFIX) \
	zsyr_U.$(SUFFIX)  zsyr_L.$(SUFFIX)  zsyr2_U.$(SUFFIX) zsyr2_L.$(SUFFIX) \
	ztbmv_NUU.$(SUFFIX) ztbmv_NUN.$(SUFFIX) ztbmv_NLU.$(SUFFIX) ztbmv_NLN.$(SUFFIX) \
	ztbmv_TUU.$(SUFFIX) ztbmv_TUN.$(SUFFIX) ztbmv_TLU.$(SUFFIX) ztbmv_TLN.$(SUFFIX) \
	ztbmv_RUU.$(SUFFIX) ztbmv_RUN.$(SUFFIX) ztbmv_RLU.$(SUFFIX) ztbmv_RLN.$(SUFFIX) \
	ztbmv_CUU.$(SUFFIX) ztbmv_CUN.$(SUFFIX) ztbmv_CLU.$(SUFFIX) ztbmv_CLN.$(SUFFIX) \
	ztbsv_NUU.$(SUFFIX) ztbsv_NUN.$(SUFFIX) ztbsv_NLU.$(SUFFIX) ztbsv_NLN.$(SUFFIX) \
	ztbsv_TUU.$(SUFFIX) ztbsv_TUN.$(SUFFIX) ztbsv_TLU.$(SUFFIX) ztbsv_TLN.$(SUFFIX) \
	ztbsv_RUU.$(SUFFIX) ztbsv_RUN.$(SUFFIX) ztbsv_RLU.$(SUFFIX) ztbsv_RLN.$(SUFFIX) \
	ztbsv_CUU.$(SUFFIX) ztbsv_CUN.$(SUFFIX) ztbsv_CLU.$(SUFFIX) ztbsv_CLN.$(SUFFIX) \
	ztpmv_NUU.$(SUFFIX) ztpmv_NUN.$(SUFFIX) ztpmv_NLU.$(SUFFIX) ztpmv_NLN.$(SUFFIX) \
	ztpmv_TUU.$(SUFFIX) ztpmv_TUN.$(SUFFIX) ztpmv_TLU.$(SUFFIX) ztpmv_TLN.$(SUFFIX) \
	ztpmv_RUU.$(SUFFIX) ztpmv_RUN.$(SUFFIX) ztpmv_RLU.$(SUFFIX) ztpmv_RLN.$(SUFFIX) \
	ztpmv_CUU.$(SUFFIX) ztpmv_CUN.$(SUFFIX) ztpmv_CLU.$(SUFFIX) ztpmv_CLN.$(SUFFIX) \
	ztpsv_NUU.$(SUFFIX) ztpsv_NUN.$(SUFFIX) ztpsv_NLU.$(SUFFIX) ztpsv_NLN.$(SUFFIX) \
	ztpsv_TUU.$(SUFFIX) ztpsv_TUN.$(SUFFIX) ztpsv_TLU.$(SUFFIX) ztpsv_TLN.$(SUFFIX) \
	ztpsv_RUU.$(SUFFIX) ztpsv_RUN.$(SUFFIX) ztpsv_RLU.$(SUFFIX) ztpsv_RLN.$(SUFFIX) \
	ztpsv_CUU.$(SUFFIX) ztpsv_CUN.$(SUFFIX) ztpsv_CLU.$(SUFFIX) ztpsv_CLN.$(SUFFIX) \
	ztrmv_NUU.$(SUFFIX) ztrmv_NUN.$(SUFFIX) ztrmv_NLU.$(SUFFIX) ztrmv_NLN.$(SUFFIX) \
	ztrmv_TUU.$(SUFFIX) ztrmv_TUN.$(SUFFIX) ztrmv_TLU.$(SUFFIX) ztrmv_TLN.$(SUFFIX) \
	ztrmv_RUU.$(SUFFIX) ztrmv_RUN.$(SUFFIX) ztrmv_RLU.$(SUFFIX) ztrmv_RLN.$(SUFFIX) \
	ztrmv_CUU.$(SUFFIX) ztrmv_CUN.$(SUFFIX) ztrmv_CLU.$(SUFFIX) ztrmv_CLN.$(SUFFIX) \
	ztrsv_NUU.$(SUFFIX) ztrsv_NUN.$(SUFFIX) ztrsv_NLU.$(SUFFIX) ztrsv_NLN.$(SUFFIX) \
	ztrsv_TUU.$(SUFFIX) ztrsv_TUN.$(SUFFIX) ztrsv_TLU.$(SUFFIX) ztrsv_TLN.$(SUFFIX) \
	ztrsv_RUU.$(SUFFIX) ztrsv_RUN.$(SUFFIX) ztrsv_RLU.$(SUFFIX) ztrsv_RLN.$(SUFFIX) \
	ztrsv_CUU.$(SUFFIX) ztrsv_CUN.$(SUFFIX) ztrsv_CLU.$(SUFFIX) ztrsv_CLN.$(SUFFIX)

XBLASOBJS += \
	xgbmv_n.$(SUFFIX) xgbmv_t.$(SUFFIX) xgbmv_r.$(SUFFIX) xgbmv_c.$(SUFFIX) \
	xgbmv_o.$(SUFFIX) xgbmv_u.$(SUFFIX) xgbmv_s.$(SUFFIX) xgbmv_d.$(SUFFIX) \
	xhbmv_U.$(SUFFIX) xhbmv_L.$(SUFFIX) xhbmv_V.$(SUFFIX) xhbmv_M.$(SUFFIX) \
	xher_U.$(SUFFIX)  xher_L.$(SUFFIX)  xher_V.$(SUFFIX)  xher_M.$(SUFFIX) \
	xher2_U.$(SUFFIX) xher2_L.$(SUFFIX) xher2_V.$(SUFFIX) xher2_M.$(SUFFIX) \
	xhpmv_U.$(SUFFIX) xhpmv_L.$(SUFFIX) xhpmv_V.$(SUFFIX) xhpmv_M.$(SUFFIX) \
	xhpr_U.$(SUFFIX)  xhpr_L.$(SUFFIX)  xhpr_V.$(SUFFIX)  xhpr_M.$(SUFFIX) \
	xhpr2_U.$(SUFFIX) xhpr2_L.$(SUFFIX) xhpr2_V.$(SUFFIX) xhpr2_M.$(SUFFIX) \
	xsbmv_U.$(SUFFIX) xsbmv_L.$(SUFFIX) xspmv_U.$(SUFFIX) xspmv_L.$(SUFFIX) \
	xspr_U.$(SUFFIX)  xspr_L.$(SUFFIX)  xspr2_U.$(SUFFIX) xspr2_L.$(SUFFIX) \
	xsyr_U.$(SUFFIX)  xsyr_L.$(SUFFIX)  xsyr2_U.$(SUFFIX) xsyr2_L.$(SUFFIX) \
	xtbmv_NUU.$(SUFFIX) xtbmv_NUN.$(SUFFIX) xtbmv_NLU.$(SUFFIX) xtbmv_NLN.$(SUFFIX) \
	xtbmv_TUU.$(SUFFIX) xtbmv_TUN.$(SUFFIX) xtbmv_TLU.$(SUFFIX) xtbmv_TLN.$(SUFFIX) \
	xtbmv_RUU.$(SUFFIX) xtbmv_RUN.$(SUFFIX) xtbmv_RLU.$(SUFFIX) xtbmv_RLN.$(SUFFIX) \
	xtbmv_CUU.$(SUFFIX) xtbmv_CUN.$(SUFFIX) xtbmv_CLU.$(SUFFIX) xtbmv_CLN.$(SUFFIX) \
	xtbsv_NUU.$(SUFFIX) xtbsv_NUN.$(SUFFIX) xtbsv_NLU.$(SUFFIX) xtbsv_NLN.$(SUFFIX) \
	xtbsv_TUU.$(SUFFIX) xtbsv_TUN.$(SUFFIX) xtbsv_TLU.$(SUFFIX) xtbsv_TLN.$(SUFFIX) \
	xtbsv_RUU.$(SUFFIX) xtbsv_RUN.$(SUFFIX) xtbsv_RLU.$(SUFFIX) xtbsv_RLN.$(SUFFIX) \
	xtbsv_CUU.$(SUFFIX) xtbsv_CUN.$(SUFFIX) xtbsv_CLU.$(SUFFIX) xtbsv_CLN.$(SUFFIX) \
	xtpmv_NUU.$(SUFFIX) xtpmv_NUN.$(SUFFIX) xtpmv_NLU.$(SUFFIX) xtpmv_NLN.$(SUFFIX) \
	xtpmv_TUU.$(SUFFIX) xtpmv_TUN.$(SUFFIX) xtpmv_TLU.$(SUFFIX) xtpmv_TLN.$(SUFFIX) \
	xtpmv_RUU.$(SUFFIX) xtpmv_RUN.$(SUFFIX) xtpmv_RLU.$(SUFFIX) xtpmv_RLN.$(SUFFIX) \
	xtpmv_CUU.$(SUFFIX) xtpmv_CUN.$(SUFFIX) xtpmv_CLU.$(SUFFIX) xtpmv_CLN.$(SUFFIX) \
	xtpsv_NUU.$(SUFFIX) xtpsv_NUN.$(SUFFIX) xtpsv_NLU.$(SUFFIX) xtpsv_NLN.$(SUFFIX) \
	xtpsv_TUU.$(SUFFIX) xtpsv_TUN.$(SUFFIX) xtpsv_TLU.$(SUFFIX) xtpsv_TLN.$(SUFFIX) \
	xtpsv_RUU.$(SUFFIX) xtpsv_RUN.$(SUFFIX) xtpsv_RLU.$(SUFFIX) xtpsv_RLN.$(SUFFIX) \
	xtpsv_CUU.$(SUFFIX) xtpsv_CUN.$(SUFFIX) xtpsv_CLU.$(SUFFIX) xtpsv_CLN.$(SUFFIX) \
	xtrmv_NUU.$(SUFFIX) xtrmv_NUN.$(SUFFIX) xtrmv_NLU.$(SUFFIX) xtrmv_NLN.$(SUFFIX) \
	xtrmv_TUU.$(SUFFIX) xtrmv_TUN.$(SUFFIX) xtrmv_TLU.$(SUFFIX) xtrmv_TLN.$(SUFFIX) \
	xtrmv_RUU.$(SUFFIX) xtrmv_RUN.$(SUFFIX) xtrmv_RLU.$(SUFFIX) xtrmv_RLN.$(SUFFIX) \
	xtrmv_CUU.$(SUFFIX) xtrmv_CUN.$(SUFFIX) xtrmv_CLU.$(SUFFIX) xtrmv_CLN.$(SUFFIX) \
	xtrsv_NUU.$(SUFFIX) xtrsv_NUN.$(SUFFIX) xtrsv_NLU.$(SUFFIX) xtrsv_NLN.$(SUFFIX) \
	xtrsv_TUU.$(SUFFIX) xtrsv_TUN.$(SUFFIX) xtrsv_TLU.$(SUFFIX) xtrsv_TLN.$(SUFFIX) \
	xtrsv_RUU.$(SUFFIX) xtrsv_RUN.$(SUFFIX) xtrsv_RLU.$(SUFFIX) xtrsv_RLN.$(SUFFIX) \
	xtrsv_CUU.$(SUFFIX) xtrsv_CUN.$(SUFFIX) xtrsv_CLU.$(SUFFIX) xtrsv_CLN.$(SUFFIX)

HPLOBJS = \
	dtrsv_NLU.$(SUFFIX) dtrsv_NUN.$(SUFFIX) dtrsv_NUU.$(SUFFIX) dtrsv_NLN.$(SUFFIX) \
	dtrsv_TLN.$(SUFFIX) dtrsv_TLU.$(SUFFIX) dtrsv_TUN.$(SUFFIX) dtrsv_TUU.$(SUFFIX)

ifdef SMP
SBLASOBJS   += \
	sgemv_thread_n.$(SUFFIX)	sgemv_thread_t.$(SUFFIX) \
	sger_thread.$(SUFFIX) \
	ssymv_thread_U.$(SUFFIX)	ssymv_thread_L.$(SUFFIX) \
	ssyr_thread_U.$(SUFFIX)		ssyr_thread_L.$(SUFFIX)  \
	ssyr2_thread_U.$(SUFFIX)	ssyr2_thread_L.$(SUFFIX) \
	sspr_thread_U.$(SUFFIX)		sspr_thread_L.$(SUFFIX)  \
	sspr2_thread_U.$(SUFFIX)	sspr2_thread_L.$(SUFFIX) \
	strmv_thread_NUU.$(SUFFIX)	strmv_thread_NUN.$(SUFFIX) \
	strmv_thread_NLU.$(SUFFIX)	strmv_thread_NLN.$(SUFFIX) \
	strmv_thread_TUU.$(SUFFIX)	strmv_thread_TUN.$(SUFFIX) \
	strmv_thread_TLU.$(SUFFIX)	strmv_thread_TLN.$(SUFFIX) \
	sspmv_thread_U.$(SUFFIX)	sspmv_thread_L.$(SUFFIX) \
	stpmv_thread_NUU.$(SUFFIX)	stpmv_thread_NUN.$(SUFFIX) \
	stpmv_thread_NLU.$(SUFFIX)	stpmv_thread_NLN.$(SUFFIX) \
	stpmv_thread_TUU.$(SUFFIX)	stpmv_thread_TUN.$(SUFFIX) \
	stpmv_thread_TLU.$(SUFFIX)	stpmv_thread_TLN.$(SUFFIX) \
	sgbmv_thread_n.$(SUFFIX)	sgbmv_thread_t.$(SUFFIX) \
	ssbmv_thread_U.$(SUFFIX)	ssbmv_thread_L.$(SUFFIX) \
	stbmv_thread_NUU.$(SUFFIX)	stbmv_thread_NUN.$(SUFFIX) \
	stbmv_thread_NLU.$(SUFFIX)	stbmv_thread_NLN.$(SUFFIX) \
	stbmv_thread_TUU.$(SUFFIX)	stbmv_thread_TUN.$(SUFFIX) \
	stbmv_thread_TLU.$(SUFFIX)	stbmv_thread_TLN.$(SUFFIX) \

DBLASOBJS   += \
	dgemv_thread_n.$(SUFFIX)	dgemv_thread_t.$(SUFFIX) \
	dger_thread.$(SUFFIX) \
	dsymv_thread_U.$(SUFFIX)	dsymv_thread_L.$(SUFFIX) \
	dsyr_thread_U.$(SUFFIX)		dsyr_thread_L.$(SUFFIX)  \
	dsyr2_thread_U.$(SUFFIX)	dsyr2_thread_L.$(SUFFIX) \
	dspr_thread_U.$(SUFFIX)		dspr_thread_L.$(SUFFIX)  \
	dspr2_thread_U.$(SUFFIX)	dspr2_thread_L.$(SUFFIX) \
	dtrmv_thread_NUU.$(SUFFIX)	dtrmv_thread_NUN.$(SUFFIX) \
	dtrmv_thread_NLU.$(SUFFIX)	dtrmv_thread_NLN.$(SUFFIX) \
	dtrmv_thread_TUU.$(SUFFIX)	dtrmv_thread_TUN.$(SUFFIX) \
	dtrmv_thread_TLU.$(SUFFIX)	dtrmv_thread_TLN.$(SUFFIX) \
	dspmv_thread_U.$(SUFFIX)	dspmv_thread_L.$(SUFFIX) \
	dtpmv_thread_NUU.$(SUFFIX)	dtpmv_thread_NUN.$(SUFFIX) \
	dtpmv_thread_NLU.$(SUFFIX)	dtpmv_thread_NLN.$(SUFFIX) \
	dtpmv_thread_TUU.$(SUFFIX)	dtpmv_thread_TUN.$(SUFFIX) \
	dtpmv_thread_TLU.$(SUFFIX)	dtpmv_thread_TLN.$(SUFFIX) \
	dgbmv_thread_n.$(SUFFIX)	dgbmv_thread_t.$(SUFFIX) \
	dsbmv_thread_U.$(SUFFIX)	dsbmv_thread_L.$(SUFFIX) \
	dtbmv_thread_NUU.$(SUFFIX)	dtbmv_thread_NUN.$(SUFFIX) \
	dtbmv_thread_NLU.$(SUFFIX)	dtbmv_thread_NLN.$(SUFFIX) \
	dtbmv_thread_TUU.$(SUFFIX)	dtbmv_thread_TUN.$(SUFFIX) \
	dtbmv_thread_TLU.$(SUFFIX)	dtbmv_thread_TLN.$(SUFFIX) \

QBLASOBJS   += \
	qgemv_thread_n.$(SUFFIX)	qgemv_thread_t.$(SUFFIX) \
	qger_thread.$(SUFFIX) \
	qsymv_thread_U.$(SUFFIX)	qsymv_thread_L.$(SUFFIX) \
	qsyr_thread_U.$(SUFFIX)		qsyr_thread_L.$(SUFFIX) \
	qsyr2_thread_U.$(SUFFIX)	qsyr2_thread_L.$(SUFFIX) \
	qspr_thread_U.$(SUFFIX)		qspr_thread_L.$(SUFFIX)  \
	qspr2_thread_U.$(SUFFIX)	qspr2_thread_L.$(SUFFIX) \
	qtrmv_thread_NUU.$(SUFFIX)	qtrmv_thread_NUN.$(SUFFIX) \
	qtrmv_thread_NLU.$(SUFFIX)	qtrmv_thread_NLN.$(SUFFIX) \
	qtrmv_thread_TUU.$(SUFFIX)	qtrmv_thread_TUN.$(SUFFIX) \
	qtrmv_thread_TLU.$(SUFFIX)	qtrmv_thread_TLN.$(SUFFIX) \
	qspmv_thread_U.$(SUFFIX)	qspmv_thread_L.$(SUFFIX) \
	qtpmv_thread_NUU.$(SUFFIX)	qtpmv_thread_NUN.$(SUFFIX) \
	qtpmv_thread_NLU.$(SUFFIX)	qtpmv_thread_NLN.$(SUFFIX) \
	qtpmv_thread_TUU.$(SUFFIX)	qtpmv_thread_TUN.$(SUFFIX) \
	qtpmv_thread_TLU.$(SUFFIX)	qtpmv_thread_TLN.$(SUFFIX) \
	qgbmv_thread_n.$(SUFFIX)	qgbmv_thread_t.$(SUFFIX) \
	qsbmv_thread_U.$(SUFFIX)	qsbmv_thread_L.$(SUFFIX) \
	qtbmv_thread_NUU.$(SUFFIX)	qtbmv_thread_NUN.$(SUFFIX) \
	qtbmv_thread_NLU.$(SUFFIX)	qtbmv_thread_NLN.$(SUFFIX) \
	qtbmv_thread_TUU.$(SUFFIX)	qtbmv_thread_TUN.$(SUFFIX) \
	qtbmv_thread_TLU.$(SUFFIX)	qtbmv_thread_TLN.$(SUFFIX) \

CBLASOBJS   += \
	cgemv_thread_n.$(SUFFIX)	cgemv_thread_t.$(SUFFIX) \
	cgemv_thread_r.$(SUFFIX)	cgemv_thread_c.$(SUFFIX) \
	cgemv_thread_o.$(SUFFIX)	cgemv_thread_u.$(SUFFIX) \
	cgemv_thread_s.$(SUFFIX)	cgemv_thread_d.$(SUFFIX) \
	cger_thread_U.$(SUFFIX)		cger_thread_C.$(SUFFIX) \
	cger_thread_V.$(SUFFIX)		cger_thread_D.$(SUFFIX) \
	csymv_thread_U.$(SUFFIX)	csymv_thread_L.$(SUFFIX) \
	chemv_thread_U.$(SUFFIX)	chemv_thread_L.$(SUFFIX) \
	chemv_thread_V.$(SUFFIX)	chemv_thread_M.$(SUFFIX) \
	csyr_thread_U.$(SUFFIX)	   	csyr_thread_L.$(SUFFIX)	 \
	cher_thread_U.$(SUFFIX)	   	cher_thread_L.$(SUFFIX)  \
	cher_thread_V.$(SUFFIX)		cher_thread_M.$(SUFFIX)  \
	csyr2_thread_U.$(SUFFIX)	csyr2_thread_L.$(SUFFIX) \
	cher2_thread_U.$(SUFFIX)	cher2_thread_L.$(SUFFIX) \
	cher2_thread_V.$(SUFFIX)	cher2_thread_M.$(SUFFIX) \
	cspr_thread_U.$(SUFFIX)		cspr_thread_L.$(SUFFIX)  \
	chpr_thread_U.$(SUFFIX)		chpr_thread_L.$(SUFFIX)  \
	chpr_thread_V.$(SUFFIX)		chpr_thread_M.$(SUFFIX)  \
	cspr2_thread_U.$(SUFFIX)	cspr2_thread_L.$(SUFFIX) \
	chpr2_thread_U.$(SUFFIX)	chpr2_thread_L.$(SUFFIX) \
	chpr2_thread_V.$(SUFFIX)	chpr2_thread_M.$(SUFFIX) \
	ctrmv_thread_NUU.$(SUFFIX)	ctrmv_thread_NUN.$(SUFFIX) \
	ctrmv_thread_NLU.$(SUFFIX)	ctrmv_thread_NLN.$(SUFFIX) \
	ctrmv_thread_TUU.$(SUFFIX)	ctrmv_thread_TUN.$(SUFFIX) \
	ctrmv_thread_TLU.$(SUFFIX)	ctrmv_thread_TLN.$(SUFFIX) \
	ctrmv_thread_RUU.$(SUFFIX)	ctrmv_thread_RUN.$(SUFFIX) \
	ctrmv_thread_RLU.$(SUFFIX)	ctrmv_thread_RLN.$(SUFFIX) \
	ctrmv_thread_CUU.$(SUFFIX)	ctrmv_thread_CUN.$(SUFFIX) \
	ctrmv_thread_CLU.$(SUFFIX)	ctrmv_thread_CLN.$(SUFFIX) \
	cspmv_thread_U.$(SUFFIX)	cspmv_thread_L.$(SUFFIX) \
	chpmv_thread_U.$(SUFFIX)	chpmv_thread_L.$(SUFFIX) \
	chpmv_thread_V.$(SUFFIX)	chpmv_thread_M.$(SUFFIX) \
	ctpmv_thread_NUU.$(SUFFIX)	ctpmv_thread_NUN.$(SUFFIX) \
	ctpmv_thread_NLU.$(SUFFIX)	ctpmv_thread_NLN.$(SUFFIX) \
	ctpmv_thread_TUU.$(SUFFIX)	ctpmv_thread_TUN.$(SUFFIX) \
	ctpmv_thread_TLU.$(SUFFIX)	ctpmv_thread_TLN.$(SUFFIX) \
	ctpmv_thread_RUU.$(SUFFIX)	ctpmv_thread_RUN.$(SUFFIX) \
	ctpmv_thread_RLU.$(SUFFIX)	ctpmv_thread_RLN.$(SUFFIX) \
	ctpmv_thread_CUU.$(SUFFIX)	ctpmv_thread_CUN.$(SUFFIX) \
	ctpmv_thread_CLU.$(SUFFIX)	ctpmv_thread_CLN.$(SUFFIX) \
	cgbmv_thread_n.$(SUFFIX)	cgbmv_thread_t.$(SUFFIX) \
	cgbmv_thread_r.$(SUFFIX)	cgbmv_thread_c.$(SUFFIX) \
	cgbmv_thread_o.$(SUFFIX)	cgbmv_thread_u.$(SUFFIX) \
	cgbmv_thread_s.$(SUFFIX)	cgbmv_thread_d.$(SUFFIX) \
	csbmv_thread_U.$(SUFFIX)	csbmv_thread_L.$(SUFFIX) \
	chbmv_thread_U.$(SUFFIX)	chbmv_thread_L.$(SUFFIX) \
	chbmv_thread_V.$(SUFFIX)	chbmv_thread_M.$(SUFFIX) \
	ctbmv_thread_NUU.$(SUFFIX)	ctbmv_thread_NUN.$(SUFFIX) \
	ctbmv_thread_NLU.$(SUFFIX)	ctbmv_thread_NLN.$(SUFFIX) \
	ctbmv_thread_TUU.$(SUFFIX)	ctbmv_thread_TUN.$(SUFFIX) \
	ctbmv_thread_TLU.$(SUFFIX)	ctbmv_thread_TLN.$(SUFFIX) \
	ctbmv_thread_RUU.$(SUFFIX)	ctbmv_thread_RUN.$(SUFFIX) \
	ctbmv_thread_RLU.$(SUFFIX)	ctbmv_thread_RLN.$(SUFFIX) \
	ctbmv_thread_CUU.$(SUFFIX)	ctbmv_thread_CUN.$(SUFFIX) \
	ctbmv_thread_CLU.$(SUFFIX)	ctbmv_thread_CLN.$(SUFFIX) \


ZBLASOBJS   += \
	zgemv_thread_n.$(SUFFIX)	zgemv_thread_t.$(SUFFIX) \
	zgemv_thread_r.$(SUFFIX)	zgemv_thread_c.$(SUFFIX) \
	zgemv_thread_o.$(SUFFIX)	zgemv_thread_u.$(SUFFIX) \
	zgemv_thread_s.$(SUFFIX)	zgemv_thread_d.$(SUFFIX) \
	zger_thread_U.$(SUFFIX)		zger_thread_C.$(SUFFIX) \
	zger_thread_V.$(SUFFIX)		zger_thread_D.$(SUFFIX) \
	zsymv_thread_U.$(SUFFIX)	zsymv_thread_L.$(SUFFIX) \
	zhemv_thread_U.$(SUFFIX)	zhemv_thread_L.$(SUFFIX) \
	zhemv_thread_V.$(SUFFIX)	zhemv_thread_M.$(SUFFIX) \
	zsyr_thread_U.$(SUFFIX)	   	zsyr_thread_L.$(SUFFIX)	 \
	zher_thread_U.$(SUFFIX)	   	zher_thread_L.$(SUFFIX)  \
	zher_thread_V.$(SUFFIX)		zher_thread_M.$(SUFFIX)  \
	zsyr2_thread_U.$(SUFFIX)	zsyr2_thread_L.$(SUFFIX) \
	zher2_thread_U.$(SUFFIX)	zher2_thread_L.$(SUFFIX) \
	zher2_thread_V.$(SUFFIX)	zher2_thread_M.$(SUFFIX) \
	zspr_thread_U.$(SUFFIX)		zspr_thread_L.$(SUFFIX)  \
	zhpr_thread_U.$(SUFFIX)		zhpr_thread_L.$(SUFFIX)  \
	zhpr_thread_V.$(SUFFIX)		zhpr_thread_M.$(SUFFIX)  \
	zspr2_thread_U.$(SUFFIX)	zspr2_thread_L.$(SUFFIX) \
	zhpr2_thread_U.$(SUFFIX)	zhpr2_thread_L.$(SUFFIX) \
	zhpr2_thread_V.$(SUFFIX)	zhpr2_thread_M.$(SUFFIX) \
	ztrmv_thread_NUU.$(SUFFIX)	ztrmv_thread_NUN.$(SUFFIX) \
	ztrmv_thread_NLU.$(SUFFIX)	ztrmv_thread_NLN.$(SUFFIX) \
	ztrmv_thread_TUU.$(SUFFIX)	ztrmv_thread_TUN.$(SUFFIX) \
	ztrmv_thread_TLU.$(SUFFIX)	ztrmv_thread_TLN.$(SUFFIX) \
	ztrmv_thread_RUU.$(SUFFIX)	ztrmv_thread_RUN.$(SUFFIX) \
	ztrmv_thread_RLU.$(SUFFIX)	ztrmv_thread_RLN.$(SUFFIX) \
	ztrmv_thread_CUU.$(SUFFIX)	ztrmv_thread_CUN.$(SUFFIX) \
	ztrmv_thread_CLU.$(SUFFIX)	ztrmv_thread_CLN.$(SUFFIX) \
	zspmv_thread_U.$(SUFFIX)	zspmv_thread_L.$(SUFFIX) \
	zhpmv_thread_U.$(SUFFIX)	zhpmv_thread_L.$(SUFFIX) \
	zhpmv_thread_V.$(SUFFIX)	zhpmv_thread_M.$(SUFFIX) \
	ztpmv_thread_NUU.$(SUFFIX)	ztpmv_thread_NUN.$(SUFFIX) \
	ztpmv_thread_NLU.$(SUFFIX)	ztpmv_thread_NLN.$(SUFFIX) \
	ztpmv_thread_TUU.$(SUFFIX)	ztpmv_thread_TUN.$(SUFFIX) \
	ztpmv_thread_TLU.$(SUFFIX)	ztpmv_thread_TLN.$(SUFFIX) \
	ztpmv_thread_RUU.$(SUFFIX)	ztpmv_thread_RUN.$(SUFFIX) \
	ztpmv_thread_RLU.$(SUFFIX)	ztpmv_thread_RLN.$(SUFFIX) \
	ztpmv_thread_CUU.$(SUFFIX)	ztpmv_thread_CUN.$(SUFFIX) \
	ztpmv_thread_CLU.$(SUFFIX)	ztpmv_thread_CLN.$(SUFFIX) \
	zgbmv_thread_n.$(SUFFIX)	zgbmv_thread_t.$(SUFFIX) \
	zgbmv_thread_r.$(SUFFIX)	zgbmv_thread_c.$(SUFFIX) \
	zgbmv_thread_o.$(SUFFIX)	zgbmv_thread_u.$(SUFFIX) \
	zgbmv_thread_s.$(SUFFIX)	zgbmv_thread_d.$(SUFFIX) \
	zsbmv_thread_U.$(SUFFIX)	zsbmv_thread_L.$(SUFFIX) \
	zhbmv_thread_U.$(SUFFIX)	zhbmv_thread_L.$(SUFFIX) \
	zhbmv_thread_V.$(SUFFIX)	zhbmv_thread_M.$(SUFFIX) \
	ztbmv_thread_NUU.$(SUFFIX)	ztbmv_thread_NUN.$(SUFFIX) \
	ztbmv_thread_NLU.$(SUFFIX)	ztbmv_thread_NLN.$(SUFFIX) \
	ztbmv_thread_TUU.$(SUFFIX)	ztbmv_thread_TUN.$(SUFFIX) \
	ztbmv_thread_TLU.$(SUFFIX)	ztbmv_thread_TLN.$(SUFFIX) \
	ztbmv_thread_RUU.$(SUFFIX)	ztbmv_thread_RUN.$(SUFFIX) \
	ztbmv_thread_RLU.$(SUFFIX)	ztbmv_thread_RLN.$(SUFFIX) \
	ztbmv_thread_CUU.$(SUFFIX)	ztbmv_thread_CUN.$(SUFFIX) \
	ztbmv_thread_CLU.$(SUFFIX)	ztbmv_thread_CLN.$(SUFFIX) \

XBLASOBJS   += \
	xgemv_thread_n.$(SUFFIX)	xgemv_thread_t.$(SUFFIX) \
	xgemv_thread_r.$(SUFFIX)	xgemv_thread_c.$(SUFFIX) \
	xgemv_thread_o.$(SUFFIX)	xgemv_thread_u.$(SUFFIX) \
	xgemv_thread_s.$(SUFFIX)	xgemv_thread_d.$(SUFFIX) \
	xger_thread_U.$(SUFFIX)		xger_thread_C.$(SUFFIX) \
	xger_thread_V.$(SUFFIX)		xger_thread_D.$(SUFFIX) \
	xsymv_thread_U.$(SUFFIX)	xsymv_thread_L.$(SUFFIX) \
	xhemv_thread_U.$(SUFFIX)	xhemv_thread_L.$(SUFFIX) \
	xhemv_thread_V.$(SUFFIX)	xhemv_thread_M.$(SUFFIX) \
	xsyr_thread_U.$(SUFFIX)	   	xsyr_thread_L.$(SUFFIX)	 \
	xher_thread_U.$(SUFFIX)	   	xher_thread_L.$(SUFFIX)  \
	xher_thread_V.$(SUFFIX)		xher_thread_M.$(SUFFIX)  \
	xsyr2_thread_U.$(SUFFIX)	xsyr2_thread_L.$(SUFFIX) \
	xher2_thread_U.$(SUFFIX)	xher2_thread_L.$(SUFFIX) \
	xher2_thread_V.$(SUFFIX)	xher2_thread_M.$(SUFFIX) \
	xspr_thread_U.$(SUFFIX)		xspr_thread_L.$(SUFFIX)  \
	xhpr_thread_U.$(SUFFIX)		xhpr_thread_L.$(SUFFIX)  \
	xhpr_thread_V.$(SUFFIX)		xhpr_thread_M.$(SUFFIX)  \
	xspr2_thread_U.$(SUFFIX)	xspr2_thread_L.$(SUFFIX) \
	xhpr2_thread_U.$(SUFFIX)	xhpr2_thread_L.$(SUFFIX) \
	xhpr2_thread_V.$(SUFFIX)	xhpr2_thread_M.$(SUFFIX) \
	xtrmv_thread_NUU.$(SUFFIX)	xtrmv_thread_NUN.$(SUFFIX) \
	xtrmv_thread_NLU.$(SUFFIX)	xtrmv_thread_NLN.$(SUFFIX) \
	xtrmv_thread_TUU.$(SUFFIX)	xtrmv_thread_TUN.$(SUFFIX) \
	xtrmv_thread_TLU.$(SUFFIX)	xtrmv_thread_TLN.$(SUFFIX) \
	xtrmv_thread_RUU.$(SUFFIX)	xtrmv_thread_RUN.$(SUFFIX) \
	xtrmv_thread_RLU.$(SUFFIX)	xtrmv_thread_RLN.$(SUFFIX) \
	xtrmv_thread_CUU.$(SUFFIX)	xtrmv_thread_CUN.$(SUFFIX) \
	xtrmv_thread_CLU.$(SUFFIX)	xtrmv_thread_CLN.$(SUFFIX) \
	xspmv_thread_U.$(SUFFIX)	xspmv_thread_L.$(SUFFIX) \
	xhpmv_thread_U.$(SUFFIX)	xhpmv_thread_L.$(SUFFIX) \
	xhpmv_thread_V.$(SUFFIX)	xhpmv_thread_M.$(SUFFIX) \
	xtpmv_thread_NUU.$(SUFFIX)	xtpmv_thread_NUN.$(SUFFIX) \
	xtpmv_thread_NLU.$(SUFFIX)	xtpmv_thread_NLN.$(SUFFIX) \
	xtpmv_thread_TUU.$(SUFFIX)	xtpmv_thread_TUN.$(SUFFIX) \
	xtpmv_thread_TLU.$(SUFFIX)	xtpmv_thread_TLN.$(SUFFIX) \
	xtpmv_thread_RUU.$(SUFFIX)	xtpmv_thread_RUN.$(SUFFIX) \
	xtpmv_thread_RLU.$(SUFFIX)	xtpmv_thread_RLN.$(SUFFIX) \
	xtpmv_thread_CUU.$(SUFFIX)	xtpmv_thread_CUN.$(SUFFIX) \
	xtpmv_thread_CLU.$(SUFFIX)	xtpmv_thread_CLN.$(SUFFIX) \
	xgbmv_thread_n.$(SUFFIX)	xgbmv_thread_t.$(SUFFIX) \
	xgbmv_thread_r.$(SUFFIX)	xgbmv_thread_c.$(SUFFIX) \
	xgbmv_thread_o.$(SUFFIX)	xgbmv_thread_u.$(SUFFIX) \
	xgbmv_thread_s.$(SUFFIX)	xgbmv_thread_d.$(SUFFIX) \
	xsbmv_thread_U.$(SUFFIX)	xsbmv_thread_L.$(SUFFIX) \
	xhbmv_thread_U.$(SUFFIX)	xhbmv_thread_L.$(SUFFIX) \
	xhbmv_thread_V.$(SUFFIX)	xhbmv_thread_M.$(SUFFIX) \
	xtbmv_thread_NUU.$(SUFFIX)	xtbmv_thread_NUN.$(SUFFIX) \
	xtbmv_thread_NLU.$(SUFFIX)	xtbmv_thread_NLN.$(SUFFIX) \
	xtbmv_thread_TUU.$(SUFFIX)	xtbmv_thread_TUN.$(SUFFIX) \
	xtbmv_thread_TLU.$(SUFFIX)	xtbmv_thread_TLN.$(SUFFIX) \
	xtbmv_thread_RUU.$(SUFFIX)	xtbmv_thread_RUN.$(SUFFIX) \
	xtbmv_thread_RLU.$(SUFFIX)	xtbmv_thread_RLN.$(SUFFIX) \
	xtbmv_thread_CUU.$(SUFFIX)	xtbmv_thread_CUN.$(SUFFIX) \
	xtbmv_thread_CLU.$(SUFFIX)	xtbmv_thread_CLN.$(SUFFIX) \

endif

all ::

sgbmv_n.$(SUFFIX)  sgbmv_n.$(PSUFFIX)  : gbmv_k.c
	$(CC) -c -UCOMPLEX -UDOUBLE -UTRANS $(CFLAGS) -o $(@F) $<

sgbmv_t.$(SUFFIX)  sgbmv_t.$(PSUFFIX)  : gbmv_k.c
	$(CC) -c -UCOMPLEX -UDOUBLE -DTRANS $(CFLAGS) -o $(@F) $<

dgbmv_n.$(SUFFIX)  dgbmv_n.$(PSUFFIX)  : gbmv_k.c
	$(CC) -c -UCOMPLEX -DDOUBLE -UTRANS $(CFLAGS) -o $(@F) $<

dgbmv_t.$(SUFFIX)  dgbmv_t.$(PSUFFIX)  : gbmv_k.c
	$(CC) -c -UCOMPLEX -DDOUBLE -DTRANS $(CFLAGS) -o $(@F) $<

qgbmv_n.$(SUFFIX)  qgbmv_n.$(PSUFFIX)  : gbmv_k.c
	$(CC) -c -UCOMPLEX -DXDOUBLE -UTRANS $(CFLAGS) -o $(@F) $<

qgbmv_t.$(SUFFIX)  qgbmv_t.$(PSUFFIX)  : gbmv_k.c
	$(CC) -c -UCOMPLEX -DXDOUBLE -DTRANS $(CFLAGS) -o $(@F) $<

cgbmv_n.$(SUFFIX)  cgbmv_n.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANS -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_t.$(SUFFIX)  cgbmv_t.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANS -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_r.$(SUFFIX)  cgbmv_r.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANS -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_c.$(SUFFIX)  cgbmv_c.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANS -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_o.$(SUFFIX)  cgbmv_o.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANS -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_u.$(SUFFIX)  cgbmv_u.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANS -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_s.$(SUFFIX)  cgbmv_s.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANS -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_d.$(SUFFIX)  cgbmv_d.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANS -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_n.$(SUFFIX)  zgbmv_n.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANS -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_t.$(SUFFIX)  zgbmv_t.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANS -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_r.$(SUFFIX)  zgbmv_r.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANS -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_c.$(SUFFIX)  zgbmv_c.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANS -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_o.$(SUFFIX)  zgbmv_o.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANS -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_u.$(SUFFIX)  zgbmv_u.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANS -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_s.$(SUFFIX)  zgbmv_s.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANS -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_d.$(SUFFIX)  zgbmv_d.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANS -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_n.$(SUFFIX)  xgbmv_n.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANS -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_t.$(SUFFIX)  xgbmv_t.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANS -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_r.$(SUFFIX)  xgbmv_r.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANS -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_c.$(SUFFIX)  xgbmv_c.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANS -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_o.$(SUFFIX)  xgbmv_o.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANS -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_u.$(SUFFIX)  xgbmv_u.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANS -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_s.$(SUFFIX)  xgbmv_s.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANS -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_d.$(SUFFIX)  xgbmv_d.$(PSUFFIX)  : zgbmv_k.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANS -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

sgbmv_thread_n.$(SUFFIX)  sgbmv_thread_n.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -UCOMPLEX -UDOUBLE -UTRANSA $(CFLAGS) -o $(@F) $<

sgbmv_thread_t.$(SUFFIX)  sgbmv_thread_t.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -UCOMPLEX -UDOUBLE -DTRANSA $(CFLAGS) -o $(@F) $<

dgbmv_thread_n.$(SUFFIX)  dgbmv_thread_n.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -UCOMPLEX -DDOUBLE -UTRANSA $(CFLAGS) -o $(@F) $<

dgbmv_thread_t.$(SUFFIX)  dgbmv_thread_t.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -UCOMPLEX -DDOUBLE -DTRANSA $(CFLAGS) -o $(@F) $<

qgbmv_thread_n.$(SUFFIX)  qgbmv_thread_n.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -UCOMPLEX -DXDOUBLE -UTRANSA $(CFLAGS) -o $(@F) $<

qgbmv_thread_t.$(SUFFIX)  qgbmv_thread_t.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -UCOMPLEX -DXDOUBLE -DTRANSA $(CFLAGS) -o $(@F) $<

cgbmv_thread_n.$(SUFFIX)  cgbmv_thread_n.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANSA -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_t.$(SUFFIX)  cgbmv_thread_t.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANSA -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_r.$(SUFFIX)  cgbmv_thread_r.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANSA -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_c.$(SUFFIX)  cgbmv_thread_c.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANSA -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_o.$(SUFFIX)  cgbmv_thread_o.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANSA -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_u.$(SUFFIX)  cgbmv_thread_u.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANSA -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_s.$(SUFFIX)  cgbmv_thread_s.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -UTRANSA -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

cgbmv_thread_d.$(SUFFIX)  cgbmv_thread_d.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -UDOUBLE -DTRANSA -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_n.$(SUFFIX)  zgbmv_thread_n.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANSA -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_t.$(SUFFIX)  zgbmv_thread_t.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANSA -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_r.$(SUFFIX)  zgbmv_thread_r.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANSA -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_c.$(SUFFIX)  zgbmv_thread_c.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANSA -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_o.$(SUFFIX)  zgbmv_thread_o.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANSA -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_u.$(SUFFIX)  zgbmv_thread_u.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANSA -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_s.$(SUFFIX)  zgbmv_thread_s.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -UTRANSA -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

zgbmv_thread_d.$(SUFFIX)  zgbmv_thread_d.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DDOUBLE -DTRANSA -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_n.$(SUFFIX)  xgbmv_thread_n.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANSA -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_t.$(SUFFIX)  xgbmv_thread_t.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANSA -UCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_r.$(SUFFIX)  xgbmv_thread_r.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANSA -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_c.$(SUFFIX)  xgbmv_thread_c.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANSA -DCONJ -UXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_o.$(SUFFIX)  xgbmv_thread_o.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANSA -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_u.$(SUFFIX)  xgbmv_thread_u.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANSA -UCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_s.$(SUFFIX)  xgbmv_thread_s.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -UTRANSA -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

xgbmv_thread_d.$(SUFFIX)  xgbmv_thread_d.$(PSUFFIX)  : gbmv_thread.c
	$(CC) -c -DCOMPLEX -DXDOUBLE -DTRANSA -DCONJ -DXCONJ $(CFLAGS) -o $(@F) $<

sgemv_thread_n.$(SUFFIX) sgemv_thread_n.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE  -UTRANSA -UCONJ -UXCONJ $< -o $(@F)

sgemv_thread_t.$(SUFFIX) sgemv_thread_t.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE  -DTRANSA -UCONJ -UXCONJ $< -o $(@F)

dgemv_thread_n.$(SUFFIX) dgemv_thread_n.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE  -UTRANSA -UCONJ -UXCONJ $< -o $(@F)

dgemv_thread_t.$(SUFFIX) dgemv_thread_t.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE  -DTRANSA -UCONJ -UXCONJ $< -o $(@F)

qgemv_thread_n.$(SUFFIX) qgemv_thread_n.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UCONJ -UXCONJ $< -o $(@F)

qgemv_thread_t.$(SUFFIX) qgemv_thread_t.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UCONJ -UXCONJ $< -o $(@F)

cgemv_thread_n.$(SUFFIX) cgemv_thread_n.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -UTRANSA -UCONJ -UXCONJ $< -o $(@F)

cgemv_thread_t.$(SUFFIX) cgemv_thread_t.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -DTRANSA -UCONJ -UXCONJ $< -o $(@F)

cgemv_thread_r.$(SUFFIX) cgemv_thread_r.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -UTRANSA -DCONJ -UXCONJ $< -o $(@F)

cgemv_thread_c.$(SUFFIX) cgemv_thread_c.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -DTRANSA -DCONJ -UXCONJ $< -o $(@F)

cgemv_thread_o.$(SUFFIX) cgemv_thread_o.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -UTRANSA -UCONJ -DXCONJ $< -o $(@F)

cgemv_thread_u.$(SUFFIX) cgemv_thread_u.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -DTRANSA -UCONJ -DXCONJ $< -o $(@F)

cgemv_thread_s.$(SUFFIX) cgemv_thread_s.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -UTRANSA -DCONJ -DXCONJ $< -o $(@F)

cgemv_thread_d.$(SUFFIX) cgemv_thread_d.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -DTRANSA -DCONJ -DXCONJ $< -o $(@F)

zgemv_thread_n.$(SUFFIX) zgemv_thread_n.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -UTRANSA -UCONJ -UXCONJ $< -o $(@F)

zgemv_thread_t.$(SUFFIX) zgemv_thread_t.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -DTRANSA -UCONJ -UXCONJ $< -o $(@F)

zgemv_thread_r.$(SUFFIX) zgemv_thread_r.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -UTRANSA -DCONJ -UXCONJ $< -o $(@F)

zgemv_thread_c.$(SUFFIX) zgemv_thread_c.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -DTRANSA -DCONJ -UXCONJ $< -o $(@F)

zgemv_thread_o.$(SUFFIX) zgemv_thread_o.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -UTRANSA -UCONJ -DXCONJ $< -o $(@F)

zgemv_thread_u.$(SUFFIX) zgemv_thread_u.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -DTRANSA -UCONJ -DXCONJ $< -o $(@F)

zgemv_thread_s.$(SUFFIX) zgemv_thread_s.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -UTRANSA -DCONJ -DXCONJ $< -o $(@F)

zgemv_thread_d.$(SUFFIX) zgemv_thread_d.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -DTRANSA -DCONJ -DXCONJ $< -o $(@F)

xgemv_thread_n.$(SUFFIX) xgemv_thread_n.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UCONJ -UXCONJ $< -o $(@F)

xgemv_thread_t.$(SUFFIX) xgemv_thread_t.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UCONJ -UXCONJ $< -o $(@F)

xgemv_thread_r.$(SUFFIX) xgemv_thread_r.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DCONJ -UXCONJ $< -o $(@F)

xgemv_thread_c.$(SUFFIX) xgemv_thread_c.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DCONJ -UXCONJ $< -o $(@F)

xgemv_thread_o.$(SUFFIX) xgemv_thread_o.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UCONJ -DXCONJ $< -o $(@F)

xgemv_thread_u.$(SUFFIX) xgemv_thread_u.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UCONJ -DXCONJ $< -o $(@F)

xgemv_thread_s.$(SUFFIX) xgemv_thread_s.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DCONJ -DXCONJ $< -o $(@F)

xgemv_thread_d.$(SUFFIX) xgemv_thread_d.$(PSUFFIX) : gemv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DCONJ -DXCONJ $< -o $(@F)

sger_thread.$(SUFFIX) sger_thread.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE  -UCONJ -UXCONJ $< -o $(@F)

dger_thread.$(SUFFIX) dger_thread.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE  -UCONJ -UXCONJ $< -o $(@F)

qger_thread.$(SUFFIX) qger_thread.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UCONJ -UXCONJ $< -o $(@F)

cger_thread_U.$(SUFFIX) cger_thread_U.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -UCONJ -UXCONJ $< -o $(@F)

cger_thread_C.$(SUFFIX) cger_thread_C.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -DCONJ -UXCONJ $< -o $(@F)

cger_thread_V.$(SUFFIX) cger_thread_V.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -UCONJ -DXCONJ $< -o $(@F)

cger_thread_D.$(SUFFIX) cger_thread_D.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE  -DCONJ -DXCONJ $< -o $(@F)

zger_thread_U.$(SUFFIX) zger_thread_U.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -UCONJ -UXCONJ $< -o $(@F)

zger_thread_C.$(SUFFIX) zger_thread_C.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -DCONJ -UXCONJ $< -o $(@F)

zger_thread_V.$(SUFFIX) zger_thread_V.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -UCONJ -DXCONJ $< -o $(@F)

zger_thread_D.$(SUFFIX) zger_thread_D.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE  -DCONJ -DXCONJ $< -o $(@F)

xger_thread_U.$(SUFFIX) xger_thread_U.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UCONJ -UXCONJ $< -o $(@F)

xger_thread_C.$(SUFFIX) xger_thread_C.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DCONJ -UXCONJ $< -o $(@F)

xger_thread_V.$(SUFFIX) xger_thread_V.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UCONJ -DXCONJ $< -o $(@F)

xger_thread_D.$(SUFFIX) xger_thread_D.$(PSUFFIX) : ger_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DCONJ -DXCONJ $< -o $(@F)

ssymv_thread_U.$(SUFFIX)  ssymv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssymv_thread_L.$(SUFFIX)  ssymv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsymv_thread_U.$(SUFFIX)  dsymv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsymv_thread_L.$(SUFFIX)  dsymv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsymv_thread_U.$(SUFFIX)  qsymv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsymv_thread_L.$(SUFFIX)  qsymv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csymv_thread_U.$(SUFFIX)  csymv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csymv_thread_L.$(SUFFIX)  csymv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsymv_thread_U.$(SUFFIX)  zsymv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsymv_thread_L.$(SUFFIX)  zsymv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsymv_thread_U.$(SUFFIX)  xsymv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsymv_thread_L.$(SUFFIX)  xsymv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

chemv_thread_U.$(SUFFIX)  chemv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMV $< -o $(@F)

chemv_thread_L.$(SUFFIX)  chemv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMV $< -o $(@F)

chemv_thread_V.$(SUFFIX)  chemv_thread_V.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chemv_thread_M.$(SUFFIX)  chemv_thread_M.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhemv_thread_U.$(SUFFIX)  zhemv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMV $< -o $(@F)

zhemv_thread_L.$(SUFFIX)  zhemv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMV $< -o $(@F)

zhemv_thread_V.$(SUFFIX)  zhemv_thread_V.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhemv_thread_M.$(SUFFIX)  zhemv_thread_M.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhemv_thread_U.$(SUFFIX)  xhemv_thread_U.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMV $< -o $(@F)

xhemv_thread_L.$(SUFFIX)  xhemv_thread_L.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMV $< -o $(@F)

xhemv_thread_V.$(SUFFIX)  xhemv_thread_V.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhemv_thread_M.$(SUFFIX)  xhemv_thread_M.$(PSUFFIX)  : symv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

ssyr_thread_U.$(SUFFIX)  ssyr_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssyr_thread_L.$(SUFFIX)  ssyr_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsyr_thread_U.$(SUFFIX)  dsyr_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsyr_thread_L.$(SUFFIX)  dsyr_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsyr_thread_U.$(SUFFIX)  qsyr_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsyr_thread_L.$(SUFFIX)  qsyr_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csyr_thread_U.$(SUFFIX)  csyr_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csyr_thread_L.$(SUFFIX)  csyr_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsyr_thread_U.$(SUFFIX)  zsyr_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsyr_thread_L.$(SUFFIX)  zsyr_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsyr_thread_U.$(SUFFIX)  xsyr_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsyr_thread_L.$(SUFFIX)  xsyr_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cher_thread_U.$(SUFFIX)  cher_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHER $< -o $(@F)

cher_thread_L.$(SUFFIX)  cher_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHER $< -o $(@F)

cher_thread_V.$(SUFFIX)  cher_thread_V.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHERREV $< -o $(@F)

cher_thread_M.$(SUFFIX)  cher_thread_M.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHERREV $< -o $(@F)

zher_thread_U.$(SUFFIX)  zher_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHER $< -o $(@F)

zher_thread_L.$(SUFFIX)  zher_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHER $< -o $(@F)

zher_thread_V.$(SUFFIX)  zher_thread_V.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHERREV $< -o $(@F)

zher_thread_M.$(SUFFIX)  zher_thread_M.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHERREV $< -o $(@F)

xher_thread_U.$(SUFFIX)  xher_thread_U.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHER $< -o $(@F)

xher_thread_L.$(SUFFIX)  xher_thread_L.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHER $< -o $(@F)

xher_thread_V.$(SUFFIX)  xher_thread_V.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHERREV $< -o $(@F)

xher_thread_M.$(SUFFIX)  xher_thread_M.$(PSUFFIX)  : syr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHERREV $< -o $(@F)

ssyr2_thread_U.$(SUFFIX)  ssyr2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssyr2_thread_L.$(SUFFIX)  ssyr2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsyr2_thread_U.$(SUFFIX)  dsyr2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsyr2_thread_L.$(SUFFIX)  dsyr2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsyr2_thread_U.$(SUFFIX)  qsyr2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsyr2_thread_L.$(SUFFIX)  qsyr2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csyr2_thread_U.$(SUFFIX)  csyr2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csyr2_thread_L.$(SUFFIX)  csyr2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsyr2_thread_U.$(SUFFIX)  zsyr2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsyr2_thread_L.$(SUFFIX)  zsyr2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsyr2_thread_U.$(SUFFIX)  xsyr2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsyr2_thread_L.$(SUFFIX)  xsyr2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cher2_thread_U.$(SUFFIX)  cher2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHER $< -o $(@F)

cher2_thread_L.$(SUFFIX)  cher2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHER $< -o $(@F)

cher2_thread_V.$(SUFFIX)  cher2_thread_V.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHERREV $< -o $(@F)

cher2_thread_M.$(SUFFIX)  cher2_thread_M.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHERREV $< -o $(@F)

zher2_thread_U.$(SUFFIX)  zher2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHER $< -o $(@F)

zher2_thread_L.$(SUFFIX)  zher2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHER $< -o $(@F)

zher2_thread_V.$(SUFFIX)  zher2_thread_V.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHERREV $< -o $(@F)

zher2_thread_M.$(SUFFIX)  zher2_thread_M.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHERREV $< -o $(@F)

xher2_thread_U.$(SUFFIX)  xher2_thread_U.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHER $< -o $(@F)

xher2_thread_L.$(SUFFIX)  xher2_thread_L.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHER $< -o $(@F)

xher2_thread_V.$(SUFFIX)  xher2_thread_V.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHERREV $< -o $(@F)

xher2_thread_M.$(SUFFIX)  xher2_thread_M.$(PSUFFIX)  : syr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHERREV $< -o $(@F)

chbmv_U.$(SUFFIX)  chbmv_U.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

chbmv_L.$(SUFFIX)  chbmv_L.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

chbmv_V.$(SUFFIX)  chbmv_V.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chbmv_M.$(SUFFIX)  chbmv_M.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhbmv_U.$(SUFFIX)  zhbmv_U.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zhbmv_L.$(SUFFIX)  zhbmv_L.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

zhbmv_V.$(SUFFIX)  zhbmv_V.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhbmv_M.$(SUFFIX)  zhbmv_M.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhbmv_U.$(SUFFIX)  xhbmv_U.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xhbmv_L.$(SUFFIX)  xhbmv_L.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

xhbmv_V.$(SUFFIX)  xhbmv_V.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhbmv_M.$(SUFFIX)  xhbmv_M.$(PSUFFIX)  : zhbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

chbmv_thread_U.$(SUFFIX)  chbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMV $< -o $(@F)

chbmv_thread_L.$(SUFFIX)  chbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMV $< -o $(@F)

chbmv_thread_V.$(SUFFIX)  chbmv_thread_V.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chbmv_thread_M.$(SUFFIX)  chbmv_thread_M.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhbmv_thread_U.$(SUFFIX)  zhbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMV $< -o $(@F)

zhbmv_thread_L.$(SUFFIX)  zhbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMV $< -o $(@F)

zhbmv_thread_V.$(SUFFIX)  zhbmv_thread_V.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhbmv_thread_M.$(SUFFIX)  zhbmv_thread_M.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhbmv_thread_U.$(SUFFIX)  xhbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMV $< -o $(@F)

xhbmv_thread_L.$(SUFFIX)  xhbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMV $< -o $(@F)

xhbmv_thread_V.$(SUFFIX)  xhbmv_thread_V.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhbmv_thread_M.$(SUFFIX)  xhbmv_thread_M.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

cher_U.$(SUFFIX)  cher_U.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -ULOWER $< -o $(@F)

cher_L.$(SUFFIX)  cher_L.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DLOWER $< -o $(@F)

cher_V.$(SUFFIX)  cher_V.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

cher_M.$(SUFFIX)  cher_M.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zher_U.$(SUFFIX)  zher_U.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -ULOWER $< -o $(@F)

zher_L.$(SUFFIX)  zher_L.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DLOWER $< -o $(@F)

zher_V.$(SUFFIX)  zher_V.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zher_M.$(SUFFIX)  zher_M.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xher_U.$(SUFFIX)  xher_U.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -ULOWER $< -o $(@F)

xher_L.$(SUFFIX)  xher_L.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DLOWER $< -o $(@F)

xher_V.$(SUFFIX)  xher_V.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xher_M.$(SUFFIX)  xher_M.$(PSUFFIX)  : zher_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

cher2_U.$(SUFFIX)  cher2_U.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

cher2_L.$(SUFFIX)  cher2_L.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

cher2_V.$(SUFFIX)  cher2_V.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

cher2_M.$(SUFFIX)  cher2_M.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DHEMVREV $< -o $(@F)

zher2_U.$(SUFFIX)  zher2_U.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

zher2_L.$(SUFFIX)  zher2_L.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

zher2_V.$(SUFFIX)  zher2_V.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

zher2_M.$(SUFFIX)  zher2_M.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DHEMVREV $< -o $(@F)

xher2_U.$(SUFFIX)  xher2_U.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX  -ULOWER $< -o $(@F)

xher2_L.$(SUFFIX)  xher2_L.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

xher2_V.$(SUFFIX)  xher2_V.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER $< -DHEMVREV -o $(@F)

xher2_M.$(SUFFIX)  xher2_M.$(PSUFFIX)  : zher2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -DHEMVREV -o $(@F)

chpmv_U.$(SUFFIX)  chpmv_U.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

chpmv_L.$(SUFFIX)  chpmv_L.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

chpmv_V.$(SUFFIX)  chpmv_V.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chpmv_M.$(SUFFIX)  chpmv_M.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhpmv_U.$(SUFFIX)  zhpmv_U.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zhpmv_L.$(SUFFIX)  zhpmv_L.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

zhpmv_V.$(SUFFIX)  zhpmv_V.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhpmv_M.$(SUFFIX)  zhpmv_M.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhpmv_U.$(SUFFIX)  xhpmv_U.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xhpmv_L.$(SUFFIX)  xhpmv_L.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

xhpmv_V.$(SUFFIX)  xhpmv_V.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhpmv_M.$(SUFFIX)  xhpmv_M.$(PSUFFIX)  : zhpmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

chpmv_thread_U.$(SUFFIX)  chpmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMV $< -o $(@F)

chpmv_thread_L.$(SUFFIX)  chpmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMV $< -o $(@F)

chpmv_thread_V.$(SUFFIX)  chpmv_thread_V.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chpmv_thread_M.$(SUFFIX)  chpmv_thread_M.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhpmv_thread_U.$(SUFFIX)  zhpmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMV $< -o $(@F)

zhpmv_thread_L.$(SUFFIX)  zhpmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMV $< -o $(@F)

zhpmv_thread_V.$(SUFFIX)  zhpmv_thread_V.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhpmv_thread_M.$(SUFFIX)  zhpmv_thread_M.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhpmv_thread_U.$(SUFFIX)  xhpmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMV $< -o $(@F)

xhpmv_thread_L.$(SUFFIX)  xhpmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMV $< -o $(@F)

xhpmv_thread_V.$(SUFFIX)  xhpmv_thread_V.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhpmv_thread_M.$(SUFFIX)  xhpmv_thread_M.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

chpr_U.$(SUFFIX)  chpr_U.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -ULOWER $< -o $(@F)

chpr_L.$(SUFFIX)  chpr_L.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DLOWER $< -o $(@F)

chpr_V.$(SUFFIX)  chpr_V.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chpr_M.$(SUFFIX)  chpr_M.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhpr_U.$(SUFFIX)  zhpr_U.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -ULOWER $< -o $(@F)

zhpr_L.$(SUFFIX)  zhpr_L.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DLOWER $< -o $(@F)

zhpr_V.$(SUFFIX)  zhpr_V.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhpr_M.$(SUFFIX)  zhpr_M.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhpr_U.$(SUFFIX)  xhpr_U.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -ULOWER $< -o $(@F)

xhpr_L.$(SUFFIX)  xhpr_L.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DLOWER $< -o $(@F)

xhpr_V.$(SUFFIX)  xhpr_V.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhpr_M.$(SUFFIX)  xhpr_M.$(PSUFFIX)  : zhpr_k.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

chpr_thread_U.$(SUFFIX)  chpr_thread_U.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -ULOWER -DHEMV $< -o $(@F)

chpr_thread_L.$(SUFFIX)  chpr_thread_L.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DLOWER -DHEMV $< -o $(@F)

chpr_thread_V.$(SUFFIX)  chpr_thread_V.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

chpr_thread_M.$(SUFFIX)  chpr_thread_M.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

zhpr_thread_U.$(SUFFIX)  zhpr_thread_U.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -ULOWER -DHEMV $< -o $(@F)

zhpr_thread_L.$(SUFFIX)  zhpr_thread_L.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DLOWER -DHEMV $< -o $(@F)

zhpr_thread_V.$(SUFFIX)  zhpr_thread_V.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

zhpr_thread_M.$(SUFFIX)  zhpr_thread_M.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

xhpr_thread_U.$(SUFFIX)  xhpr_thread_U.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -ULOWER -DHEMV $< -o $(@F)

xhpr_thread_L.$(SUFFIX)  xhpr_thread_L.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DLOWER -DHEMV $< -o $(@F)

xhpr_thread_V.$(SUFFIX)  xhpr_thread_V.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -ULOWER -DHEMVREV $< -o $(@F)

xhpr_thread_M.$(SUFFIX)  xhpr_thread_M.$(PSUFFIX)  : spr_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DLOWER -DHEMVREV $< -o $(@F)

chpr2_U.$(SUFFIX)  chpr2_U.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

chpr2_L.$(SUFFIX)  chpr2_L.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

chpr2_V.$(SUFFIX)  chpr2_V.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

chpr2_M.$(SUFFIX)  chpr2_M.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DHEMVREV $< -o $(@F)

zhpr2_U.$(SUFFIX)  zhpr2_U.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX  -ULOWER $< -o $(@F)

zhpr2_L.$(SUFFIX)  zhpr2_L.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

zhpr2_V.$(SUFFIX)  zhpr2_V.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

zhpr2_M.$(SUFFIX)  zhpr2_M.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DHEMVREV $< -o $(@F)

xhpr2_U.$(SUFFIX)  xhpr2_U.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX  -ULOWER $< -o $(@F)

xhpr2_L.$(SUFFIX)  xhpr2_L.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

xhpr2_V.$(SUFFIX)  xhpr2_V.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

xhpr2_M.$(SUFFIX)  xhpr2_M.$(PSUFFIX)  : zhpr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -DHEMVREV -o $(@F)

chpr2_thread_U.$(SUFFIX)  chpr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DHEMV $< -o $(@F)

chpr2_thread_L.$(SUFFIX)  chpr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DHEMV $< -o $(@F)

chpr2_thread_V.$(SUFFIX)  chpr2_thread_V.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

chpr2_thread_M.$(SUFFIX)  chpr2_thread_M.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DHEMVREV $< -o $(@F)

zhpr2_thread_U.$(SUFFIX)  zhpr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX  -ULOWER -DHEMV $< -o $(@F)

zhpr2_thread_L.$(SUFFIX)  zhpr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DHEMV $< -o $(@F)

zhpr2_thread_V.$(SUFFIX)  zhpr2_thread_V.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

zhpr2_thread_M.$(SUFFIX)  zhpr2_thread_M.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DHEMVREV $< -o $(@F)

xhpr2_thread_U.$(SUFFIX)  xhpr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX  -ULOWER -DHEMV $< -o $(@F)

xhpr2_thread_L.$(SUFFIX)  xhpr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DHEMV $< -o $(@F)

xhpr2_thread_V.$(SUFFIX)  xhpr2_thread_V.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DHEMVREV $< -o $(@F)

xhpr2_thread_M.$(SUFFIX)  xhpr2_thread_M.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -DHEMVREV -o $(@F)

ssbmv_U.$(SUFFIX)  ssbmv_U.$(PSUFFIX)  : sbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssbmv_L.$(SUFFIX)  ssbmv_L.$(PSUFFIX)  : sbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsbmv_U.$(SUFFIX)  dsbmv_U.$(PSUFFIX)  : sbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsbmv_L.$(SUFFIX)  dsbmv_L.$(PSUFFIX)  : sbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsbmv_U.$(SUFFIX)  qsbmv_U.$(PSUFFIX)  : sbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsbmv_L.$(SUFFIX)  qsbmv_L.$(PSUFFIX)  : sbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csbmv_U.$(SUFFIX)  csbmv_U.$(PSUFFIX)  : zsbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csbmv_L.$(SUFFIX)  csbmv_L.$(PSUFFIX)  : zsbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsbmv_U.$(SUFFIX)  zsbmv_U.$(PSUFFIX)  : zsbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsbmv_L.$(SUFFIX)  zsbmv_L.$(PSUFFIX)  : zsbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsbmv_U.$(SUFFIX)  xsbmv_U.$(PSUFFIX)  : zsbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsbmv_L.$(SUFFIX)  xsbmv_L.$(PSUFFIX)  : zsbmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

ssbmv_thread_U.$(SUFFIX)  ssbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssbmv_thread_L.$(SUFFIX)  ssbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsbmv_thread_U.$(SUFFIX)  dsbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsbmv_thread_L.$(SUFFIX)  dsbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsbmv_thread_U.$(SUFFIX)  qsbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsbmv_thread_L.$(SUFFIX)  qsbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csbmv_thread_U.$(SUFFIX)  csbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csbmv_thread_L.$(SUFFIX)  csbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsbmv_thread_U.$(SUFFIX)  zsbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsbmv_thread_L.$(SUFFIX)  zsbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsbmv_thread_U.$(SUFFIX)  xsbmv_thread_U.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsbmv_thread_L.$(SUFFIX)  xsbmv_thread_L.$(PSUFFIX)  : sbmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

sspmv_U.$(SUFFIX)  sspmv_U.$(PSUFFIX)  : spmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

sspmv_L.$(SUFFIX)  sspmv_L.$(PSUFFIX)  : spmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dspmv_U.$(SUFFIX)  dspmv_U.$(PSUFFIX)  : spmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dspmv_L.$(SUFFIX)  dspmv_L.$(PSUFFIX)  : spmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qspmv_U.$(SUFFIX)  qspmv_U.$(PSUFFIX)  : spmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qspmv_L.$(SUFFIX)  qspmv_L.$(PSUFFIX)  : spmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cspmv_U.$(SUFFIX)  cspmv_U.$(PSUFFIX)  : zspmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

cspmv_L.$(SUFFIX)  cspmv_L.$(PSUFFIX)  : zspmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zspmv_U.$(SUFFIX)  zspmv_U.$(PSUFFIX)  : zspmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zspmv_L.$(SUFFIX)  zspmv_L.$(PSUFFIX)  : zspmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xspmv_U.$(SUFFIX)  xspmv_U.$(PSUFFIX)  : zspmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xspmv_L.$(SUFFIX)  xspmv_L.$(PSUFFIX)  : zspmv_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

sspmv_thread_U.$(SUFFIX)  sspmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

sspmv_thread_L.$(SUFFIX)  sspmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dspmv_thread_U.$(SUFFIX)  dspmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dspmv_thread_L.$(SUFFIX)  dspmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qspmv_thread_U.$(SUFFIX)  qspmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qspmv_thread_L.$(SUFFIX)  qspmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cspmv_thread_U.$(SUFFIX)  cspmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

cspmv_thread_L.$(SUFFIX)  cspmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zspmv_thread_U.$(SUFFIX)  zspmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zspmv_thread_L.$(SUFFIX)  zspmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xspmv_thread_U.$(SUFFIX)  xspmv_thread_U.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xspmv_thread_L.$(SUFFIX)  xspmv_thread_L.$(PSUFFIX)  : spmv_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

sspr_U.$(SUFFIX)  sspr_U.$(PSUFFIX)  : spr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

sspr_L.$(SUFFIX)  sspr_L.$(PSUFFIX)  : spr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dspr_U.$(SUFFIX)  dspr_U.$(PSUFFIX)  : spr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dspr_L.$(SUFFIX)  dspr_L.$(PSUFFIX)  : spr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qspr_U.$(SUFFIX)  qspr_U.$(PSUFFIX)  : spr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qspr_L.$(SUFFIX)  qspr_L.$(PSUFFIX)  : spr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cspr_U.$(SUFFIX)  cspr_U.$(PSUFFIX)  : zspr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

cspr_L.$(SUFFIX)  cspr_L.$(PSUFFIX)  : zspr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zspr_U.$(SUFFIX)  zspr_U.$(PSUFFIX)  : zspr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zspr_L.$(SUFFIX)  zspr_L.$(PSUFFIX)  : zspr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xspr_U.$(SUFFIX)  xspr_U.$(PSUFFIX)  : zspr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xspr_L.$(SUFFIX)  xspr_L.$(PSUFFIX)  : zspr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

sspr_thread_U.$(SUFFIX)  sspr_thread_U.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

sspr_thread_L.$(SUFFIX)  sspr_thread_L.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dspr_thread_U.$(SUFFIX)  dspr_thread_U.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dspr_thread_L.$(SUFFIX)  dspr_thread_L.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qspr_thread_U.$(SUFFIX)  qspr_thread_U.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qspr_thread_L.$(SUFFIX)  qspr_thread_L.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cspr_thread_U.$(SUFFIX)  cspr_thread_U.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

cspr_thread_L.$(SUFFIX)  cspr_thread_L.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zspr_thread_U.$(SUFFIX)  zspr_thread_U.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zspr_thread_L.$(SUFFIX)  zspr_thread_L.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xspr_thread_U.$(SUFFIX)  xspr_thread_U.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xspr_thread_L.$(SUFFIX)  xspr_thread_L.$(PSUFFIX)  : spr_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

sspr2_U.$(SUFFIX)  sspr2_U.$(PSUFFIX)  : spr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

sspr2_L.$(SUFFIX)  sspr2_L.$(PSUFFIX)  : spr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dspr2_U.$(SUFFIX)  dspr2_U.$(PSUFFIX)  : spr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dspr2_L.$(SUFFIX)  dspr2_L.$(PSUFFIX)  : spr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qspr2_U.$(SUFFIX)  qspr2_U.$(PSUFFIX)  : spr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qspr2_L.$(SUFFIX)  qspr2_L.$(PSUFFIX)  : spr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cspr2_U.$(SUFFIX)  cspr2_U.$(PSUFFIX)  : zspr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

cspr2_L.$(SUFFIX)  cspr2_L.$(PSUFFIX)  : zspr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zspr2_U.$(SUFFIX)  zspr2_U.$(PSUFFIX)  : zspr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zspr2_L.$(SUFFIX)  zspr2_L.$(PSUFFIX)  : zspr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xspr2_U.$(SUFFIX)  xspr2_U.$(PSUFFIX)  : zspr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xspr2_L.$(SUFFIX)  xspr2_L.$(PSUFFIX)  : zspr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

sspr2_thread_U.$(SUFFIX)  sspr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

sspr2_thread_L.$(SUFFIX)  sspr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dspr2_thread_U.$(SUFFIX)  dspr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dspr2_thread_L.$(SUFFIX)  dspr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qspr2_thread_U.$(SUFFIX)  qspr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qspr2_thread_L.$(SUFFIX)  qspr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

cspr2_thread_U.$(SUFFIX)  cspr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

cspr2_thread_L.$(SUFFIX)  cspr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zspr2_thread_U.$(SUFFIX)  zspr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zspr2_thread_L.$(SUFFIX)  zspr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xspr2_thread_U.$(SUFFIX)  xspr2_thread_U.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xspr2_thread_L.$(SUFFIX)  xspr2_thread_L.$(PSUFFIX)  : spr2_thread.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

ssyr_U.$(SUFFIX)  ssyr_U.$(PSUFFIX)  : syr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssyr_L.$(SUFFIX)  ssyr_L.$(PSUFFIX)  : syr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsyr_U.$(SUFFIX)  dsyr_U.$(PSUFFIX)  : syr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsyr_L.$(SUFFIX)  dsyr_L.$(PSUFFIX)  : syr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsyr_U.$(SUFFIX)  qsyr_U.$(PSUFFIX)  : syr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsyr_L.$(SUFFIX)  qsyr_L.$(PSUFFIX)  : syr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csyr_U.$(SUFFIX)  csyr_U.$(PSUFFIX)  : zsyr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csyr_L.$(SUFFIX)  csyr_L.$(PSUFFIX)  : zsyr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsyr_U.$(SUFFIX)  zsyr_U.$(PSUFFIX)  : zsyr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsyr_L.$(SUFFIX)  zsyr_L.$(PSUFFIX)  : zsyr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsyr_U.$(SUFFIX)  xsyr_U.$(PSUFFIX)  : zsyr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsyr_L.$(SUFFIX)  xsyr_L.$(PSUFFIX)  : zsyr_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

ssyr2_U.$(SUFFIX)  ssyr2_U.$(PSUFFIX)  : syr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

ssyr2_L.$(SUFFIX)  ssyr2_L.$(PSUFFIX)  : syr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

dsyr2_U.$(SUFFIX)  dsyr2_U.$(PSUFFIX)  : syr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

dsyr2_L.$(SUFFIX)  dsyr2_L.$(PSUFFIX)  : syr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

qsyr2_U.$(SUFFIX)  qsyr2_U.$(PSUFFIX)  : syr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

qsyr2_L.$(SUFFIX)  qsyr2_L.$(PSUFFIX)  : syr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

csyr2_U.$(SUFFIX)  csyr2_U.$(PSUFFIX)  : zsyr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER $< -o $(@F)

csyr2_L.$(SUFFIX)  csyr2_L.$(PSUFFIX)  : zsyr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER $< -o $(@F)

zsyr2_U.$(SUFFIX)  zsyr2_U.$(PSUFFIX)  : zsyr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER $< -o $(@F)

zsyr2_L.$(SUFFIX)  zsyr2_L.$(PSUFFIX)  : zsyr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER $< -o $(@F)

xsyr2_U.$(SUFFIX)  xsyr2_U.$(PSUFFIX)  : zsyr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER $< -o $(@F)

xsyr2_L.$(SUFFIX)  xsyr2_L.$(PSUFFIX)  : zsyr2_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER $< -o $(@F)

stbmv_NUU.$(SUFFIX)  stbmv_NUU.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stbmv_NUN.$(SUFFIX)  stbmv_NUN.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stbmv_TLU.$(SUFFIX)  stbmv_TLU.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stbmv_TLN.$(SUFFIX)  stbmv_TLN.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

stbmv_NLU.$(SUFFIX)  stbmv_NLU.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stbmv_NLN.$(SUFFIX)  stbmv_NLN.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stbmv_TUU.$(SUFFIX)  stbmv_TUU.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stbmv_TUN.$(SUFFIX)  stbmv_TUN.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtbmv_NUU.$(SUFFIX)  dtbmv_NUU.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtbmv_NUN.$(SUFFIX)  dtbmv_NUN.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtbmv_TLU.$(SUFFIX)  dtbmv_TLU.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtbmv_TLN.$(SUFFIX)  dtbmv_TLN.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtbmv_NLU.$(SUFFIX)  dtbmv_NLU.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtbmv_NLN.$(SUFFIX)  dtbmv_NLN.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtbmv_TUU.$(SUFFIX)  dtbmv_TUU.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtbmv_TUN.$(SUFFIX)  dtbmv_TUN.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtbmv_NUU.$(SUFFIX)  qtbmv_NUU.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtbmv_NUN.$(SUFFIX)  qtbmv_NUN.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtbmv_TLU.$(SUFFIX)  qtbmv_TLU.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtbmv_TLN.$(SUFFIX)  qtbmv_TLN.$(PSUFFIX)  : tbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtbmv_NLU.$(SUFFIX)  qtbmv_NLU.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtbmv_NLN.$(SUFFIX)  qtbmv_NLN.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtbmv_TUU.$(SUFFIX)  qtbmv_TUU.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtbmv_TUN.$(SUFFIX)  qtbmv_TUN.$(PSUFFIX)  : tbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

ctbmv_NUU.$(SUFFIX)  ctbmv_NUU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctbmv_NUN.$(SUFFIX)  ctbmv_NUN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctbmv_TLU.$(SUFFIX)  ctbmv_TLU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctbmv_TLN.$(SUFFIX)  ctbmv_TLN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctbmv_RLU.$(SUFFIX)  ctbmv_RLU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctbmv_RLN.$(SUFFIX)  ctbmv_RLN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctbmv_CLU.$(SUFFIX)  ctbmv_CLU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctbmv_CLN.$(SUFFIX)  ctbmv_CLN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ctbmv_NLU.$(SUFFIX)  ctbmv_NLU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctbmv_NLN.$(SUFFIX)  ctbmv_NLN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctbmv_TUU.$(SUFFIX)  ctbmv_TUU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctbmv_TUN.$(SUFFIX)  ctbmv_TUN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctbmv_RUU.$(SUFFIX)  ctbmv_RUU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctbmv_RUN.$(SUFFIX)  ctbmv_RUN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctbmv_CUU.$(SUFFIX)  ctbmv_CUU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctbmv_CUN.$(SUFFIX)  ctbmv_CUN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztbmv_NUU.$(SUFFIX)  ztbmv_NUU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztbmv_NUN.$(SUFFIX)  ztbmv_NUN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztbmv_TLU.$(SUFFIX)  ztbmv_TLU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztbmv_TLN.$(SUFFIX)  ztbmv_TLN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztbmv_RLU.$(SUFFIX)  ztbmv_RLU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztbmv_RLN.$(SUFFIX)  ztbmv_RLN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztbmv_CLU.$(SUFFIX)  ztbmv_CLU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztbmv_CLN.$(SUFFIX)  ztbmv_CLN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztbmv_NLU.$(SUFFIX)  ztbmv_NLU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztbmv_NLN.$(SUFFIX)  ztbmv_NLN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztbmv_TUU.$(SUFFIX)  ztbmv_TUU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztbmv_TUN.$(SUFFIX)  ztbmv_TUN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztbmv_RUU.$(SUFFIX)  ztbmv_RUU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztbmv_RUN.$(SUFFIX)  ztbmv_RUN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztbmv_CUU.$(SUFFIX)  ztbmv_CUU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztbmv_CUN.$(SUFFIX)  ztbmv_CUN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtbmv_NUU.$(SUFFIX)  xtbmv_NUU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtbmv_NUN.$(SUFFIX)  xtbmv_NUN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtbmv_TLU.$(SUFFIX)  xtbmv_TLU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtbmv_TLN.$(SUFFIX)  xtbmv_TLN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtbmv_RLU.$(SUFFIX)  xtbmv_RLU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtbmv_RLN.$(SUFFIX)  xtbmv_RLN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtbmv_CLU.$(SUFFIX)  xtbmv_CLU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtbmv_CLN.$(SUFFIX)  xtbmv_CLN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtbmv_NLU.$(SUFFIX)  xtbmv_NLU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtbmv_NLN.$(SUFFIX)  xtbmv_NLN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtbmv_TUU.$(SUFFIX)  xtbmv_TUU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtbmv_TUN.$(SUFFIX)  xtbmv_TUN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtbmv_RUU.$(SUFFIX)  xtbmv_RUU.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtbmv_RUN.$(SUFFIX)  xtbmv_RUN.$(PSUFFIX)  : ztbmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtbmv_CUU.$(SUFFIX)  xtbmv_CUU.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtbmv_CUN.$(SUFFIX)  xtbmv_CUN.$(PSUFFIX)  : ztbmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

stbmv_thread_NUU.$(SUFFIX)  stbmv_thread_NUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

stbmv_thread_NUN.$(SUFFIX)  stbmv_thread_NUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

stbmv_thread_TLU.$(SUFFIX)  stbmv_thread_TLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

stbmv_thread_TLN.$(SUFFIX)  stbmv_thread_TLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

stbmv_thread_NLU.$(SUFFIX)  stbmv_thread_NLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

stbmv_thread_NLN.$(SUFFIX)  stbmv_thread_NLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

stbmv_thread_TUU.$(SUFFIX)  stbmv_thread_TUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

stbmv_thread_TUN.$(SUFFIX)  stbmv_thread_TUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

dtbmv_thread_NUU.$(SUFFIX)  dtbmv_thread_NUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

dtbmv_thread_NUN.$(SUFFIX)  dtbmv_thread_NUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

dtbmv_thread_TLU.$(SUFFIX)  dtbmv_thread_TLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

dtbmv_thread_TLN.$(SUFFIX)  dtbmv_thread_TLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

dtbmv_thread_NLU.$(SUFFIX)  dtbmv_thread_NLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

dtbmv_thread_NLN.$(SUFFIX)  dtbmv_thread_NLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

dtbmv_thread_TUU.$(SUFFIX)  dtbmv_thread_TUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

dtbmv_thread_TUN.$(SUFFIX)  dtbmv_thread_TUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

qtbmv_thread_NUU.$(SUFFIX)  qtbmv_thread_NUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

qtbmv_thread_NUN.$(SUFFIX)  qtbmv_thread_NUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

qtbmv_thread_TLU.$(SUFFIX)  qtbmv_thread_TLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

qtbmv_thread_TLN.$(SUFFIX)  qtbmv_thread_TLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

qtbmv_thread_NLU.$(SUFFIX)  qtbmv_thread_NLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

qtbmv_thread_NLN.$(SUFFIX)  qtbmv_thread_NLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

qtbmv_thread_TUU.$(SUFFIX)  qtbmv_thread_TUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

qtbmv_thread_TUN.$(SUFFIX)  qtbmv_thread_TUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

ctbmv_thread_NUU.$(SUFFIX)  ctbmv_thread_NUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ctbmv_thread_NUN.$(SUFFIX)  ctbmv_thread_NUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ctbmv_thread_TLU.$(SUFFIX)  ctbmv_thread_TLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ctbmv_thread_TLN.$(SUFFIX)  ctbmv_thread_TLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ctbmv_thread_RLU.$(SUFFIX)  ctbmv_thread_RLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ctbmv_thread_RLN.$(SUFFIX)  ctbmv_thread_RLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ctbmv_thread_CLU.$(SUFFIX)  ctbmv_thread_CLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ctbmv_thread_CLN.$(SUFFIX)  ctbmv_thread_CLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ctbmv_thread_NLU.$(SUFFIX)  ctbmv_thread_NLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ctbmv_thread_NLN.$(SUFFIX)  ctbmv_thread_NLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ctbmv_thread_TUU.$(SUFFIX)  ctbmv_thread_TUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ctbmv_thread_TUN.$(SUFFIX)  ctbmv_thread_TUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ctbmv_thread_RUU.$(SUFFIX)  ctbmv_thread_RUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ctbmv_thread_RUN.$(SUFFIX)  ctbmv_thread_RUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ctbmv_thread_CUU.$(SUFFIX)  ctbmv_thread_CUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ctbmv_thread_CUN.$(SUFFIX)  ctbmv_thread_CUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ztbmv_thread_NUU.$(SUFFIX)  ztbmv_thread_NUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ztbmv_thread_NUN.$(SUFFIX)  ztbmv_thread_NUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ztbmv_thread_TLU.$(SUFFIX)  ztbmv_thread_TLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ztbmv_thread_TLN.$(SUFFIX)  ztbmv_thread_TLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ztbmv_thread_RLU.$(SUFFIX)  ztbmv_thread_RLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ztbmv_thread_RLN.$(SUFFIX)  ztbmv_thread_RLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ztbmv_thread_CLU.$(SUFFIX)  ztbmv_thread_CLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ztbmv_thread_CLN.$(SUFFIX)  ztbmv_thread_CLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ztbmv_thread_NLU.$(SUFFIX)  ztbmv_thread_NLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ztbmv_thread_NLN.$(SUFFIX)  ztbmv_thread_NLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ztbmv_thread_TUU.$(SUFFIX)  ztbmv_thread_TUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ztbmv_thread_TUN.$(SUFFIX)  ztbmv_thread_TUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ztbmv_thread_RUU.$(SUFFIX)  ztbmv_thread_RUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ztbmv_thread_RUN.$(SUFFIX)  ztbmv_thread_RUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ztbmv_thread_CUU.$(SUFFIX)  ztbmv_thread_CUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ztbmv_thread_CUN.$(SUFFIX)  ztbmv_thread_CUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

xtbmv_thread_NUU.$(SUFFIX)  xtbmv_thread_NUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

xtbmv_thread_NUN.$(SUFFIX)  xtbmv_thread_NUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

xtbmv_thread_TLU.$(SUFFIX)  xtbmv_thread_TLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

xtbmv_thread_TLN.$(SUFFIX)  xtbmv_thread_TLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

xtbmv_thread_RLU.$(SUFFIX)  xtbmv_thread_RLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

xtbmv_thread_RLN.$(SUFFIX)  xtbmv_thread_RLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

xtbmv_thread_CLU.$(SUFFIX)  xtbmv_thread_CLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

xtbmv_thread_CLN.$(SUFFIX)  xtbmv_thread_CLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

xtbmv_thread_NLU.$(SUFFIX)  xtbmv_thread_NLU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

xtbmv_thread_NLN.$(SUFFIX)  xtbmv_thread_NLN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

xtbmv_thread_TUU.$(SUFFIX)  xtbmv_thread_TUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

xtbmv_thread_TUN.$(SUFFIX)  xtbmv_thread_TUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

xtbmv_thread_RUU.$(SUFFIX)  xtbmv_thread_RUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

xtbmv_thread_RUN.$(SUFFIX)  xtbmv_thread_RUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

xtbmv_thread_CUU.$(SUFFIX)  xtbmv_thread_CUU.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

xtbmv_thread_CUN.$(SUFFIX)  xtbmv_thread_CUN.$(PSUFFIX)  : tbmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

stbsv_NUU.$(SUFFIX)  stbsv_NUU.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stbsv_NUN.$(SUFFIX)  stbsv_NUN.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stbsv_TLU.$(SUFFIX)  stbsv_TLU.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stbsv_TLN.$(SUFFIX)  stbsv_TLN.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

stbsv_NLU.$(SUFFIX)  stbsv_NLU.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stbsv_NLN.$(SUFFIX)  stbsv_NLN.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stbsv_TUU.$(SUFFIX)  stbsv_TUU.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stbsv_TUN.$(SUFFIX)  stbsv_TUN.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtbsv_NUU.$(SUFFIX)  dtbsv_NUU.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtbsv_NUN.$(SUFFIX)  dtbsv_NUN.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtbsv_TLU.$(SUFFIX)  dtbsv_TLU.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtbsv_TLN.$(SUFFIX)  dtbsv_TLN.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtbsv_NLU.$(SUFFIX)  dtbsv_NLU.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtbsv_NLN.$(SUFFIX)  dtbsv_NLN.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtbsv_TUU.$(SUFFIX)  dtbsv_TUU.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtbsv_TUN.$(SUFFIX)  dtbsv_TUN.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtbsv_NUU.$(SUFFIX)  qtbsv_NUU.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtbsv_NUN.$(SUFFIX)  qtbsv_NUN.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtbsv_TLU.$(SUFFIX)  qtbsv_TLU.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtbsv_TLN.$(SUFFIX)  qtbsv_TLN.$(PSUFFIX)  : tbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtbsv_NLU.$(SUFFIX)  qtbsv_NLU.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtbsv_NLN.$(SUFFIX)  qtbsv_NLN.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtbsv_TUU.$(SUFFIX)  qtbsv_TUU.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtbsv_TUN.$(SUFFIX)  qtbsv_TUN.$(PSUFFIX)  : tbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

ctbsv_NUU.$(SUFFIX)  ctbsv_NUU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctbsv_NUN.$(SUFFIX)  ctbsv_NUN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctbsv_TLU.$(SUFFIX)  ctbsv_TLU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctbsv_TLN.$(SUFFIX)  ctbsv_TLN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctbsv_RLU.$(SUFFIX)  ctbsv_RLU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctbsv_RLN.$(SUFFIX)  ctbsv_RLN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctbsv_CLU.$(SUFFIX)  ctbsv_CLU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctbsv_CLN.$(SUFFIX)  ctbsv_CLN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ctbsv_NLU.$(SUFFIX)  ctbsv_NLU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctbsv_NLN.$(SUFFIX)  ctbsv_NLN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctbsv_TUU.$(SUFFIX)  ctbsv_TUU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctbsv_TUN.$(SUFFIX)  ctbsv_TUN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctbsv_RUU.$(SUFFIX)  ctbsv_RUU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctbsv_RUN.$(SUFFIX)  ctbsv_RUN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctbsv_CUU.$(SUFFIX)  ctbsv_CUU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctbsv_CUN.$(SUFFIX)  ctbsv_CUN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztbsv_NUU.$(SUFFIX)  ztbsv_NUU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztbsv_NUN.$(SUFFIX)  ztbsv_NUN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztbsv_TLU.$(SUFFIX)  ztbsv_TLU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztbsv_TLN.$(SUFFIX)  ztbsv_TLN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztbsv_RLU.$(SUFFIX)  ztbsv_RLU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztbsv_RLN.$(SUFFIX)  ztbsv_RLN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztbsv_CLU.$(SUFFIX)  ztbsv_CLU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztbsv_CLN.$(SUFFIX)  ztbsv_CLN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztbsv_NLU.$(SUFFIX)  ztbsv_NLU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztbsv_NLN.$(SUFFIX)  ztbsv_NLN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztbsv_TUU.$(SUFFIX)  ztbsv_TUU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztbsv_TUN.$(SUFFIX)  ztbsv_TUN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztbsv_RUU.$(SUFFIX)  ztbsv_RUU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztbsv_RUN.$(SUFFIX)  ztbsv_RUN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztbsv_CUU.$(SUFFIX)  ztbsv_CUU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztbsv_CUN.$(SUFFIX)  ztbsv_CUN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtbsv_NUU.$(SUFFIX)  xtbsv_NUU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtbsv_NUN.$(SUFFIX)  xtbsv_NUN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtbsv_TLU.$(SUFFIX)  xtbsv_TLU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtbsv_TLN.$(SUFFIX)  xtbsv_TLN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtbsv_RLU.$(SUFFIX)  xtbsv_RLU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtbsv_RLN.$(SUFFIX)  xtbsv_RLN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtbsv_CLU.$(SUFFIX)  xtbsv_CLU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtbsv_CLN.$(SUFFIX)  xtbsv_CLN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtbsv_NLU.$(SUFFIX)  xtbsv_NLU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtbsv_NLN.$(SUFFIX)  xtbsv_NLN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtbsv_TUU.$(SUFFIX)  xtbsv_TUU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtbsv_TUN.$(SUFFIX)  xtbsv_TUN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtbsv_RUU.$(SUFFIX)  xtbsv_RUU.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtbsv_RUN.$(SUFFIX)  xtbsv_RUN.$(PSUFFIX)  : ztbsv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtbsv_CUU.$(SUFFIX)  xtbsv_CUU.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtbsv_CUN.$(SUFFIX)  xtbsv_CUN.$(PSUFFIX)  : ztbsv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

stpmv_NUU.$(SUFFIX)  stpmv_NUU.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stpmv_NUN.$(SUFFIX)  stpmv_NUN.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stpmv_TLU.$(SUFFIX)  stpmv_TLU.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stpmv_TLN.$(SUFFIX)  stpmv_TLN.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

stpmv_NLU.$(SUFFIX)  stpmv_NLU.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stpmv_NLN.$(SUFFIX)  stpmv_NLN.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stpmv_TUU.$(SUFFIX)  stpmv_TUU.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stpmv_TUN.$(SUFFIX)  stpmv_TUN.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtpmv_NUU.$(SUFFIX)  dtpmv_NUU.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtpmv_NUN.$(SUFFIX)  dtpmv_NUN.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtpmv_TLU.$(SUFFIX)  dtpmv_TLU.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtpmv_TLN.$(SUFFIX)  dtpmv_TLN.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtpmv_NLU.$(SUFFIX)  dtpmv_NLU.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtpmv_NLN.$(SUFFIX)  dtpmv_NLN.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtpmv_TUU.$(SUFFIX)  dtpmv_TUU.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtpmv_TUN.$(SUFFIX)  dtpmv_TUN.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtpmv_NUU.$(SUFFIX)  qtpmv_NUU.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtpmv_NUN.$(SUFFIX)  qtpmv_NUN.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtpmv_TLU.$(SUFFIX)  qtpmv_TLU.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtpmv_TLN.$(SUFFIX)  qtpmv_TLN.$(PSUFFIX)  : tpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtpmv_NLU.$(SUFFIX)  qtpmv_NLU.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtpmv_NLN.$(SUFFIX)  qtpmv_NLN.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtpmv_TUU.$(SUFFIX)  qtpmv_TUU.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtpmv_TUN.$(SUFFIX)  qtpmv_TUN.$(PSUFFIX)  : tpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

ctpmv_NUU.$(SUFFIX)  ctpmv_NUU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctpmv_NUN.$(SUFFIX)  ctpmv_NUN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctpmv_TLU.$(SUFFIX)  ctpmv_TLU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctpmv_TLN.$(SUFFIX)  ctpmv_TLN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctpmv_RLU.$(SUFFIX)  ctpmv_RLU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctpmv_RLN.$(SUFFIX)  ctpmv_RLN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctpmv_CLU.$(SUFFIX)  ctpmv_CLU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctpmv_CLN.$(SUFFIX)  ctpmv_CLN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ctpmv_NLU.$(SUFFIX)  ctpmv_NLU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctpmv_NLN.$(SUFFIX)  ctpmv_NLN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctpmv_TUU.$(SUFFIX)  ctpmv_TUU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctpmv_TUN.$(SUFFIX)  ctpmv_TUN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctpmv_RUU.$(SUFFIX)  ctpmv_RUU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctpmv_RUN.$(SUFFIX)  ctpmv_RUN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctpmv_CUU.$(SUFFIX)  ctpmv_CUU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctpmv_CUN.$(SUFFIX)  ctpmv_CUN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztpmv_NUU.$(SUFFIX)  ztpmv_NUU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztpmv_NUN.$(SUFFIX)  ztpmv_NUN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztpmv_TLU.$(SUFFIX)  ztpmv_TLU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztpmv_TLN.$(SUFFIX)  ztpmv_TLN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztpmv_RLU.$(SUFFIX)  ztpmv_RLU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztpmv_RLN.$(SUFFIX)  ztpmv_RLN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztpmv_CLU.$(SUFFIX)  ztpmv_CLU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztpmv_CLN.$(SUFFIX)  ztpmv_CLN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztpmv_NLU.$(SUFFIX)  ztpmv_NLU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztpmv_NLN.$(SUFFIX)  ztpmv_NLN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztpmv_TUU.$(SUFFIX)  ztpmv_TUU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztpmv_TUN.$(SUFFIX)  ztpmv_TUN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztpmv_RUU.$(SUFFIX)  ztpmv_RUU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztpmv_RUN.$(SUFFIX)  ztpmv_RUN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztpmv_CUU.$(SUFFIX)  ztpmv_CUU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztpmv_CUN.$(SUFFIX)  ztpmv_CUN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtpmv_NUU.$(SUFFIX)  xtpmv_NUU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtpmv_NUN.$(SUFFIX)  xtpmv_NUN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtpmv_TLU.$(SUFFIX)  xtpmv_TLU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtpmv_TLN.$(SUFFIX)  xtpmv_TLN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtpmv_RLU.$(SUFFIX)  xtpmv_RLU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtpmv_RLN.$(SUFFIX)  xtpmv_RLN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtpmv_CLU.$(SUFFIX)  xtpmv_CLU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtpmv_CLN.$(SUFFIX)  xtpmv_CLN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtpmv_NLU.$(SUFFIX)  xtpmv_NLU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtpmv_NLN.$(SUFFIX)  xtpmv_NLN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtpmv_TUU.$(SUFFIX)  xtpmv_TUU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtpmv_TUN.$(SUFFIX)  xtpmv_TUN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtpmv_RUU.$(SUFFIX)  xtpmv_RUU.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtpmv_RUN.$(SUFFIX)  xtpmv_RUN.$(PSUFFIX)  : ztpmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtpmv_CUU.$(SUFFIX)  xtpmv_CUU.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtpmv_CUN.$(SUFFIX)  xtpmv_CUN.$(PSUFFIX)  : ztpmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)


stpmv_thread_NUU.$(SUFFIX)  stpmv_thread_NUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

stpmv_thread_NUN.$(SUFFIX)  stpmv_thread_NUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

stpmv_thread_TLU.$(SUFFIX)  stpmv_thread_TLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

stpmv_thread_TLN.$(SUFFIX)  stpmv_thread_TLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

stpmv_thread_NLU.$(SUFFIX)  stpmv_thread_NLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

stpmv_thread_NLN.$(SUFFIX)  stpmv_thread_NLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

stpmv_thread_TUU.$(SUFFIX)  stpmv_thread_TUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

stpmv_thread_TUN.$(SUFFIX)  stpmv_thread_TUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

dtpmv_thread_NUU.$(SUFFIX)  dtpmv_thread_NUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

dtpmv_thread_NUN.$(SUFFIX)  dtpmv_thread_NUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

dtpmv_thread_TLU.$(SUFFIX)  dtpmv_thread_TLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

dtpmv_thread_TLN.$(SUFFIX)  dtpmv_thread_TLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

dtpmv_thread_NLU.$(SUFFIX)  dtpmv_thread_NLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

dtpmv_thread_NLN.$(SUFFIX)  dtpmv_thread_NLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

dtpmv_thread_TUU.$(SUFFIX)  dtpmv_thread_TUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

dtpmv_thread_TUN.$(SUFFIX)  dtpmv_thread_TUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

qtpmv_thread_NUU.$(SUFFIX)  qtpmv_thread_NUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

qtpmv_thread_NUN.$(SUFFIX)  qtpmv_thread_NUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

qtpmv_thread_TLU.$(SUFFIX)  qtpmv_thread_TLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

qtpmv_thread_TLN.$(SUFFIX)  qtpmv_thread_TLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

qtpmv_thread_NLU.$(SUFFIX)  qtpmv_thread_NLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

qtpmv_thread_NLN.$(SUFFIX)  qtpmv_thread_NLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

qtpmv_thread_TUU.$(SUFFIX)  qtpmv_thread_TUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

qtpmv_thread_TUN.$(SUFFIX)  qtpmv_thread_TUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

ctpmv_thread_NUU.$(SUFFIX)  ctpmv_thread_NUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ctpmv_thread_NUN.$(SUFFIX)  ctpmv_thread_NUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ctpmv_thread_TLU.$(SUFFIX)  ctpmv_thread_TLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ctpmv_thread_TLN.$(SUFFIX)  ctpmv_thread_TLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ctpmv_thread_RLU.$(SUFFIX)  ctpmv_thread_RLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ctpmv_thread_RLN.$(SUFFIX)  ctpmv_thread_RLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ctpmv_thread_CLU.$(SUFFIX)  ctpmv_thread_CLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ctpmv_thread_CLN.$(SUFFIX)  ctpmv_thread_CLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ctpmv_thread_NLU.$(SUFFIX)  ctpmv_thread_NLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ctpmv_thread_NLN.$(SUFFIX)  ctpmv_thread_NLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ctpmv_thread_TUU.$(SUFFIX)  ctpmv_thread_TUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ctpmv_thread_TUN.$(SUFFIX)  ctpmv_thread_TUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ctpmv_thread_RUU.$(SUFFIX)  ctpmv_thread_RUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ctpmv_thread_RUN.$(SUFFIX)  ctpmv_thread_RUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ctpmv_thread_CUU.$(SUFFIX)  ctpmv_thread_CUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ctpmv_thread_CUN.$(SUFFIX)  ctpmv_thread_CUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ztpmv_thread_NUU.$(SUFFIX)  ztpmv_thread_NUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ztpmv_thread_NUN.$(SUFFIX)  ztpmv_thread_NUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ztpmv_thread_TLU.$(SUFFIX)  ztpmv_thread_TLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ztpmv_thread_TLN.$(SUFFIX)  ztpmv_thread_TLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ztpmv_thread_RLU.$(SUFFIX)  ztpmv_thread_RLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ztpmv_thread_RLN.$(SUFFIX)  ztpmv_thread_RLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ztpmv_thread_CLU.$(SUFFIX)  ztpmv_thread_CLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ztpmv_thread_CLN.$(SUFFIX)  ztpmv_thread_CLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ztpmv_thread_NLU.$(SUFFIX)  ztpmv_thread_NLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ztpmv_thread_NLN.$(SUFFIX)  ztpmv_thread_NLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ztpmv_thread_TUU.$(SUFFIX)  ztpmv_thread_TUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ztpmv_thread_TUN.$(SUFFIX)  ztpmv_thread_TUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ztpmv_thread_RUU.$(SUFFIX)  ztpmv_thread_RUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ztpmv_thread_RUN.$(SUFFIX)  ztpmv_thread_RUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ztpmv_thread_CUU.$(SUFFIX)  ztpmv_thread_CUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ztpmv_thread_CUN.$(SUFFIX)  ztpmv_thread_CUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

xtpmv_thread_NUU.$(SUFFIX)  xtpmv_thread_NUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

xtpmv_thread_NUN.$(SUFFIX)  xtpmv_thread_NUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

xtpmv_thread_TLU.$(SUFFIX)  xtpmv_thread_TLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

xtpmv_thread_TLN.$(SUFFIX)  xtpmv_thread_TLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

xtpmv_thread_RLU.$(SUFFIX)  xtpmv_thread_RLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

xtpmv_thread_RLN.$(SUFFIX)  xtpmv_thread_RLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

xtpmv_thread_CLU.$(SUFFIX)  xtpmv_thread_CLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

xtpmv_thread_CLN.$(SUFFIX)  xtpmv_thread_CLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

xtpmv_thread_NLU.$(SUFFIX)  xtpmv_thread_NLU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

xtpmv_thread_NLN.$(SUFFIX)  xtpmv_thread_NLN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

xtpmv_thread_TUU.$(SUFFIX)  xtpmv_thread_TUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

xtpmv_thread_TUN.$(SUFFIX)  xtpmv_thread_TUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

xtpmv_thread_RUU.$(SUFFIX)  xtpmv_thread_RUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

xtpmv_thread_RUN.$(SUFFIX)  xtpmv_thread_RUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

xtpmv_thread_CUU.$(SUFFIX)  xtpmv_thread_CUU.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

xtpmv_thread_CUN.$(SUFFIX)  xtpmv_thread_CUN.$(PSUFFIX)  : tpmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

stpsv_NUU.$(SUFFIX)  stpsv_NUU.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stpsv_NUN.$(SUFFIX)  stpsv_NUN.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stpsv_TLU.$(SUFFIX)  stpsv_TLU.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stpsv_TLN.$(SUFFIX)  stpsv_TLN.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

stpsv_NLU.$(SUFFIX)  stpsv_NLU.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

stpsv_NLN.$(SUFFIX)  stpsv_NLN.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

stpsv_TUU.$(SUFFIX)  stpsv_TUU.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

stpsv_TUN.$(SUFFIX)  stpsv_TUN.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtpsv_NUU.$(SUFFIX)  dtpsv_NUU.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtpsv_NUN.$(SUFFIX)  dtpsv_NUN.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtpsv_TLU.$(SUFFIX)  dtpsv_TLU.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtpsv_TLN.$(SUFFIX)  dtpsv_TLN.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtpsv_NLU.$(SUFFIX)  dtpsv_NLU.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtpsv_NLN.$(SUFFIX)  dtpsv_NLN.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtpsv_TUU.$(SUFFIX)  dtpsv_TUU.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtpsv_TUN.$(SUFFIX)  dtpsv_TUN.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtpsv_NUU.$(SUFFIX)  qtpsv_NUU.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtpsv_NUN.$(SUFFIX)  qtpsv_NUN.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtpsv_TLU.$(SUFFIX)  qtpsv_TLU.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtpsv_TLN.$(SUFFIX)  qtpsv_TLN.$(PSUFFIX)  : tpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtpsv_NLU.$(SUFFIX)  qtpsv_NLU.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtpsv_NLN.$(SUFFIX)  qtpsv_NLN.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtpsv_TUU.$(SUFFIX)  qtpsv_TUU.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtpsv_TUN.$(SUFFIX)  qtpsv_TUN.$(PSUFFIX)  : tpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

ctpsv_NUU.$(SUFFIX)  ctpsv_NUU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ctpsv_NUN.$(SUFFIX)  ctpsv_NUN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ctpsv_TLU.$(SUFFIX)  ctpsv_TLU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ctpsv_TLN.$(SUFFIX)  ctpsv_TLN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ctpsv_RLU.$(SUFFIX)  ctpsv_RLU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ctpsv_RLN.$(SUFFIX)  ctpsv_RLN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ctpsv_CLU.$(SUFFIX)  ctpsv_CLU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ctpsv_CLN.$(SUFFIX)  ctpsv_CLN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

ctpsv_NLU.$(SUFFIX)  ctpsv_NLU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ctpsv_NLN.$(SUFFIX)  ctpsv_NLN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ctpsv_TUU.$(SUFFIX)  ctpsv_TUU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ctpsv_TUN.$(SUFFIX)  ctpsv_TUN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ctpsv_RUU.$(SUFFIX)  ctpsv_RUU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ctpsv_RUN.$(SUFFIX)  ctpsv_RUN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ctpsv_CUU.$(SUFFIX)  ctpsv_CUU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ctpsv_CUN.$(SUFFIX)  ctpsv_CUN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

ztpsv_NUU.$(SUFFIX)  ztpsv_NUU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ztpsv_NUN.$(SUFFIX)  ztpsv_NUN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ztpsv_TLU.$(SUFFIX)  ztpsv_TLU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ztpsv_TLN.$(SUFFIX)  ztpsv_TLN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ztpsv_RLU.$(SUFFIX)  ztpsv_RLU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ztpsv_RLN.$(SUFFIX)  ztpsv_RLN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ztpsv_CLU.$(SUFFIX)  ztpsv_CLU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ztpsv_CLN.$(SUFFIX)  ztpsv_CLN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

ztpsv_NLU.$(SUFFIX)  ztpsv_NLU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ztpsv_NLN.$(SUFFIX)  ztpsv_NLN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ztpsv_TUU.$(SUFFIX)  ztpsv_TUU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ztpsv_TUN.$(SUFFIX)  ztpsv_TUN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ztpsv_RUU.$(SUFFIX)  ztpsv_RUU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ztpsv_RUN.$(SUFFIX)  ztpsv_RUN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ztpsv_CUU.$(SUFFIX)  ztpsv_CUU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ztpsv_CUN.$(SUFFIX)  ztpsv_CUN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

xtpsv_NUU.$(SUFFIX)  xtpsv_NUU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

xtpsv_NUN.$(SUFFIX)  xtpsv_NUN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

xtpsv_TLU.$(SUFFIX)  xtpsv_TLU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

xtpsv_TLN.$(SUFFIX)  xtpsv_TLN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

xtpsv_RLU.$(SUFFIX)  xtpsv_RLU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

xtpsv_RLN.$(SUFFIX)  xtpsv_RLN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

xtpsv_CLU.$(SUFFIX)  xtpsv_CLU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

xtpsv_CLN.$(SUFFIX)  xtpsv_CLN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

xtpsv_NLU.$(SUFFIX)  xtpsv_NLU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

xtpsv_NLN.$(SUFFIX)  xtpsv_NLN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

xtpsv_TUU.$(SUFFIX)  xtpsv_TUU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

xtpsv_TUN.$(SUFFIX)  xtpsv_TUN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

xtpsv_RUU.$(SUFFIX)  xtpsv_RUU.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

xtpsv_RUN.$(SUFFIX)  xtpsv_RUN.$(PSUFFIX)  : ztpsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

xtpsv_CUU.$(SUFFIX)  xtpsv_CUU.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

xtpsv_CUN.$(SUFFIX)  xtpsv_CUN.$(PSUFFIX)  : ztpsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

strmv_NUU.$(SUFFIX)  strmv_NUU.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

strmv_NUN.$(SUFFIX)  strmv_NUN.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

strmv_TLU.$(SUFFIX)  strmv_TLU.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

strmv_TLN.$(SUFFIX)  strmv_TLN.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

strmv_NLU.$(SUFFIX)  strmv_NLU.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

strmv_NLN.$(SUFFIX)  strmv_NLN.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

strmv_TUU.$(SUFFIX)  strmv_TUU.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

strmv_TUN.$(SUFFIX)  strmv_TUN.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtrmv_NUU.$(SUFFIX)  dtrmv_NUU.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtrmv_NUN.$(SUFFIX)  dtrmv_NUN.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtrmv_TLU.$(SUFFIX)  dtrmv_TLU.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtrmv_TLN.$(SUFFIX)  dtrmv_TLN.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtrmv_NLU.$(SUFFIX)  dtrmv_NLU.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtrmv_NLN.$(SUFFIX)  dtrmv_NLN.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtrmv_TUU.$(SUFFIX)  dtrmv_TUU.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtrmv_TUN.$(SUFFIX)  dtrmv_TUN.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtrmv_NUU.$(SUFFIX)  qtrmv_NUU.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtrmv_NUN.$(SUFFIX)  qtrmv_NUN.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtrmv_TLU.$(SUFFIX)  qtrmv_TLU.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtrmv_TLN.$(SUFFIX)  qtrmv_TLN.$(PSUFFIX)  : trmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtrmv_NLU.$(SUFFIX)  qtrmv_NLU.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtrmv_NLN.$(SUFFIX)  qtrmv_NLN.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtrmv_TUU.$(SUFFIX)  qtrmv_TUU.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtrmv_TUN.$(SUFFIX)  qtrmv_TUN.$(PSUFFIX)  : trmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

ctrmv_NUU.$(SUFFIX)  ctrmv_NUU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctrmv_NUN.$(SUFFIX)  ctrmv_NUN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctrmv_TLU.$(SUFFIX)  ctrmv_TLU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctrmv_TLN.$(SUFFIX)  ctrmv_TLN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctrmv_RLU.$(SUFFIX)  ctrmv_RLU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctrmv_RLN.$(SUFFIX)  ctrmv_RLN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctrmv_CLU.$(SUFFIX)  ctrmv_CLU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctrmv_CLN.$(SUFFIX)  ctrmv_CLN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ctrmv_NLU.$(SUFFIX)  ctrmv_NLU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ctrmv_NLN.$(SUFFIX)  ctrmv_NLN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ctrmv_TUU.$(SUFFIX)  ctrmv_TUU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ctrmv_TUN.$(SUFFIX)  ctrmv_TUN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ctrmv_RUU.$(SUFFIX)  ctrmv_RUU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ctrmv_RUN.$(SUFFIX)  ctrmv_RUN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ctrmv_CUU.$(SUFFIX)  ctrmv_CUU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ctrmv_CUN.$(SUFFIX)  ctrmv_CUN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztrmv_NUU.$(SUFFIX)  ztrmv_NUU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztrmv_NUN.$(SUFFIX)  ztrmv_NUN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztrmv_TLU.$(SUFFIX)  ztrmv_TLU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztrmv_TLN.$(SUFFIX)  ztrmv_TLN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztrmv_RLU.$(SUFFIX)  ztrmv_RLU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztrmv_RLN.$(SUFFIX)  ztrmv_RLN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztrmv_CLU.$(SUFFIX)  ztrmv_CLU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztrmv_CLN.$(SUFFIX)  ztrmv_CLN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

ztrmv_NLU.$(SUFFIX)  ztrmv_NLU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

ztrmv_NLN.$(SUFFIX)  ztrmv_NLN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

ztrmv_TUU.$(SUFFIX)  ztrmv_TUU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

ztrmv_TUN.$(SUFFIX)  ztrmv_TUN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

ztrmv_RUU.$(SUFFIX)  ztrmv_RUU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

ztrmv_RUN.$(SUFFIX)  ztrmv_RUN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

ztrmv_CUU.$(SUFFIX)  ztrmv_CUU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

ztrmv_CUN.$(SUFFIX)  ztrmv_CUN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtrmv_NUU.$(SUFFIX)  xtrmv_NUU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtrmv_NUN.$(SUFFIX)  xtrmv_NUN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtrmv_TLU.$(SUFFIX)  xtrmv_TLU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtrmv_TLN.$(SUFFIX)  xtrmv_TLN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtrmv_RLU.$(SUFFIX)  xtrmv_RLU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtrmv_RLN.$(SUFFIX)  xtrmv_RLN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtrmv_CLU.$(SUFFIX)  xtrmv_CLU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtrmv_CLN.$(SUFFIX)  xtrmv_CLN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

xtrmv_NLU.$(SUFFIX)  xtrmv_NLU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -DUNIT $< -o $(@F)

xtrmv_NLN.$(SUFFIX)  xtrmv_NLN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=1 -UUNIT $< -o $(@F)

xtrmv_TUU.$(SUFFIX)  xtrmv_TUU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -DUNIT $< -o $(@F)

xtrmv_TUN.$(SUFFIX)  xtrmv_TUN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=2 -UUNIT $< -o $(@F)

xtrmv_RUU.$(SUFFIX)  xtrmv_RUU.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -DUNIT $< -o $(@F)

xtrmv_RUN.$(SUFFIX)  xtrmv_RUN.$(PSUFFIX)  : ztrmv_U.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=3 -UUNIT $< -o $(@F)

xtrmv_CUU.$(SUFFIX)  xtrmv_CUU.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -DUNIT $< -o $(@F)

xtrmv_CUN.$(SUFFIX)  xtrmv_CUN.$(PSUFFIX)  : ztrmv_L.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA=4 -UUNIT $< -o $(@F)

strmv_thread_NUU.$(SUFFIX)  strmv_thread_NUU.$(PSUFFIX)  : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

strmv_thread_NUN.$(SUFFIX) strmv_thread_NUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

strmv_thread_TLU.$(SUFFIX) strmv_thread_TLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

strmv_thread_TLN.$(SUFFIX) strmv_thread_TLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

strmv_thread_NLU.$(SUFFIX) strmv_thread_NLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

strmv_thread_NLN.$(SUFFIX) strmv_thread_NLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

strmv_thread_TUU.$(SUFFIX) strmv_thread_TUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

strmv_thread_TUN.$(SUFFIX) strmv_thread_TUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

dtrmv_thread_NUU.$(SUFFIX) dtrmv_thread_NUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

dtrmv_thread_NUN.$(SUFFIX) dtrmv_thread_NUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

dtrmv_thread_TLU.$(SUFFIX) dtrmv_thread_TLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

dtrmv_thread_TLN.$(SUFFIX) dtrmv_thread_TLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

dtrmv_thread_NLU.$(SUFFIX) dtrmv_thread_NLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

dtrmv_thread_NLN.$(SUFFIX) dtrmv_thread_NLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

dtrmv_thread_TUU.$(SUFFIX) dtrmv_thread_TUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

dtrmv_thread_TUN.$(SUFFIX) dtrmv_thread_TUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

qtrmv_thread_NUU.$(SUFFIX) qtrmv_thread_NUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -UTRANSA -DUNIT $< -o $(@F)

qtrmv_thread_NUN.$(SUFFIX) qtrmv_thread_NUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -UTRANSA -UUNIT $< -o $(@F)

qtrmv_thread_TLU.$(SUFFIX) qtrmv_thread_TLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -DTRANSA -DUNIT $< -o $(@F)

qtrmv_thread_TLN.$(SUFFIX) qtrmv_thread_TLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -DTRANSA -UUNIT $< -o $(@F)

qtrmv_thread_NLU.$(SUFFIX) qtrmv_thread_NLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -UTRANSA -DUNIT $< -o $(@F)

qtrmv_thread_NLN.$(SUFFIX) qtrmv_thread_NLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DLOWER -UTRANSA -UUNIT $< -o $(@F)

qtrmv_thread_TUU.$(SUFFIX) qtrmv_thread_TUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -DTRANSA -DUNIT $< -o $(@F)

qtrmv_thread_TUN.$(SUFFIX) qtrmv_thread_TUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -ULOWER -DTRANSA -UUNIT $< -o $(@F)

ctrmv_thread_NUU.$(SUFFIX) ctrmv_thread_NUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ctrmv_thread_NUN.$(SUFFIX) ctrmv_thread_NUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ctrmv_thread_TLU.$(SUFFIX) ctrmv_thread_TLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ctrmv_thread_TLN.$(SUFFIX) ctrmv_thread_TLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ctrmv_thread_RLU.$(SUFFIX) ctrmv_thread_RLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ctrmv_thread_RLN.$(SUFFIX) ctrmv_thread_RLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ctrmv_thread_CLU.$(SUFFIX) ctrmv_thread_CLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ctrmv_thread_CLN.$(SUFFIX) ctrmv_thread_CLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ctrmv_thread_NLU.$(SUFFIX) ctrmv_thread_NLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ctrmv_thread_NLN.$(SUFFIX) ctrmv_thread_NLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ctrmv_thread_TUU.$(SUFFIX) ctrmv_thread_TUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ctrmv_thread_TUN.$(SUFFIX) ctrmv_thread_TUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ctrmv_thread_RUU.$(SUFFIX) ctrmv_thread_RUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ctrmv_thread_RUN.$(SUFFIX) ctrmv_thread_RUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ctrmv_thread_CUU.$(SUFFIX) ctrmv_thread_CUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ctrmv_thread_CUN.$(SUFFIX) ctrmv_thread_CUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ztrmv_thread_NUU.$(SUFFIX) ztrmv_thread_NUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ztrmv_thread_NUN.$(SUFFIX) ztrmv_thread_NUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ztrmv_thread_TLU.$(SUFFIX) ztrmv_thread_TLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ztrmv_thread_TLN.$(SUFFIX) ztrmv_thread_TLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ztrmv_thread_RLU.$(SUFFIX) ztrmv_thread_RLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ztrmv_thread_RLN.$(SUFFIX) ztrmv_thread_RLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ztrmv_thread_CLU.$(SUFFIX) ztrmv_thread_CLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ztrmv_thread_CLN.$(SUFFIX) ztrmv_thread_CLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

ztrmv_thread_NLU.$(SUFFIX) ztrmv_thread_NLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

ztrmv_thread_NLN.$(SUFFIX) ztrmv_thread_NLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

ztrmv_thread_TUU.$(SUFFIX) ztrmv_thread_TUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

ztrmv_thread_TUN.$(SUFFIX) ztrmv_thread_TUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

ztrmv_thread_RUU.$(SUFFIX) ztrmv_thread_RUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

ztrmv_thread_RUN.$(SUFFIX) ztrmv_thread_RUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

ztrmv_thread_CUU.$(SUFFIX) ztrmv_thread_CUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

ztrmv_thread_CUN.$(SUFFIX) ztrmv_thread_CUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

xtrmv_thread_NUU.$(SUFFIX) xtrmv_thread_NUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=1 -DUNIT $< -o $(@F)

xtrmv_thread_NUN.$(SUFFIX) xtrmv_thread_NUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=1 -UUNIT $< -o $(@F)

xtrmv_thread_TLU.$(SUFFIX) xtrmv_thread_TLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=2 -DUNIT $< -o $(@F)

xtrmv_thread_TLN.$(SUFFIX) xtrmv_thread_TLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=2 -UUNIT $< -o $(@F)

xtrmv_thread_RLU.$(SUFFIX) xtrmv_thread_RLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=3 -DUNIT $< -o $(@F)

xtrmv_thread_RLN.$(SUFFIX) xtrmv_thread_RLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=3 -UUNIT $< -o $(@F)

xtrmv_thread_CLU.$(SUFFIX) xtrmv_thread_CLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=4 -DUNIT $< -o $(@F)

xtrmv_thread_CLN.$(SUFFIX) xtrmv_thread_CLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=4 -UUNIT $< -o $(@F)

xtrmv_thread_NLU.$(SUFFIX) xtrmv_thread_NLU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=1 -DUNIT $< -o $(@F)

xtrmv_thread_NLN.$(SUFFIX) xtrmv_thread_NLN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DLOWER -DTRANSA=1 -UUNIT $< -o $(@F)

xtrmv_thread_TUU.$(SUFFIX) xtrmv_thread_TUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=2 -DUNIT $< -o $(@F)

xtrmv_thread_TUN.$(SUFFIX) xtrmv_thread_TUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=2 -UUNIT $< -o $(@F)

xtrmv_thread_RUU.$(SUFFIX) xtrmv_thread_RUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=3 -DUNIT $< -o $(@F)

xtrmv_thread_RUN.$(SUFFIX) xtrmv_thread_RUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=3 -UUNIT $< -o $(@F)

xtrmv_thread_CUU.$(SUFFIX) xtrmv_thread_CUU.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=4 -DUNIT $< -o $(@F)

xtrmv_thread_CUN.$(SUFFIX) xtrmv_thread_CUN.$(PSUFFIX) : trmv_thread.c ../../common.h
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -ULOWER -DTRANSA=4 -UUNIT $< -o $(@F)

strsv_NUU.$(SUFFIX)  strsv_NUU.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

strsv_NUN.$(SUFFIX)  strsv_NUN.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

strsv_TLU.$(SUFFIX)  strsv_TLU.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

strsv_TLN.$(SUFFIX)  strsv_TLN.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

strsv_NLU.$(SUFFIX)  strsv_NLU.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -DUNIT $< -o $(@F)

strsv_NLN.$(SUFFIX)  strsv_NLN.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UTRANSA -UUNIT $< -o $(@F)

strsv_TUU.$(SUFFIX)  strsv_TUU.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -DUNIT $< -o $(@F)

strsv_TUN.$(SUFFIX)  strsv_TUN.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtrsv_NUU.$(SUFFIX)  dtrsv_NUU.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtrsv_NUN.$(SUFFIX)  dtrsv_NUN.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtrsv_TLU.$(SUFFIX)  dtrsv_TLU.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtrsv_TLN.$(SUFFIX)  dtrsv_TLN.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

dtrsv_NLU.$(SUFFIX)  dtrsv_NLU.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -DUNIT $< -o $(@F)

dtrsv_NLN.$(SUFFIX)  dtrsv_NLN.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UTRANSA -UUNIT $< -o $(@F)

dtrsv_TUU.$(SUFFIX)  dtrsv_TUU.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -DUNIT $< -o $(@F)

dtrsv_TUN.$(SUFFIX)  dtrsv_TUN.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtrsv_NUU.$(SUFFIX)  qtrsv_NUU.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtrsv_NUN.$(SUFFIX)  qtrsv_NUN.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtrsv_TLU.$(SUFFIX)  qtrsv_TLU.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtrsv_TLN.$(SUFFIX)  qtrsv_TLN.$(PSUFFIX)  : trsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

qtrsv_NLU.$(SUFFIX)  qtrsv_NLU.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -DUNIT $< -o $(@F)

qtrsv_NLN.$(SUFFIX)  qtrsv_NLN.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UTRANSA -UUNIT $< -o $(@F)

qtrsv_TUU.$(SUFFIX)  qtrsv_TUU.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -DUNIT $< -o $(@F)

qtrsv_TUN.$(SUFFIX)  qtrsv_TUN.$(PSUFFIX)  : trsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DTRANSA -UUNIT $< -o $(@F)

ctrsv_NUU.$(SUFFIX)  ctrsv_NUU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ctrsv_NUN.$(SUFFIX)  ctrsv_NUN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ctrsv_TLU.$(SUFFIX)  ctrsv_TLU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ctrsv_TLN.$(SUFFIX)  ctrsv_TLN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ctrsv_RLU.$(SUFFIX)  ctrsv_RLU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ctrsv_RLN.$(SUFFIX)  ctrsv_RLN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ctrsv_CLU.$(SUFFIX)  ctrsv_CLU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ctrsv_CLN.$(SUFFIX)  ctrsv_CLN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

ctrsv_NLU.$(SUFFIX)  ctrsv_NLU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ctrsv_NLN.$(SUFFIX)  ctrsv_NLN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ctrsv_TUU.$(SUFFIX)  ctrsv_TUU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ctrsv_TUN.$(SUFFIX)  ctrsv_TUN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ctrsv_RUU.$(SUFFIX)  ctrsv_RUU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ctrsv_RUN.$(SUFFIX)  ctrsv_RUN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ctrsv_CUU.$(SUFFIX)  ctrsv_CUU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ctrsv_CUN.$(SUFFIX)  ctrsv_CUN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

ztrsv_NUU.$(SUFFIX)  ztrsv_NUU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ztrsv_NUN.$(SUFFIX)  ztrsv_NUN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ztrsv_TLU.$(SUFFIX)  ztrsv_TLU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ztrsv_TLN.$(SUFFIX)  ztrsv_TLN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ztrsv_RLU.$(SUFFIX)  ztrsv_RLU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ztrsv_RLN.$(SUFFIX)  ztrsv_RLN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ztrsv_CLU.$(SUFFIX)  ztrsv_CLU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ztrsv_CLN.$(SUFFIX)  ztrsv_CLN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

ztrsv_NLU.$(SUFFIX)  ztrsv_NLU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

ztrsv_NLN.$(SUFFIX)  ztrsv_NLN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

ztrsv_TUU.$(SUFFIX)  ztrsv_TUU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

ztrsv_TUN.$(SUFFIX)  ztrsv_TUN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

ztrsv_RUU.$(SUFFIX)  ztrsv_RUU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

ztrsv_RUN.$(SUFFIX)  ztrsv_RUN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

ztrsv_CUU.$(SUFFIX)  ztrsv_CUU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

ztrsv_CUN.$(SUFFIX)  ztrsv_CUN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

xtrsv_NUU.$(SUFFIX)  xtrsv_NUU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

xtrsv_NUN.$(SUFFIX)  xtrsv_NUN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

xtrsv_TLU.$(SUFFIX)  xtrsv_TLU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

xtrsv_TLN.$(SUFFIX)  xtrsv_TLN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

xtrsv_RLU.$(SUFFIX)  xtrsv_RLU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

xtrsv_RLN.$(SUFFIX)  xtrsv_RLN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

xtrsv_CLU.$(SUFFIX)  xtrsv_CLU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

xtrsv_CLN.$(SUFFIX)  xtrsv_CLN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

xtrsv_NLU.$(SUFFIX)  xtrsv_NLU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -DUNIT $< -o $(@F)

xtrsv_NLN.$(SUFFIX)  xtrsv_NLN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=1 -UUNIT $< -o $(@F)

xtrsv_TUU.$(SUFFIX)  xtrsv_TUU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -DUNIT $< -o $(@F)

xtrsv_TUN.$(SUFFIX)  xtrsv_TUN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=2 -UUNIT $< -o $(@F)

xtrsv_RUU.$(SUFFIX)  xtrsv_RUU.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -DUNIT $< -o $(@F)

xtrsv_RUN.$(SUFFIX)  xtrsv_RUN.$(PSUFFIX)  : ztrsv_U.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=3 -UUNIT $< -o $(@F)

xtrsv_CUU.$(SUFFIX)  xtrsv_CUU.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -DUNIT $< -o $(@F)

xtrsv_CUN.$(SUFFIX)  xtrsv_CUN.$(PSUFFIX)  : ztrsv_L.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DTRANSA=4 -UUNIT $< -o $(@F)

include ../../Makefile.tail
