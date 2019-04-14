TOPDIR	= ../..
include ../../Makefile.system

USE_GEMM3M = 0

ifeq ($(ARCH), x86)
USE_GEMM3M = 1
endif

ifeq ($(ARCH), x86_64)
USE_GEMM3M = 1
endif

ifeq ($(ARCH), ia64)
USE_GEMM3M = 1
endif

ifeq ($(ARCH), MIPS)
USE_GEMM3M = 1
endif

SBLASOBJS	+= \
	sgemm_nn.$(SUFFIX) sgemm_nt.$(SUFFIX) sgemm_tn.$(SUFFIX) sgemm_tt.$(SUFFIX) \
	strmm_LNUU.$(SUFFIX) strmm_LNUN.$(SUFFIX) strmm_LNLU.$(SUFFIX) strmm_LNLN.$(SUFFIX) \
	strmm_LTUU.$(SUFFIX) strmm_LTUN.$(SUFFIX) strmm_LTLU.$(SUFFIX) strmm_LTLN.$(SUFFIX) \
	strmm_RNUU.$(SUFFIX) strmm_RNUN.$(SUFFIX) strmm_RNLU.$(SUFFIX) strmm_RNLN.$(SUFFIX) \
	strmm_RTUU.$(SUFFIX) strmm_RTUN.$(SUFFIX) strmm_RTLU.$(SUFFIX) strmm_RTLN.$(SUFFIX) \
	strsm_LNUU.$(SUFFIX) strsm_LNUN.$(SUFFIX) strsm_LNLU.$(SUFFIX) strsm_LNLN.$(SUFFIX) \
	strsm_LTUU.$(SUFFIX) strsm_LTUN.$(SUFFIX) strsm_LTLU.$(SUFFIX) strsm_LTLN.$(SUFFIX) \
	strsm_RNUU.$(SUFFIX) strsm_RNUN.$(SUFFIX) strsm_RNLU.$(SUFFIX) strsm_RNLN.$(SUFFIX) \
	strsm_RTUU.$(SUFFIX) strsm_RTUN.$(SUFFIX) strsm_RTLU.$(SUFFIX) strsm_RTLN.$(SUFFIX) \
	ssymm_LU.$(SUFFIX) ssymm_LL.$(SUFFIX) ssymm_RU.$(SUFFIX) ssymm_RL.$(SUFFIX) \
	ssyrk_UN.$(SUFFIX) ssyrk_UT.$(SUFFIX) ssyrk_LN.$(SUFFIX) ssyrk_LT.$(SUFFIX) \
	ssyr2k_UN.$(SUFFIX) ssyr2k_UT.$(SUFFIX) ssyr2k_LN.$(SUFFIX) ssyr2k_LT.$(SUFFIX) \
	ssyrk_kernel_U.$(SUFFIX)  ssyrk_kernel_L.$(SUFFIX) \
	ssyr2k_kernel_U.$(SUFFIX) ssyr2k_kernel_L.$(SUFFIX)

DBLASOBJS	+= \
	dgemm_nn.$(SUFFIX) dgemm_nt.$(SUFFIX) dgemm_tn.$(SUFFIX) dgemm_tt.$(SUFFIX) \
	dtrmm_LNUU.$(SUFFIX) dtrmm_LNUN.$(SUFFIX) dtrmm_LNLU.$(SUFFIX) dtrmm_LNLN.$(SUFFIX) \
	dtrmm_LTUU.$(SUFFIX) dtrmm_LTUN.$(SUFFIX) dtrmm_LTLU.$(SUFFIX) dtrmm_LTLN.$(SUFFIX) \
	dtrmm_RNUU.$(SUFFIX) dtrmm_RNUN.$(SUFFIX) dtrmm_RNLU.$(SUFFIX) dtrmm_RNLN.$(SUFFIX) \
	dtrmm_RTUU.$(SUFFIX) dtrmm_RTUN.$(SUFFIX) dtrmm_RTLU.$(SUFFIX) dtrmm_RTLN.$(SUFFIX) \
	dtrsm_LNUU.$(SUFFIX) dtrsm_LNUN.$(SUFFIX) dtrsm_LNLU.$(SUFFIX) dtrsm_LNLN.$(SUFFIX) \
	dtrsm_LTUU.$(SUFFIX) dtrsm_LTUN.$(SUFFIX) dtrsm_LTLU.$(SUFFIX) dtrsm_LTLN.$(SUFFIX) \
	dtrsm_RNUU.$(SUFFIX) dtrsm_RNUN.$(SUFFIX) dtrsm_RNLU.$(SUFFIX) dtrsm_RNLN.$(SUFFIX) \
	dtrsm_RTUU.$(SUFFIX) dtrsm_RTUN.$(SUFFIX) dtrsm_RTLU.$(SUFFIX) dtrsm_RTLN.$(SUFFIX) \
	dsymm_LU.$(SUFFIX) dsymm_LL.$(SUFFIX) dsymm_RU.$(SUFFIX) dsymm_RL.$(SUFFIX) \
	dsyrk_UN.$(SUFFIX) dsyrk_UT.$(SUFFIX) dsyrk_LN.$(SUFFIX) dsyrk_LT.$(SUFFIX) \
	dsyr2k_UN.$(SUFFIX) dsyr2k_UT.$(SUFFIX) dsyr2k_LN.$(SUFFIX) dsyr2k_LT.$(SUFFIX) \
	dsyrk_kernel_U.$(SUFFIX)  dsyrk_kernel_L.$(SUFFIX) \
	dsyr2k_kernel_U.$(SUFFIX) dsyr2k_kernel_L.$(SUFFIX)

QBLASOBJS	+= \
	qgemm_nn.$(SUFFIX) qgemm_nt.$(SUFFIX) qgemm_tn.$(SUFFIX) qgemm_tt.$(SUFFIX) \
	qtrmm_LNUU.$(SUFFIX) qtrmm_LNUN.$(SUFFIX) qtrmm_LNLU.$(SUFFIX) qtrmm_LNLN.$(SUFFIX) \
	qtrmm_LTUU.$(SUFFIX) qtrmm_LTUN.$(SUFFIX) qtrmm_LTLU.$(SUFFIX) qtrmm_LTLN.$(SUFFIX) \
	qtrmm_RNUU.$(SUFFIX) qtrmm_RNUN.$(SUFFIX) qtrmm_RNLU.$(SUFFIX) qtrmm_RNLN.$(SUFFIX) \
	qtrmm_RTUU.$(SUFFIX) qtrmm_RTUN.$(SUFFIX) qtrmm_RTLU.$(SUFFIX) qtrmm_RTLN.$(SUFFIX) \
	qtrsm_LNUU.$(SUFFIX) qtrsm_LNUN.$(SUFFIX) qtrsm_LNLU.$(SUFFIX) qtrsm_LNLN.$(SUFFIX) \
	qtrsm_LTUU.$(SUFFIX) qtrsm_LTUN.$(SUFFIX) qtrsm_LTLU.$(SUFFIX) qtrsm_LTLN.$(SUFFIX) \
	qtrsm_RNUU.$(SUFFIX) qtrsm_RNUN.$(SUFFIX) qtrsm_RNLU.$(SUFFIX) qtrsm_RNLN.$(SUFFIX) \
	qtrsm_RTUU.$(SUFFIX) qtrsm_RTUN.$(SUFFIX) qtrsm_RTLU.$(SUFFIX) qtrsm_RTLN.$(SUFFIX) \
	qsymm_LU.$(SUFFIX) qsymm_LL.$(SUFFIX) qsymm_RU.$(SUFFIX) qsymm_RL.$(SUFFIX) \
	qsyrk_UN.$(SUFFIX) qsyrk_UT.$(SUFFIX) qsyrk_LN.$(SUFFIX) qsyrk_LT.$(SUFFIX) \
	qsyr2k_UN.$(SUFFIX) qsyr2k_UT.$(SUFFIX) qsyr2k_LN.$(SUFFIX) qsyr2k_LT.$(SUFFIX) \
	qsyrk_kernel_U.$(SUFFIX)  qsyrk_kernel_L.$(SUFFIX) \
	qsyr2k_kernel_U.$(SUFFIX) qsyr2k_kernel_L.$(SUFFIX)

CBLASOBJS	+= \
	cgemm_nn.$(SUFFIX) cgemm_cn.$(SUFFIX) cgemm_tn.$(SUFFIX) cgemm_nc.$(SUFFIX) \
	cgemm_nt.$(SUFFIX) cgemm_cc.$(SUFFIX) cgemm_ct.$(SUFFIX) cgemm_tc.$(SUFFIX) \
	cgemm_tt.$(SUFFIX) cgemm_nr.$(SUFFIX) cgemm_tr.$(SUFFIX) cgemm_cr.$(SUFFIX) \
	cgemm_rn.$(SUFFIX) cgemm_rt.$(SUFFIX) cgemm_rc.$(SUFFIX) cgemm_rr.$(SUFFIX) \
	ctrmm_LNUU.$(SUFFIX) ctrmm_LNUN.$(SUFFIX) ctrmm_LNLU.$(SUFFIX) ctrmm_LNLN.$(SUFFIX) \
	ctrmm_LTUU.$(SUFFIX) ctrmm_LTUN.$(SUFFIX) ctrmm_LTLU.$(SUFFIX) ctrmm_LTLN.$(SUFFIX) \
	ctrmm_LRUU.$(SUFFIX) ctrmm_LRUN.$(SUFFIX) ctrmm_LRLU.$(SUFFIX) ctrmm_LRLN.$(SUFFIX) \
	ctrmm_LCUU.$(SUFFIX) ctrmm_LCUN.$(SUFFIX) ctrmm_LCLU.$(SUFFIX) ctrmm_LCLN.$(SUFFIX) \
	ctrmm_RNUU.$(SUFFIX) ctrmm_RNUN.$(SUFFIX) ctrmm_RNLU.$(SUFFIX) ctrmm_RNLN.$(SUFFIX) \
	ctrmm_RTUU.$(SUFFIX) ctrmm_RTUN.$(SUFFIX) ctrmm_RTLU.$(SUFFIX) ctrmm_RTLN.$(SUFFIX) \
	ctrmm_RRUU.$(SUFFIX) ctrmm_RRUN.$(SUFFIX) ctrmm_RRLU.$(SUFFIX) ctrmm_RRLN.$(SUFFIX) \
	ctrmm_RCUU.$(SUFFIX) ctrmm_RCUN.$(SUFFIX) ctrmm_RCLU.$(SUFFIX) ctrmm_RCLN.$(SUFFIX) \
	ctrsm_LNUU.$(SUFFIX) ctrsm_LNUN.$(SUFFIX) ctrsm_LNLU.$(SUFFIX) ctrsm_LNLN.$(SUFFIX) \
	ctrsm_LTUU.$(SUFFIX) ctrsm_LTUN.$(SUFFIX) ctrsm_LTLU.$(SUFFIX) ctrsm_LTLN.$(SUFFIX) \
	ctrsm_LRUU.$(SUFFIX) ctrsm_LRUN.$(SUFFIX) ctrsm_LRLU.$(SUFFIX) ctrsm_LRLN.$(SUFFIX) \
	ctrsm_LCUU.$(SUFFIX) ctrsm_LCUN.$(SUFFIX) ctrsm_LCLU.$(SUFFIX) ctrsm_LCLN.$(SUFFIX) \
	ctrsm_RNUU.$(SUFFIX) ctrsm_RNUN.$(SUFFIX) ctrsm_RNLU.$(SUFFIX) ctrsm_RNLN.$(SUFFIX) \
	ctrsm_RTUU.$(SUFFIX) ctrsm_RTUN.$(SUFFIX) ctrsm_RTLU.$(SUFFIX) ctrsm_RTLN.$(SUFFIX) \
	ctrsm_RRUU.$(SUFFIX) ctrsm_RRUN.$(SUFFIX) ctrsm_RRLU.$(SUFFIX) ctrsm_RRLN.$(SUFFIX) \
	ctrsm_RCUU.$(SUFFIX) ctrsm_RCUN.$(SUFFIX) ctrsm_RCLU.$(SUFFIX) ctrsm_RCLN.$(SUFFIX) \
	csymm_LU.$(SUFFIX) csymm_LL.$(SUFFIX) csymm_RU.$(SUFFIX) csymm_RL.$(SUFFIX) \
	chemm_LU.$(SUFFIX) chemm_LL.$(SUFFIX) chemm_RU.$(SUFFIX) chemm_RL.$(SUFFIX) \
	csyrk_UN.$(SUFFIX) csyrk_UT.$(SUFFIX) csyrk_LN.$(SUFFIX) csyrk_LT.$(SUFFIX) \
	cherk_UN.$(SUFFIX) cherk_UC.$(SUFFIX) cherk_LN.$(SUFFIX) cherk_LC.$(SUFFIX) \
	csyr2k_UN.$(SUFFIX) csyr2k_UT.$(SUFFIX) csyr2k_LN.$(SUFFIX) csyr2k_LT.$(SUFFIX) \
	cher2k_UN.$(SUFFIX) cher2k_UC.$(SUFFIX) cher2k_LN.$(SUFFIX) cher2k_LC.$(SUFFIX) \
	csyrk_kernel_U.$(SUFFIX)   csyrk_kernel_L.$(SUFFIX) \
	cherk_kernel_UN.$(SUFFIX)  cherk_kernel_UC.$(SUFFIX) \
	cherk_kernel_LN.$(SUFFIX)  cherk_kernel_LC.$(SUFFIX) \
	csyr2k_kernel_U.$(SUFFIX)  csyr2k_kernel_L.$(SUFFIX) \
	cher2k_kernel_UN.$(SUFFIX) cher2k_kernel_UC.$(SUFFIX) \
	cher2k_kernel_LN.$(SUFFIX) cher2k_kernel_LC.$(SUFFIX)

ZBLASOBJS	+= \
	zgemm_nn.$(SUFFIX) zgemm_cn.$(SUFFIX) zgemm_tn.$(SUFFIX) zgemm_nc.$(SUFFIX) \
	zgemm_nt.$(SUFFIX) zgemm_cc.$(SUFFIX) zgemm_ct.$(SUFFIX) zgemm_tc.$(SUFFIX) \
	zgemm_tt.$(SUFFIX) zgemm_nr.$(SUFFIX) zgemm_tr.$(SUFFIX) zgemm_cr.$(SUFFIX) \
	zgemm_rn.$(SUFFIX) zgemm_rt.$(SUFFIX) zgemm_rc.$(SUFFIX) zgemm_rr.$(SUFFIX) \
	ztrmm_LNUU.$(SUFFIX) ztrmm_LNUN.$(SUFFIX) ztrmm_LNLU.$(SUFFIX) ztrmm_LNLN.$(SUFFIX) \
	ztrmm_LTUU.$(SUFFIX) ztrmm_LTUN.$(SUFFIX) ztrmm_LTLU.$(SUFFIX) ztrmm_LTLN.$(SUFFIX) \
	ztrmm_LRUU.$(SUFFIX) ztrmm_LRUN.$(SUFFIX) ztrmm_LRLU.$(SUFFIX) ztrmm_LRLN.$(SUFFIX) \
	ztrmm_LCUU.$(SUFFIX) ztrmm_LCUN.$(SUFFIX) ztrmm_LCLU.$(SUFFIX) ztrmm_LCLN.$(SUFFIX) \
	ztrmm_RNUU.$(SUFFIX) ztrmm_RNUN.$(SUFFIX) ztrmm_RNLU.$(SUFFIX) ztrmm_RNLN.$(SUFFIX) \
	ztrmm_RTUU.$(SUFFIX) ztrmm_RTUN.$(SUFFIX) ztrmm_RTLU.$(SUFFIX) ztrmm_RTLN.$(SUFFIX) \
	ztrmm_RRUU.$(SUFFIX) ztrmm_RRUN.$(SUFFIX) ztrmm_RRLU.$(SUFFIX) ztrmm_RRLN.$(SUFFIX) \
	ztrmm_RCUU.$(SUFFIX) ztrmm_RCUN.$(SUFFIX) ztrmm_RCLU.$(SUFFIX) ztrmm_RCLN.$(SUFFIX) \
	ztrsm_LNUU.$(SUFFIX) ztrsm_LNUN.$(SUFFIX) ztrsm_LNLU.$(SUFFIX) ztrsm_LNLN.$(SUFFIX) \
	ztrsm_LTUU.$(SUFFIX) ztrsm_LTUN.$(SUFFIX) ztrsm_LTLU.$(SUFFIX) ztrsm_LTLN.$(SUFFIX) \
	ztrsm_LRUU.$(SUFFIX) ztrsm_LRUN.$(SUFFIX) ztrsm_LRLU.$(SUFFIX) ztrsm_LRLN.$(SUFFIX) \
	ztrsm_LCUU.$(SUFFIX) ztrsm_LCUN.$(SUFFIX) ztrsm_LCLU.$(SUFFIX) ztrsm_LCLN.$(SUFFIX) \
	ztrsm_RNUU.$(SUFFIX) ztrsm_RNUN.$(SUFFIX) ztrsm_RNLU.$(SUFFIX) ztrsm_RNLN.$(SUFFIX) \
	ztrsm_RTUU.$(SUFFIX) ztrsm_RTUN.$(SUFFIX) ztrsm_RTLU.$(SUFFIX) ztrsm_RTLN.$(SUFFIX) \
	ztrsm_RRUU.$(SUFFIX) ztrsm_RRUN.$(SUFFIX) ztrsm_RRLU.$(SUFFIX) ztrsm_RRLN.$(SUFFIX) \
	ztrsm_RCUU.$(SUFFIX) ztrsm_RCUN.$(SUFFIX) ztrsm_RCLU.$(SUFFIX) ztrsm_RCLN.$(SUFFIX) \
	zsymm_LU.$(SUFFIX) zsymm_LL.$(SUFFIX) zsymm_RU.$(SUFFIX) zsymm_RL.$(SUFFIX) \
	zhemm_LU.$(SUFFIX) zhemm_LL.$(SUFFIX) zhemm_RU.$(SUFFIX) zhemm_RL.$(SUFFIX) \
	zsyrk_UN.$(SUFFIX) zsyrk_UT.$(SUFFIX) zsyrk_LN.$(SUFFIX) zsyrk_LT.$(SUFFIX) \
	zherk_UN.$(SUFFIX) zherk_UC.$(SUFFIX) zherk_LN.$(SUFFIX) zherk_LC.$(SUFFIX) \
	zsyr2k_UN.$(SUFFIX) zsyr2k_UT.$(SUFFIX) zsyr2k_LN.$(SUFFIX) zsyr2k_LT.$(SUFFIX) \
	zher2k_UN.$(SUFFIX) zher2k_UC.$(SUFFIX) zher2k_LN.$(SUFFIX) zher2k_LC.$(SUFFIX) \
	zsyrk_kernel_U.$(SUFFIX)   zsyrk_kernel_L.$(SUFFIX) \
	zherk_kernel_UN.$(SUFFIX)  zherk_kernel_UC.$(SUFFIX) \
	zherk_kernel_LN.$(SUFFIX)  zherk_kernel_LC.$(SUFFIX) \
	zsyr2k_kernel_U.$(SUFFIX)  zsyr2k_kernel_L.$(SUFFIX) \
	zher2k_kernel_UN.$(SUFFIX) zher2k_kernel_UC.$(SUFFIX) \
	zher2k_kernel_LN.$(SUFFIX) zher2k_kernel_LC.$(SUFFIX)


XBLASOBJS	+= \
	xgemm_nn.$(SUFFIX) xgemm_cn.$(SUFFIX) xgemm_tn.$(SUFFIX) xgemm_nc.$(SUFFIX) \
	xgemm_nt.$(SUFFIX) xgemm_cc.$(SUFFIX) xgemm_ct.$(SUFFIX) xgemm_tc.$(SUFFIX) \
	xgemm_tt.$(SUFFIX) xgemm_nr.$(SUFFIX) xgemm_tr.$(SUFFIX) xgemm_cr.$(SUFFIX) \
	xgemm_rn.$(SUFFIX) xgemm_rt.$(SUFFIX) xgemm_rc.$(SUFFIX) xgemm_rr.$(SUFFIX) \
	xtrmm_LNUU.$(SUFFIX) xtrmm_LNUN.$(SUFFIX) xtrmm_LNLU.$(SUFFIX) xtrmm_LNLN.$(SUFFIX) \
	xtrmm_LTUU.$(SUFFIX) xtrmm_LTUN.$(SUFFIX) xtrmm_LTLU.$(SUFFIX) xtrmm_LTLN.$(SUFFIX) \
	xtrmm_LRUU.$(SUFFIX) xtrmm_LRUN.$(SUFFIX) xtrmm_LRLU.$(SUFFIX) xtrmm_LRLN.$(SUFFIX) \
	xtrmm_LCUU.$(SUFFIX) xtrmm_LCUN.$(SUFFIX) xtrmm_LCLU.$(SUFFIX) xtrmm_LCLN.$(SUFFIX) \
	xtrmm_RNUU.$(SUFFIX) xtrmm_RNUN.$(SUFFIX) xtrmm_RNLU.$(SUFFIX) xtrmm_RNLN.$(SUFFIX) \
	xtrmm_RTUU.$(SUFFIX) xtrmm_RTUN.$(SUFFIX) xtrmm_RTLU.$(SUFFIX) xtrmm_RTLN.$(SUFFIX) \
	xtrmm_RRUU.$(SUFFIX) xtrmm_RRUN.$(SUFFIX) xtrmm_RRLU.$(SUFFIX) xtrmm_RRLN.$(SUFFIX) \
	xtrmm_RCUU.$(SUFFIX) xtrmm_RCUN.$(SUFFIX) xtrmm_RCLU.$(SUFFIX) xtrmm_RCLN.$(SUFFIX) \
	xtrsm_LNUU.$(SUFFIX) xtrsm_LNUN.$(SUFFIX) xtrsm_LNLU.$(SUFFIX) xtrsm_LNLN.$(SUFFIX) \
	xtrsm_LTUU.$(SUFFIX) xtrsm_LTUN.$(SUFFIX) xtrsm_LTLU.$(SUFFIX) xtrsm_LTLN.$(SUFFIX) \
	xtrsm_LRUU.$(SUFFIX) xtrsm_LRUN.$(SUFFIX) xtrsm_LRLU.$(SUFFIX) xtrsm_LRLN.$(SUFFIX) \
	xtrsm_LCUU.$(SUFFIX) xtrsm_LCUN.$(SUFFIX) xtrsm_LCLU.$(SUFFIX) xtrsm_LCLN.$(SUFFIX) \
	xtrsm_RNUU.$(SUFFIX) xtrsm_RNUN.$(SUFFIX) xtrsm_RNLU.$(SUFFIX) xtrsm_RNLN.$(SUFFIX) \
	xtrsm_RTUU.$(SUFFIX) xtrsm_RTUN.$(SUFFIX) xtrsm_RTLU.$(SUFFIX) xtrsm_RTLN.$(SUFFIX) \
	xtrsm_RRUU.$(SUFFIX) xtrsm_RRUN.$(SUFFIX) xtrsm_RRLU.$(SUFFIX) xtrsm_RRLN.$(SUFFIX) \
	xtrsm_RCUU.$(SUFFIX) xtrsm_RCUN.$(SUFFIX) xtrsm_RCLU.$(SUFFIX) xtrsm_RCLN.$(SUFFIX) \
	xsymm_LU.$(SUFFIX) xsymm_LL.$(SUFFIX) xsymm_RU.$(SUFFIX) xsymm_RL.$(SUFFIX) \
	xhemm_LU.$(SUFFIX) xhemm_LL.$(SUFFIX) xhemm_RU.$(SUFFIX) xhemm_RL.$(SUFFIX) \
	xsyrk_UN.$(SUFFIX) xsyrk_UT.$(SUFFIX) xsyrk_LN.$(SUFFIX) xsyrk_LT.$(SUFFIX) \
	xherk_UN.$(SUFFIX) xherk_UC.$(SUFFIX) xherk_LN.$(SUFFIX) xherk_LC.$(SUFFIX) \
	xsyr2k_UN.$(SUFFIX) xsyr2k_UT.$(SUFFIX) xsyr2k_LN.$(SUFFIX) xsyr2k_LT.$(SUFFIX) \
	xher2k_UN.$(SUFFIX) xher2k_UC.$(SUFFIX) xher2k_LN.$(SUFFIX) xher2k_LC.$(SUFFIX) \
	xsyrk_kernel_U.$(SUFFIX)   xsyrk_kernel_L.$(SUFFIX) \
	xherk_kernel_UN.$(SUFFIX)  xherk_kernel_UC.$(SUFFIX) \
	xherk_kernel_LN.$(SUFFIX)  xherk_kernel_LC.$(SUFFIX) \
	xsyr2k_kernel_U.$(SUFFIX)  xsyr2k_kernel_L.$(SUFFIX) \
	xher2k_kernel_UN.$(SUFFIX) xher2k_kernel_UC.$(SUFFIX) \
	xher2k_kernel_LN.$(SUFFIX) xher2k_kernel_LC.$(SUFFIX)

ifeq ($(USE_GEMM3M), 1)

CBLASOBJS   +=  \
	cgemm3m_nn.$(SUFFIX) cgemm3m_cn.$(SUFFIX) cgemm3m_tn.$(SUFFIX) cgemm3m_nc.$(SUFFIX) \
	cgemm3m_nt.$(SUFFIX) cgemm3m_cc.$(SUFFIX) cgemm3m_ct.$(SUFFIX) cgemm3m_tc.$(SUFFIX) \
	cgemm3m_tt.$(SUFFIX) cgemm3m_nr.$(SUFFIX) cgemm3m_tr.$(SUFFIX) cgemm3m_cr.$(SUFFIX) \
	cgemm3m_rn.$(SUFFIX) cgemm3m_rt.$(SUFFIX) cgemm3m_rc.$(SUFFIX) cgemm3m_rr.$(SUFFIX) \
	csymm3m_LU.$(SUFFIX) csymm3m_LL.$(SUFFIX) csymm3m_RU.$(SUFFIX) csymm3m_RL.$(SUFFIX) \
	chemm3m_LU.$(SUFFIX) chemm3m_LL.$(SUFFIX) chemm3m_RU.$(SUFFIX) chemm3m_RL.$(SUFFIX)

ZBLASOBJS   +=  \
	zgemm3m_nn.$(SUFFIX) zgemm3m_cn.$(SUFFIX) zgemm3m_tn.$(SUFFIX) zgemm3m_nc.$(SUFFIX) \
	zgemm3m_nt.$(SUFFIX) zgemm3m_cc.$(SUFFIX) zgemm3m_ct.$(SUFFIX) zgemm3m_tc.$(SUFFIX) \
	zgemm3m_tt.$(SUFFIX) zgemm3m_nr.$(SUFFIX) zgemm3m_tr.$(SUFFIX) zgemm3m_cr.$(SUFFIX) \
	zgemm3m_rn.$(SUFFIX) zgemm3m_rt.$(SUFFIX) zgemm3m_rc.$(SUFFIX) zgemm3m_rr.$(SUFFIX) \
	zsymm3m_LU.$(SUFFIX) zsymm3m_LL.$(SUFFIX) zsymm3m_RU.$(SUFFIX) zsymm3m_RL.$(SUFFIX) \
	zhemm3m_LU.$(SUFFIX) zhemm3m_LL.$(SUFFIX) zhemm3m_RU.$(SUFFIX) zhemm3m_RL.$(SUFFIX)

XBLASOBJS   +=  \
	xgemm3m_nn.$(SUFFIX) xgemm3m_cn.$(SUFFIX) xgemm3m_tn.$(SUFFIX) xgemm3m_nc.$(SUFFIX) \
	xgemm3m_nt.$(SUFFIX) xgemm3m_cc.$(SUFFIX) xgemm3m_ct.$(SUFFIX) xgemm3m_tc.$(SUFFIX) \
	xgemm3m_tt.$(SUFFIX) xgemm3m_nr.$(SUFFIX) xgemm3m_tr.$(SUFFIX) xgemm3m_cr.$(SUFFIX) \
	xgemm3m_rn.$(SUFFIX) xgemm3m_rt.$(SUFFIX) xgemm3m_rc.$(SUFFIX) xgemm3m_rr.$(SUFFIX) \
	xsymm3m_LU.$(SUFFIX) xsymm3m_LL.$(SUFFIX) xsymm3m_RU.$(SUFFIX) xsymm3m_RL.$(SUFFIX) \
	xhemm3m_LU.$(SUFFIX) xhemm3m_LL.$(SUFFIX) xhemm3m_RU.$(SUFFIX) xhemm3m_RL.$(SUFFIX)

endif

ifdef SMP
COMMONOBJS  += gemm_thread_m.$(SUFFIX) gemm_thread_n.$(SUFFIX) gemm_thread_mn.$(SUFFIX) gemm_thread_variable.$(SUFFIX)
COMMONOBJS  += syrk_thread.$(SUFFIX)

ifndef USE_SIMPLE_THREADED_LEVEL3

SBLASOBJS    += sgemm_thread_nn.$(SUFFIX) sgemm_thread_nt.$(SUFFIX) sgemm_thread_tn.$(SUFFIX) sgemm_thread_tt.$(SUFFIX)
DBLASOBJS    += dgemm_thread_nn.$(SUFFIX) dgemm_thread_nt.$(SUFFIX) dgemm_thread_tn.$(SUFFIX) dgemm_thread_tt.$(SUFFIX)
QBLASOBJS    += qgemm_thread_nn.$(SUFFIX) qgemm_thread_nt.$(SUFFIX) qgemm_thread_tn.$(SUFFIX) qgemm_thread_tt.$(SUFFIX)
CBLASOBJS    += cgemm_thread_nn.$(SUFFIX) cgemm_thread_nt.$(SUFFIX) cgemm_thread_nr.$(SUFFIX) cgemm_thread_nc.$(SUFFIX)
CBLASOBJS    += cgemm_thread_tn.$(SUFFIX) cgemm_thread_tt.$(SUFFIX) cgemm_thread_tr.$(SUFFIX) cgemm_thread_tc.$(SUFFIX)
CBLASOBJS    += cgemm_thread_rn.$(SUFFIX) cgemm_thread_rt.$(SUFFIX) cgemm_thread_rr.$(SUFFIX) cgemm_thread_rc.$(SUFFIX)
CBLASOBJS    += cgemm_thread_cn.$(SUFFIX) cgemm_thread_ct.$(SUFFIX) cgemm_thread_cr.$(SUFFIX) cgemm_thread_cc.$(SUFFIX)
ZBLASOBJS    += zgemm_thread_nn.$(SUFFIX) zgemm_thread_nt.$(SUFFIX) zgemm_thread_nr.$(SUFFIX) zgemm_thread_nc.$(SUFFIX)
ZBLASOBJS    += zgemm_thread_tn.$(SUFFIX) zgemm_thread_tt.$(SUFFIX) zgemm_thread_tr.$(SUFFIX) zgemm_thread_tc.$(SUFFIX)
ZBLASOBJS    += zgemm_thread_rn.$(SUFFIX) zgemm_thread_rt.$(SUFFIX) zgemm_thread_rr.$(SUFFIX) zgemm_thread_rc.$(SUFFIX)
ZBLASOBJS    += zgemm_thread_cn.$(SUFFIX) zgemm_thread_ct.$(SUFFIX) zgemm_thread_cr.$(SUFFIX) zgemm_thread_cc.$(SUFFIX)
XBLASOBJS    += xgemm_thread_nn.$(SUFFIX) xgemm_thread_nt.$(SUFFIX) xgemm_thread_nr.$(SUFFIX) xgemm_thread_nc.$(SUFFIX)
XBLASOBJS    += xgemm_thread_tn.$(SUFFIX) xgemm_thread_tt.$(SUFFIX) xgemm_thread_tr.$(SUFFIX) xgemm_thread_tc.$(SUFFIX)
XBLASOBJS    += xgemm_thread_rn.$(SUFFIX) xgemm_thread_rt.$(SUFFIX) xgemm_thread_rr.$(SUFFIX) xgemm_thread_rc.$(SUFFIX)
XBLASOBJS    += xgemm_thread_cn.$(SUFFIX) xgemm_thread_ct.$(SUFFIX) xgemm_thread_cr.$(SUFFIX) xgemm_thread_cc.$(SUFFIX)

SBLASOBJS    += ssymm_thread_LU.$(SUFFIX) ssymm_thread_LL.$(SUFFIX) ssymm_thread_RU.$(SUFFIX) ssymm_thread_RL.$(SUFFIX)
DBLASOBJS    += dsymm_thread_LU.$(SUFFIX) dsymm_thread_LL.$(SUFFIX) dsymm_thread_RU.$(SUFFIX) dsymm_thread_RL.$(SUFFIX)
QBLASOBJS    += qsymm_thread_LU.$(SUFFIX) qsymm_thread_LL.$(SUFFIX) qsymm_thread_RU.$(SUFFIX) qsymm_thread_RL.$(SUFFIX)
CBLASOBJS    += csymm_thread_LU.$(SUFFIX) csymm_thread_LL.$(SUFFIX) csymm_thread_RU.$(SUFFIX) csymm_thread_RL.$(SUFFIX)
ZBLASOBJS    += zsymm_thread_LU.$(SUFFIX) zsymm_thread_LL.$(SUFFIX) zsymm_thread_RU.$(SUFFIX) zsymm_thread_RL.$(SUFFIX)
XBLASOBJS    += xsymm_thread_LU.$(SUFFIX) xsymm_thread_LL.$(SUFFIX) xsymm_thread_RU.$(SUFFIX) xsymm_thread_RL.$(SUFFIX)

CBLASOBJS    += chemm_thread_LU.$(SUFFIX) chemm_thread_LL.$(SUFFIX) chemm_thread_RU.$(SUFFIX) chemm_thread_RL.$(SUFFIX)
ZBLASOBJS    += zhemm_thread_LU.$(SUFFIX) zhemm_thread_LL.$(SUFFIX) zhemm_thread_RU.$(SUFFIX) zhemm_thread_RL.$(SUFFIX)
XBLASOBJS    += xhemm_thread_LU.$(SUFFIX) xhemm_thread_LL.$(SUFFIX) xhemm_thread_RU.$(SUFFIX) xhemm_thread_RL.$(SUFFIX)

SBLASOBJS    += ssyrk_thread_UN.$(SUFFIX) ssyrk_thread_UT.$(SUFFIX) ssyrk_thread_LN.$(SUFFIX) ssyrk_thread_LT.$(SUFFIX)
DBLASOBJS    += dsyrk_thread_UN.$(SUFFIX) dsyrk_thread_UT.$(SUFFIX) dsyrk_thread_LN.$(SUFFIX) dsyrk_thread_LT.$(SUFFIX)
QBLASOBJS    += qsyrk_thread_UN.$(SUFFIX) qsyrk_thread_UT.$(SUFFIX) qsyrk_thread_LN.$(SUFFIX) qsyrk_thread_LT.$(SUFFIX)
CBLASOBJS    += csyrk_thread_UN.$(SUFFIX) csyrk_thread_UT.$(SUFFIX) csyrk_thread_LN.$(SUFFIX) csyrk_thread_LT.$(SUFFIX)
ZBLASOBJS    += zsyrk_thread_UN.$(SUFFIX) zsyrk_thread_UT.$(SUFFIX) zsyrk_thread_LN.$(SUFFIX) zsyrk_thread_LT.$(SUFFIX)
XBLASOBJS    += xsyrk_thread_UN.$(SUFFIX) xsyrk_thread_UT.$(SUFFIX) xsyrk_thread_LN.$(SUFFIX) xsyrk_thread_LT.$(SUFFIX)
CBLASOBJS    += cherk_thread_UN.$(SUFFIX) cherk_thread_UC.$(SUFFIX) cherk_thread_LN.$(SUFFIX) cherk_thread_LC.$(SUFFIX)
ZBLASOBJS    += zherk_thread_UN.$(SUFFIX) zherk_thread_UC.$(SUFFIX) zherk_thread_LN.$(SUFFIX) zherk_thread_LC.$(SUFFIX)
XBLASOBJS    += xherk_thread_UN.$(SUFFIX) xherk_thread_UC.$(SUFFIX) xherk_thread_LN.$(SUFFIX) xherk_thread_LC.$(SUFFIX)

ifeq ($(USE_GEMM3M), 1)

CBLASOBJS    += cgemm3m_thread_nn.$(SUFFIX) cgemm3m_thread_nt.$(SUFFIX) cgemm3m_thread_nr.$(SUFFIX) cgemm3m_thread_nc.$(SUFFIX)
CBLASOBJS    += cgemm3m_thread_tn.$(SUFFIX) cgemm3m_thread_tt.$(SUFFIX) cgemm3m_thread_tr.$(SUFFIX) cgemm3m_thread_tc.$(SUFFIX)
CBLASOBJS    += cgemm3m_thread_rn.$(SUFFIX) cgemm3m_thread_rt.$(SUFFIX) cgemm3m_thread_rr.$(SUFFIX) cgemm3m_thread_rc.$(SUFFIX)
CBLASOBJS    += cgemm3m_thread_cn.$(SUFFIX) cgemm3m_thread_ct.$(SUFFIX) cgemm3m_thread_cr.$(SUFFIX) cgemm3m_thread_cc.$(SUFFIX)
ZBLASOBJS    += zgemm3m_thread_nn.$(SUFFIX) zgemm3m_thread_nt.$(SUFFIX) zgemm3m_thread_nr.$(SUFFIX) zgemm3m_thread_nc.$(SUFFIX)
ZBLASOBJS    += zgemm3m_thread_tn.$(SUFFIX) zgemm3m_thread_tt.$(SUFFIX) zgemm3m_thread_tr.$(SUFFIX) zgemm3m_thread_tc.$(SUFFIX)
ZBLASOBJS    += zgemm3m_thread_rn.$(SUFFIX) zgemm3m_thread_rt.$(SUFFIX) zgemm3m_thread_rr.$(SUFFIX) zgemm3m_thread_rc.$(SUFFIX)
ZBLASOBJS    += zgemm3m_thread_cn.$(SUFFIX) zgemm3m_thread_ct.$(SUFFIX) zgemm3m_thread_cr.$(SUFFIX) zgemm3m_thread_cc.$(SUFFIX)
XBLASOBJS    += xgemm3m_thread_nn.$(SUFFIX) xgemm3m_thread_nt.$(SUFFIX) xgemm3m_thread_nr.$(SUFFIX) xgemm3m_thread_nc.$(SUFFIX)
XBLASOBJS    += xgemm3m_thread_tn.$(SUFFIX) xgemm3m_thread_tt.$(SUFFIX) xgemm3m_thread_tr.$(SUFFIX) xgemm3m_thread_tc.$(SUFFIX)
XBLASOBJS    += xgemm3m_thread_rn.$(SUFFIX) xgemm3m_thread_rt.$(SUFFIX) xgemm3m_thread_rr.$(SUFFIX) xgemm3m_thread_rc.$(SUFFIX)
XBLASOBJS    += xgemm3m_thread_cn.$(SUFFIX) xgemm3m_thread_ct.$(SUFFIX) xgemm3m_thread_cr.$(SUFFIX) xgemm3m_thread_cc.$(SUFFIX)

CBLASOBJS    += csymm3m_thread_LU.$(SUFFIX) csymm3m_thread_LL.$(SUFFIX) csymm3m_thread_RU.$(SUFFIX) csymm3m_thread_RL.$(SUFFIX)
ZBLASOBJS    += zsymm3m_thread_LU.$(SUFFIX) zsymm3m_thread_LL.$(SUFFIX) zsymm3m_thread_RU.$(SUFFIX) zsymm3m_thread_RL.$(SUFFIX)
XBLASOBJS    += xsymm3m_thread_LU.$(SUFFIX) xsymm3m_thread_LL.$(SUFFIX) xsymm3m_thread_RU.$(SUFFIX) xsymm3m_thread_RL.$(SUFFIX)

CBLASOBJS    += chemm3m_thread_LU.$(SUFFIX) chemm3m_thread_LL.$(SUFFIX) chemm3m_thread_RU.$(SUFFIX) chemm3m_thread_RL.$(SUFFIX)
ZBLASOBJS    += zhemm3m_thread_LU.$(SUFFIX) zhemm3m_thread_LL.$(SUFFIX) zhemm3m_thread_RU.$(SUFFIX) zhemm3m_thread_RL.$(SUFFIX)
XBLASOBJS    += xhemm3m_thread_LU.$(SUFFIX) xhemm3m_thread_LL.$(SUFFIX) xhemm3m_thread_RU.$(SUFFIX) xhemm3m_thread_RL.$(SUFFIX)

endif

endif
endif

HPLOBJS = \
	dgemm_nn.$(SUFFIX) dgemm_nt.$(SUFFIX) dgemm_tn.$(SUFFIX) dgemm_tt.$(SUFFIX) \
	dtrsm_LNUU.$(SUFFIX) dtrsm_LNUN.$(SUFFIX) dtrsm_LNLU.$(SUFFIX) dtrsm_LNLN.$(SUFFIX) \
	dtrsm_LTUU.$(SUFFIX) dtrsm_LTUN.$(SUFFIX) dtrsm_LTLU.$(SUFFIX) dtrsm_LTLN.$(SUFFIX) \
	dtrsm_RNUU.$(SUFFIX) dtrsm_RNUN.$(SUFFIX) dtrsm_RNLU.$(SUFFIX) dtrsm_RNLN.$(SUFFIX) \
	dtrsm_RTUU.$(SUFFIX) dtrsm_RTUN.$(SUFFIX) dtrsm_RTLU.$(SUFFIX) dtrsm_RTLN.$(SUFFIX)

ifndef USE_SIMPLE_THREADED_LEVEL3
HPLOBJS += dgemm_thread_nn.$(SUFFIX) dgemm_thread_nt.$(SUFFIX) \
	   dgemm_thread_tn.$(SUFFIX) dgemm_thread_tt.$(SUFFIX)
endif

all ::

sgemm_nn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DNN $< -o $(@F)

sgemm_nt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DNT $< -o $(@F)

sgemm_tn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DTN $< -o $(@F)

sgemm_tt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DTT $< -o $(@F)

dgemm_nn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DNN $< -o $(@F)

dgemm_nt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DNT $< -o $(@F)

dgemm_tn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DTN $< -o $(@F)

dgemm_tt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DTT $< -o $(@F)

qgemm_nn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DNN $< -o $(@F)

qgemm_nt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DNT $< -o $(@F)

qgemm_tn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DTN $< -o $(@F)

qgemm_tt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DTT $< -o $(@F)

cgemm_nn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm_nt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm_nr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm_nc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm_tn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm_tt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm_tr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm_tc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm_rn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm_rt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm_rr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm_rc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm_cn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm_ct.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm_cr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm_cc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm_nn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm_nt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm_nr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm_nc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm_tn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm_tt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm_tr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm_tc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm_rn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm_rt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm_rr.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm_rc.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm_cn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm_ct.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm_cr.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm_cc.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm_nn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm_nt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm_nr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm_nc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm_tn.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm_tt.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm_tr.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm_tc.$(SUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm_rn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm_rt.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm_rr.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm_rc.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm_cn.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm_ct.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm_cr.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm_cc.$(SUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

gemm_thread_m.$(SUFFIX) : gemm_thread_m.c ../../common.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

gemm_thread_n.$(SUFFIX) : gemm_thread_n.c ../../common.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

gemm_thread_mn.$(SUFFIX) : gemm_thread_mn.c ../../common.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

gemm_thread_variable.$(SUFFIX) : gemm_thread_variable.c ../../common.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

beta_thread.$(SUFFIX) : beta_thread.c ../../common.h
	$(CC) -c $(CFLAGS) $< -o $(@F)


sgemm_thread_nn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DNN $< -o $(@F)

sgemm_thread_nt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DNT $< -o $(@F)

sgemm_thread_tn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DTN $< -o $(@F)

sgemm_thread_tt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DTT $< -o $(@F)

dgemm_thread_nn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DNN $< -o $(@F)

dgemm_thread_nt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DNT $< -o $(@F)

dgemm_thread_tn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DTN $< -o $(@F)

dgemm_thread_tt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DTT $< -o $(@F)

qgemm_thread_nn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DNN $< -o $(@F)

qgemm_thread_nt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DNT $< -o $(@F)

qgemm_thread_tn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DTN $< -o $(@F)

qgemm_thread_tt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DTT $< -o $(@F)

cgemm_thread_nn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm_thread_nt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm_thread_nr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm_thread_nc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm_thread_tn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm_thread_tt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm_thread_tr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm_thread_tc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm_thread_rn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm_thread_rt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm_thread_rr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm_thread_rc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm_thread_cn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm_thread_ct.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm_thread_cr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm_thread_cc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm_thread_nn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm_thread_nt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm_thread_nr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm_thread_nc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm_thread_tn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm_thread_tt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm_thread_tr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm_thread_tc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm_thread_rn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm_thread_rt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm_thread_rr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm_thread_rc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm_thread_cn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm_thread_ct.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm_thread_cr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm_thread_cc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm_thread_nn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm_thread_nt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm_thread_nr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm_thread_nc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm_thread_tn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm_thread_tt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm_thread_tr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm_thread_tc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm_thread_rn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm_thread_rt.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm_thread_rr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm_thread_rc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm_thread_cn.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm_thread_ct.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm_thread_cr.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm_thread_cc.$(SUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

strmm_LNUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_LNUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_LNLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_LNLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strmm_LTUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_LTUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_LTLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_LTLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

strmm_RNUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_RNUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_RNLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_RNLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strmm_RTUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_RTUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_RTLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_RTLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_LNUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_LNUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_LNLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_LNLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_LTUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_LTUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_LTLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_LTLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_RNUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_RNUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_RNLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_RNLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_RTUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_RTUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_RTLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_RTLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_LNUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_LNUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_LNLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_LNLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_LTUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_LTUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_LTLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_LTLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_RNUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_RNUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_RNLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_RNLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_RTUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_RTUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_RTLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_RTLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

ctrmm_LNUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LNUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LNLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LNLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LTUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LTUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LTLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LTLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LRUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LRUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_LRLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LRLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_LCUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LCUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_LCLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LCLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RNUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RNUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RNLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RNLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RTUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RTUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RTLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RTLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RRUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RRUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RRLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RRLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RCUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RCUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RCLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RCLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LNUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LNUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LNLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LNLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LTUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LTUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LTLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LTLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LRUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LRUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LRLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LRLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LCUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LCUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LCLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LCLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RNUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RNUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RNLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RNLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RTUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RTUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RTLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RTLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RRUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RRUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RRLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RRLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RCUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RCUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RCLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RCLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LNUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LNUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LNLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LNLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LTUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LTUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LTLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LTLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LRUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LRUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LRLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LRLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LCUU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LCUN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LCLU.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LCLN.$(SUFFIX) : trmm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RNUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RNUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RNLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RNLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RTUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RTUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RTLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RTLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RRUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RRUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RRLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RRLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RCUU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RCUN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RCLU.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RCLN.$(SUFFIX) : trmm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ssymm_LU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

ssymm_LL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

ssymm_RU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

ssymm_RL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

dsymm_LU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

dsymm_LL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

dsymm_RU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

dsymm_RL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

qsymm_LU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

qsymm_LL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

qsymm_RU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

qsymm_RL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

csymm_LU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm_LL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm_RU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm_RL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm_LU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm_LL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm_RU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm_RL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm_LU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm_LL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm_RU.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm_RL.$(SUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

ssymm_thread_LU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

ssymm_thread_LL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

ssymm_thread_RU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

ssymm_thread_RL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

dsymm_thread_LU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

dsymm_thread_LL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

dsymm_thread_RU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

dsymm_thread_RL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

qsymm_thread_LU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

qsymm_thread_LL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

qsymm_thread_RU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

qsymm_thread_RL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

csymm_thread_LU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm_thread_LL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm_thread_RU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm_thread_RL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm_thread_LU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm_thread_LL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm_thread_RU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm_thread_RL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm_thread_LU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm_thread_LL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm_thread_RU.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm_thread_RL.$(SUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

ssyrk_UN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

ssyrk_UT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

ssyrk_LN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

ssyrk_LT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

dsyrk_UN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

dsyrk_UT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

dsyrk_LN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

dsyrk_LT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

qsyrk_UN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

qsyrk_UT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

qsyrk_LN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

qsyrk_LT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

csyrk_UN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

csyrk_UT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

csyrk_LN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

csyrk_LT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

zsyrk_UN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

zsyrk_UT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

zsyrk_LN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

zsyrk_LT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

xsyrk_UN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

xsyrk_UT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

xsyrk_LN.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

xsyrk_LT.$(SUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

ssyrk_thread_UN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

ssyrk_thread_UT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

ssyrk_thread_LN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

ssyrk_thread_LT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

dsyrk_thread_UN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

dsyrk_thread_UT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

dsyrk_thread_LN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

dsyrk_thread_LT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

qsyrk_thread_UN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

qsyrk_thread_UT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

qsyrk_thread_LN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

qsyrk_thread_LT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

csyrk_thread_UN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

csyrk_thread_UT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

csyrk_thread_LN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

csyrk_thread_LT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

zsyrk_thread_UN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

zsyrk_thread_UT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

zsyrk_thread_LN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

zsyrk_thread_LT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

xsyrk_thread_UN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

xsyrk_thread_UT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

xsyrk_thread_LN.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

xsyrk_thread_LT.$(SUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

ssyrk_kernel_U.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

ssyrk_kernel_L.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

dsyrk_kernel_U.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

dsyrk_kernel_L.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

qsyrk_kernel_U.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

qsyrk_kernel_L.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

csyrk_kernel_U.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

csyrk_kernel_L.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

zsyrk_kernel_U.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

zsyrk_kernel_L.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

xsyrk_kernel_U.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

xsyrk_kernel_L.$(SUFFIX) : syrk_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

syrk_thread.$(SUFFIX) : syrk_thread.c ../../common.h
	$(CC) -c $(CFLAGS) $< -o $(@F)

ssyr2k_UN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

ssyr2k_UT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

ssyr2k_LN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

ssyr2k_LT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

dsyr2k_UN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

dsyr2k_UT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

dsyr2k_LN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

dsyr2k_LT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

qsyr2k_UN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

qsyr2k_UT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

qsyr2k_LN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

qsyr2k_LT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

csyr2k_UN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

csyr2k_UT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

csyr2k_LN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

csyr2k_LT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

zsyr2k_UN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

zsyr2k_UT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

zsyr2k_LN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

zsyr2k_LT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

xsyr2k_UN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

xsyr2k_UT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

xsyr2k_LN.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

xsyr2k_LT.$(SUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

ssyr2k_kernel_U.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

ssyr2k_kernel_L.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

dsyr2k_kernel_U.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

dsyr2k_kernel_L.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

qsyr2k_kernel_U.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

qsyr2k_kernel_L.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

csyr2k_kernel_U.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

csyr2k_kernel_L.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

zsyr2k_kernel_U.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

zsyr2k_kernel_L.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

xsyr2k_kernel_U.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

xsyr2k_kernel_L.$(SUFFIX) : syr2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

chemm_LU.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm_LL.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm_RU.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

chemm_RL.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

zhemm_LU.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm_LL.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm_RU.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

zhemm_RL.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

xhemm_LU.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm_LL.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm_RU.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

xhemm_RL.$(SUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

chemm_thread_LU.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm_thread_LL.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm_thread_RU.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

chemm_thread_RL.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

zhemm_thread_LU.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm_thread_LL.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm_thread_RU.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

zhemm_thread_RL.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

xhemm_thread_LU.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm_thread_LL.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm_thread_RU.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

xhemm_thread_RL.$(SUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

cherk_UN.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cherk_UC.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cherk_LN.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cherk_LC.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zherk_UN.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zherk_UC.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zherk_LN.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zherk_LC.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xherk_UN.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xherk_UC.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xherk_LN.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xherk_LC.$(SUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cherk_kernel_UN.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

cherk_kernel_UC.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

cherk_kernel_LN.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

cherk_kernel_LC.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -UDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

zherk_kernel_UN.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

zherk_kernel_UC.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

zherk_kernel_LN.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

zherk_kernel_LC.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

xherk_kernel_UN.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

xherk_kernel_UC.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

xherk_kernel_LN.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

xherk_kernel_LC.$(SUFFIX) : zherk_kernel.c
	$(CC) -c $(CFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

cherk_thread_UN.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -ULOWER -UTRANS -UCONJ $< -o $(@F)

cherk_thread_UC.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -ULOWER -DTRANS -DCONJ $< -o $(@F)

cherk_thread_LN.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -DLOWER -UTRANS -UCONJ $< -o $(@F)

cherk_thread_LC.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -DLOWER -DTRANS -DCONJ $< -o $(@F)

zherk_thread_UN.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -ULOWER -UTRANS -UCONJ $< -o $(@F)

zherk_thread_UC.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -ULOWER -DTRANS -DCONJ $< -o $(@F)

zherk_thread_LN.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -DLOWER -UTRANS -UCONJ $< -o $(@F)

zherk_thread_LC.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -DLOWER -DTRANS -DCONJ $< -o $(@F)

xherk_thread_UN.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS -UCONJ $< -o $(@F)

xherk_thread_UC.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS -DCONJ $< -o $(@F)

xherk_thread_LN.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS -UCONJ $< -o $(@F)

xherk_thread_LC.$(SUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS -DCONJ $< -o $(@F)

cher2k_UN.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -UDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cher2k_UC.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -UDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cher2k_LN.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -UDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cher2k_LC.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -UDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zher2k_UN.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zher2k_UC.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zher2k_LN.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zher2k_LC.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xher2k_UN.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DXDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xher2k_UC.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DXDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xher2k_LN.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DXDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xher2k_LC.$(SUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(CFLAGS) -DHER2K -DXDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cher2k_kernel_UN.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

cher2k_kernel_UC.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

cher2k_kernel_LN.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

cher2k_kernel_LC.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

zher2k_kernel_UN.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

zher2k_kernel_UC.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

zher2k_kernel_LN.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

zher2k_kernel_LC.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

xher2k_kernel_UN.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

xher2k_kernel_UC.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

xher2k_kernel_LN.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

xher2k_kernel_LC.$(SUFFIX) : zher2k_kernel.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

cgemm3m_nn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm3m_nt.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm3m_nr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm3m_nc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm3m_tn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm3m_tt.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm3m_tr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm3m_tc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm3m_rn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm3m_rt.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm3m_rr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm3m_rc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm3m_cn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm3m_ct.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm3m_cr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm3m_cc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm3m_nn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm3m_nt.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm3m_nr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm3m_nc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm3m_tn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm3m_tt.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm3m_tr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm3m_tc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm3m_rn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm3m_rt.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm3m_rr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm3m_rc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm3m_cn.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm3m_ct.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm3m_cr.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm3m_cc.$(SUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(CFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm3m_nn.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm3m_nt.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm3m_nr.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm3m_nc.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm3m_tn.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm3m_tt.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm3m_tr.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm3m_tc.$(SUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm3m_rn.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm3m_rt.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm3m_rr.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm3m_rc.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm3m_cn.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm3m_ct.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm3m_cr.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm3m_cc.$(SUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

cgemmf.$(SUFFIX) : zgemmf.c
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX $< -o $(@F)

zgemmf.$(SUFFIX) : zgemmf.c
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX $< -o $(@F)

xgemmf.$(SUFFIX) : zgemmf.c
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX $< -o $(@F)

cgemm3m_thread_nn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm3m_thread_nt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm3m_thread_nr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm3m_thread_nc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm3m_thread_tn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm3m_thread_tt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm3m_thread_tr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm3m_thread_tc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm3m_thread_rn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm3m_thread_rt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm3m_thread_rr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm3m_thread_rc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm3m_thread_cn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm3m_thread_ct.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm3m_thread_cr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm3m_thread_cc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm3m_thread_nn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm3m_thread_nt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm3m_thread_nr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm3m_thread_nc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm3m_thread_tn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm3m_thread_tt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm3m_thread_tr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm3m_thread_tc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm3m_thread_rn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm3m_thread_rt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm3m_thread_rr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm3m_thread_rc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm3m_thread_cn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm3m_thread_ct.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm3m_thread_cr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm3m_thread_cc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm3m_thread_nn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm3m_thread_nt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm3m_thread_nr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm3m_thread_nc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm3m_thread_tn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm3m_thread_tt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm3m_thread_tr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm3m_thread_tc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm3m_thread_rn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm3m_thread_rt.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm3m_thread_rr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm3m_thread_rc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm3m_thread_cn.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm3m_thread_ct.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm3m_thread_cr.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm3m_thread_cc.$(SUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(CFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

csymm3m_LU.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm3m_LL.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm3m_RU.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm3m_RL.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_LU.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_LL.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_RU.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_RL.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_LU.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_LL.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_RU.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_RL.$(SUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

csymm3m_thread_LU.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm3m_thread_LL.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm3m_thread_RU.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm3m_thread_RL.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_thread_LU.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_thread_LL.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_thread_RU.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_thread_RL.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_thread_LU.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_thread_LL.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_thread_RU.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_thread_RL.$(SUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_LU.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm3m_LL.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm3m_RU.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_RL.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_LU.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_LL.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_RU.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_RL.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_LU.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_LL.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_RU.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_RL.$(SUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_thread_LU.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm3m_thread_LL.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm3m_thread_RU.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_thread_RL.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_thread_LU.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_thread_LL.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_thread_RU.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_thread_RL.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_thread_LU.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_thread_LL.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_thread_RU.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_thread_RL.$(SUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(CFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

strsm_LNUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_LNUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_LNLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_LNLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strsm_LTUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_LTUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_LTLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_LTLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

strsm_RNUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_RNUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_RNLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_RNLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strsm_RTUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_RTUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_RTLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_RTLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_LNUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_LNUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_LNLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_LNLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_LTUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_LTUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_LTLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_LTLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_RNUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_RNUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_RNLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_RNLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_RTUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_RTUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_RTLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_RTLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_LNUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_LNUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_LNLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_LNLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_LTUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_LTUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_LTLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_LTLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_RNUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_RNUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_RNLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_RNLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_RTUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_RTUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_RTLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_RTLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

ctrsm_LNUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LNUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LNLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LNLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LTUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LTUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LTLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LTLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LRUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LRUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_LRLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LRLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_LCUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LCUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_LCLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LCLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RNUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RNUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RNLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RNLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RTUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RTUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RTLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RTLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RRUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RRUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RRLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RRLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RCUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RCUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RCLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RCLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)


ztrsm_LNUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LNUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LNLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LNLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LTUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LTUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LTLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LTLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LRUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LRUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_LRLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LRLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_LCUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LCUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_LCLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LCLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RNUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RNUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RNLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RNLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RTUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RTUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RTLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RTLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RRUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RRUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RRLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RRLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RCUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RCUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RCLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RCLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LNUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LNUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LNLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LNLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LTUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LTUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LTLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LTLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LRUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LRUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LRLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LRLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LCUU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LCUN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LCLU.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LCLN.$(SUFFIX) : trsm_L.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RNUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RNUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RNLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RNLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RTUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RTUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RTLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RTLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RRUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RRUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RRLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RRLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RCUU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RCUN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RCLU.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RCLN.$(SUFFIX) : trsm_R.c
	$(CC) -c $(CFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

sgemm_nn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DNN $< -o $(@F)

sgemm_nt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DNT $< -o $(@F)

sgemm_tn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DTN $< -o $(@F)

sgemm_tt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -UCOMPLEX -DTT $< -o $(@F)

dgemm_nn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DNN $< -o $(@F)

dgemm_nt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DNT $< -o $(@F)

dgemm_tn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DTN $< -o $(@F)

dgemm_tt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -UCOMPLEX -DTT $< -o $(@F)

qgemm_nn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DNN $< -o $(@F)

qgemm_nt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DNT $< -o $(@F)

qgemm_tn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DTN $< -o $(@F)

qgemm_tt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -UCOMPLEX -DTT $< -o $(@F)

cgemm_nn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm_nt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm_nr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm_nc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm_tn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm_tt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm_tr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm_tc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm_rn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm_rt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm_rr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm_rc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm_cn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm_ct.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm_cr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm_cc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm_nn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm_nt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm_nr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm_nc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm_tn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm_tt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm_tr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm_tc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm_rn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm_rt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm_rr.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm_rc.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm_cn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm_ct.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm_cr.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm_cc.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm_nn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm_nt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm_nr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm_nc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm_tn.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm_tt.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm_tr.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm_tc.$(PSUFFIX) : gemm.c level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm_rn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm_rt.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm_rr.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm_rc.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm_cn.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm_ct.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm_cr.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm_cc.$(PSUFFIX) : gemm.c level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

gemm_thread_m.$(PSUFFIX) : gemm_thread_m.c ../../common.h
	$(CC) -c $(PFLAGS) $< -o $(@F)

gemm_thread_n.$(PSUFFIX) : gemm_thread_n.c ../../common.h
	$(CC) -c $(PFLAGS) $< -o $(@F)

gemm_thread_mn.$(PSUFFIX) : gemm_thread_mn.c ../../common.h
	$(CC) -c $(PFLAGS) $< -o $(@F)

gemm_thread_variable.$(PSUFFIX) : gemm_thread_variable.c ../../common.h
	$(CC) -c $(PFLAGS) $< -o $(@F)

beta_thread.$(PSUFFIX) : beta_thread.c ../../common.h
	$(CC) -c $(PFLAGS) $< -o $(@F)


sgemm_thread_nn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DNN $< -o $(@F)

sgemm_thread_nt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DNT $< -o $(@F)

sgemm_thread_tn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DTN $< -o $(@F)

sgemm_thread_tt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -UCOMPLEX -DTT $< -o $(@F)

dgemm_thread_nn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DNN $< -o $(@F)

dgemm_thread_nt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DNT $< -o $(@F)

dgemm_thread_tn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DTN $< -o $(@F)

dgemm_thread_tt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -UCOMPLEX -DTT $< -o $(@F)

qgemm_thread_nn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DNN $< -o $(@F)

qgemm_thread_nt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DNT $< -o $(@F)

qgemm_thread_tn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DTN $< -o $(@F)

qgemm_thread_tt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -UCOMPLEX -DTT $< -o $(@F)

cgemm_thread_nn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm_thread_nt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm_thread_nr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm_thread_nc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm_thread_tn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm_thread_tt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm_thread_tr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm_thread_tc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm_thread_rn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm_thread_rt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm_thread_rr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm_thread_rc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm_thread_cn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm_thread_ct.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm_thread_cr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm_thread_cc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm_thread_nn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm_thread_nt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm_thread_nr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm_thread_nc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm_thread_tn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm_thread_tt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm_thread_tr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm_thread_tc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm_thread_rn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm_thread_rt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm_thread_rr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm_thread_rc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm_thread_cn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm_thread_ct.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm_thread_cr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm_thread_cc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm_thread_nn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm_thread_nt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm_thread_nr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm_thread_nc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm_thread_tn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm_thread_tt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm_thread_tr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm_thread_tc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm_thread_rn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm_thread_rt.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm_thread_rr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm_thread_rc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm_thread_cn.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm_thread_ct.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm_thread_cr.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm_thread_cc.$(PSUFFIX) : gemm.c level3_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

strmm_LNUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_LNUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_LNLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_LNLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strmm_LTUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_LTUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_LTLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_LTLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

strmm_RNUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_RNUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_RNLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_RNLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strmm_RTUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strmm_RTUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strmm_RTLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strmm_RTLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_LNUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_LNUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_LNLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_LNLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_LTUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_LTUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_LTLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_LTLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_RNUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_RNUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_RNLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_RNLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrmm_RTUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrmm_RTUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrmm_RTLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrmm_RTLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_LNUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_LNUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_LNLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_LNLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_LTUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_LTUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_LTLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_LTLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_RNUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_RNUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_RNLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_RNLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrmm_RTUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrmm_RTUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrmm_RTLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrmm_RTLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

ctrmm_LNUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LNUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LNLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LNLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LTUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LTUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LTLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_LTLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_LRUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LRUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_LRLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LRLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_LCUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LCUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_LCLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_LCLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RNUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RNUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RNLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RNLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RTUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RTUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RTLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrmm_RTLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrmm_RRUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RRUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RRLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RRLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RCUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RCUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrmm_RCLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrmm_RCLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LNUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LNUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LNLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LNLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LTUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LTUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LTLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_LTLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_LRUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LRUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LRLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LRLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LCUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LCUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_LCLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_LCLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RNUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RNUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RNLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RNLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RTUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RTUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RTLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrmm_RTLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrmm_RRUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RRUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RRLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RRLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RCUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RCUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrmm_RCLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrmm_RCLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LNUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LNUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LNLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LNLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LTUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LTUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LTLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_LTLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_LRUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LRUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LRLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LRLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LCUU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LCUN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_LCLU.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_LCLN.$(PSUFFIX) : trmm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RNUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RNUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RNLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RNLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RTUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RTUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RTLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrmm_RTLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrmm_RRUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RRUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RRLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RRLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RCUU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RCUN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrmm_RCLU.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrmm_RCLN.$(PSUFFIX) : trmm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ssymm_LU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

ssymm_LL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

ssymm_RU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

ssymm_RL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

dsymm_LU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

dsymm_LL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

dsymm_RU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

dsymm_RL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

qsymm_LU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

qsymm_LL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

qsymm_RU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

qsymm_RL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

csymm_LU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm_LL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm_RU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm_RL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm_LU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm_LL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm_RU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm_RL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm_LU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm_LL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm_RU.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm_RL.$(PSUFFIX) : symm_k.c level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

ssymm_thread_LU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

ssymm_thread_LL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

ssymm_thread_RU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

ssymm_thread_RL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

dsymm_thread_LU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

dsymm_thread_LL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

dsymm_thread_RU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

dsymm_thread_RL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

qsymm_thread_LU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

qsymm_thread_LL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

qsymm_thread_RU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

qsymm_thread_RL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

csymm_thread_LU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm_thread_LL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm_thread_RU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm_thread_RL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm_thread_LU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm_thread_LL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm_thread_RU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm_thread_RL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm_thread_LU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm_thread_LL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm_thread_RU.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm_thread_RL.$(PSUFFIX) : symm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

ssyrk_UN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

ssyrk_UT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

ssyrk_LN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

ssyrk_LT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

dsyrk_UN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

dsyrk_UT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

dsyrk_LN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

dsyrk_LT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

qsyrk_UN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

qsyrk_UT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

qsyrk_LN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

qsyrk_LT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

csyrk_UN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

csyrk_UT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

csyrk_LN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

csyrk_LT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

zsyrk_UN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

zsyrk_UT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

zsyrk_LN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

zsyrk_LT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

xsyrk_UN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

xsyrk_UT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

xsyrk_LN.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

xsyrk_LT.$(PSUFFIX) : syrk_k.c level3_syrk.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

ssyrk_thread_UN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

ssyrk_thread_UT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

ssyrk_thread_LN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

ssyrk_thread_LT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

dsyrk_thread_UN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

dsyrk_thread_UT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

dsyrk_thread_LN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

dsyrk_thread_LT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

qsyrk_thread_UN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

qsyrk_thread_UT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

qsyrk_thread_LN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

qsyrk_thread_LT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

csyrk_thread_UN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

csyrk_thread_UT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

csyrk_thread_LN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

csyrk_thread_LT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

zsyrk_thread_UN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

zsyrk_thread_UT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

zsyrk_thread_LN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

zsyrk_thread_LT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

xsyrk_thread_UN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

xsyrk_thread_UT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

xsyrk_thread_LN.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

xsyrk_thread_LT.$(PSUFFIX) : syrk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

ssyrk_kernel_U.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

ssyrk_kernel_L.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

dsyrk_kernel_U.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

dsyrk_kernel_L.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

qsyrk_kernel_U.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

qsyrk_kernel_L.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

csyrk_kernel_U.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

csyrk_kernel_L.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

zsyrk_kernel_U.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

zsyrk_kernel_L.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

xsyrk_kernel_U.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

xsyrk_kernel_L.$(PSUFFIX) : syrk_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

syrk_thread.$(PSUFFIX) : syrk_thread.c ../../common.h
	$(CC) -c $(PFLAGS) $< -o $(@F)

ssyr2k_UN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

ssyr2k_UT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

ssyr2k_LN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

ssyr2k_LT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

dsyr2k_UN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

dsyr2k_UT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

dsyr2k_LN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

dsyr2k_LT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

qsyr2k_UN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -UTRANS $< -o $(@F)

qsyr2k_UT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER -DTRANS $< -o $(@F)

qsyr2k_LN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -UTRANS $< -o $(@F)

qsyr2k_LT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER -DTRANS $< -o $(@F)

csyr2k_UN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

csyr2k_UT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

csyr2k_LN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

csyr2k_LT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

zsyr2k_UN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

zsyr2k_UT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

zsyr2k_LN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

zsyr2k_LT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

xsyr2k_UN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS $< -o $(@F)

xsyr2k_UT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS $< -o $(@F)

xsyr2k_LN.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS $< -o $(@F)

xsyr2k_LT.$(PSUFFIX) : syr2k_k.c level3_syr2k.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS $< -o $(@F)

ssyr2k_kernel_U.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

ssyr2k_kernel_L.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

dsyr2k_kernel_U.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

dsyr2k_kernel_L.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

qsyr2k_kernel_U.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -ULOWER $< -o $(@F)

qsyr2k_kernel_L.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -UCOMPLEX -DLOWER $< -o $(@F)

csyr2k_kernel_U.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

csyr2k_kernel_L.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

zsyr2k_kernel_U.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

zsyr2k_kernel_L.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

xsyr2k_kernel_U.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER $< -o $(@F)

xsyr2k_kernel_L.$(PSUFFIX) : syr2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER $< -o $(@F)

chemm_LU.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm_LL.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm_RU.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

chemm_RL.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

zhemm_LU.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm_LL.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm_RU.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

zhemm_RL.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

xhemm_LU.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm_LL.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm_RU.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

xhemm_RL.$(PSUFFIX) : zhemm_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

chemm_thread_LU.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm_thread_LL.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm_thread_RU.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

chemm_thread_RL.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

zhemm_thread_LU.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm_thread_LL.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm_thread_RU.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

zhemm_thread_RL.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

xhemm_thread_LU.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm_thread_LL.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm_thread_RU.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNC $< -o $(@F)

xhemm_thread_RL.$(PSUFFIX) : zhemm_k.c level3_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNC $< -o $(@F)

cherk_UN.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cherk_UC.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cherk_LN.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cherk_LC.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zherk_UN.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zherk_UC.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zherk_LN.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zherk_LC.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xherk_UN.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xherk_UC.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xherk_LN.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xherk_LC.$(PSUFFIX) : zherk_k.c level3_syrk.c ../../common.h
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cherk_kernel_UN.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

cherk_kernel_UC.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

cherk_kernel_LN.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

cherk_kernel_LC.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -UDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

zherk_kernel_UN.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

zherk_kernel_UC.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

zherk_kernel_LN.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

zherk_kernel_LC.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

xherk_kernel_UN.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

xherk_kernel_UC.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

xherk_kernel_LN.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

xherk_kernel_LC.$(PSUFFIX) : zherk_kernel.c
	$(CC) -c $(PFLAGS) -DHERK -DXDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

cherk_thread_UN.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -ULOWER -UTRANS -UCONJ $< -o $(@F)

cherk_thread_UC.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -ULOWER -DTRANS -DCONJ $< -o $(@F)

cherk_thread_LN.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -DLOWER -UTRANS -UCONJ $< -o $(@F)

cherk_thread_LC.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -UDOUBLE -DCOMPLEX -DLOWER -DTRANS -DCONJ $< -o $(@F)

zherk_thread_UN.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -ULOWER -UTRANS -UCONJ $< -o $(@F)

zherk_thread_UC.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -ULOWER -DTRANS -DCONJ $< -o $(@F)

zherk_thread_LN.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -DLOWER -UTRANS -UCONJ $< -o $(@F)

zherk_thread_LC.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DDOUBLE -DCOMPLEX -DLOWER -DTRANS -DCONJ $< -o $(@F)

xherk_thread_UN.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -ULOWER -UTRANS -UCONJ $< -o $(@F)

xherk_thread_UC.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -ULOWER -DTRANS -DCONJ $< -o $(@F)

xherk_thread_LN.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -DLOWER -UTRANS -UCONJ $< -o $(@F)

xherk_thread_LC.$(PSUFFIX) : zherk_k.c level3_syrk_threaded.c
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DHERK  -DXDOUBLE -DCOMPLEX -DLOWER -DTRANS -DCONJ $< -o $(@F)

cher2k_UN.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -UDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cher2k_UC.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -UDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cher2k_LN.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -UDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

cher2k_LC.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -UDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zher2k_UN.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zher2k_UC.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

zher2k_LN.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

zher2k_LC.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xher2k_UN.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DXDOUBLE -ULOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xher2k_UC.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DXDOUBLE -ULOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

xher2k_LN.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DXDOUBLE -DLOWER -UTRANS -DCOMPLEX -UCONJ $< -o $(@F)

xher2k_LC.$(PSUFFIX) : zher2k_k.c level3_syr2k.c ../../common.h
	$(CC) -c $(PFLAGS) -DHER2K -DXDOUBLE -DLOWER -DTRANS -DCOMPLEX -DCONJ $< -o $(@F)

cher2k_kernel_UN.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

cher2k_kernel_UC.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

cher2k_kernel_LN.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

cher2k_kernel_LC.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

zher2k_kernel_UN.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

zher2k_kernel_UC.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

zher2k_kernel_LN.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

zher2k_kernel_LC.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

xher2k_kernel_UN.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -UCONJ $< -o $(@F)

xher2k_kernel_UC.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DCONJ $< -o $(@F)

xher2k_kernel_LN.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -UCONJ $< -o $(@F)

xher2k_kernel_LC.$(PSUFFIX) : zher2k_kernel.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DCONJ $< -o $(@F)

cgemm3m_nn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm3m_nt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm3m_nr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm3m_nc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm3m_tn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm3m_tt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm3m_tr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm3m_tc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm3m_rn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm3m_rt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm3m_rr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm3m_rc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm3m_cn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm3m_ct.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm3m_cr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm3m_cc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm3m_nn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm3m_nt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm3m_nr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm3m_nc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm3m_tn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm3m_tt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm3m_tr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm3m_tc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm3m_rn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm3m_rt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm3m_rr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm3m_rc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm3m_cn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm3m_ct.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm3m_cr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm3m_cc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c
	$(CC) $(PFLAGS) $(BLOCKS) -c -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm3m_nn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm3m_nt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm3m_nr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm3m_nc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm3m_tn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm3m_tt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm3m_tr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm3m_tc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c  ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm3m_rn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm3m_rt.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm3m_rr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm3m_rc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm3m_cn.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm3m_ct.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm3m_cr.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm3m_cc.$(PSUFFIX) : gemm3m.c gemm3m_level3.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

cgemmf.$(PSUFFIX) : zgemmf.c
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX $< -o $(@F)

zgemmf.$(PSUFFIX) : zgemmf.c
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX $< -o $(@F)

xgemmf.$(PSUFFIX) : zgemmf.c
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX $< -o $(@F)

cgemm3m_thread_nn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNN $< -o $(@F)

cgemm3m_thread_nt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNT $< -o $(@F)

cgemm3m_thread_nr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNR $< -o $(@F)

cgemm3m_thread_nc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DNC $< -o $(@F)

cgemm3m_thread_tn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTN $< -o $(@F)

cgemm3m_thread_tt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTT $< -o $(@F)

cgemm3m_thread_tr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTR $< -o $(@F)

cgemm3m_thread_tc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DTC $< -o $(@F)

cgemm3m_thread_rn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRN $< -o $(@F)

cgemm3m_thread_rt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRT $< -o $(@F)

cgemm3m_thread_rr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRR $< -o $(@F)

cgemm3m_thread_rc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DRC $< -o $(@F)

cgemm3m_thread_cn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCN $< -o $(@F)

cgemm3m_thread_ct.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCT $< -o $(@F)

cgemm3m_thread_cr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

cgemm3m_thread_cc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -UDOUBLE -DCOMPLEX -DCC $< -o $(@F)

zgemm3m_thread_nn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNN $< -o $(@F)

zgemm3m_thread_nt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNT $< -o $(@F)

zgemm3m_thread_nr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNR $< -o $(@F)

zgemm3m_thread_nc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DNC $< -o $(@F)

zgemm3m_thread_tn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTN $< -o $(@F)

zgemm3m_thread_tt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTT $< -o $(@F)

zgemm3m_thread_tr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTR $< -o $(@F)

zgemm3m_thread_tc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DTC $< -o $(@F)

zgemm3m_thread_rn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRN $< -o $(@F)

zgemm3m_thread_rt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRT $< -o $(@F)

zgemm3m_thread_rr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRR $< -o $(@F)

zgemm3m_thread_rc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DRC $< -o $(@F)

zgemm3m_thread_cn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCN $< -o $(@F)

zgemm3m_thread_ct.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCT $< -o $(@F)

zgemm3m_thread_cr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

zgemm3m_thread_cc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DDOUBLE -DCOMPLEX -DCC $< -o $(@F)

xgemm3m_thread_nn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNN $< -o $(@F)

xgemm3m_thread_nt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNT $< -o $(@F)

xgemm3m_thread_nr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNR $< -o $(@F)

xgemm3m_thread_nc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DNC $< -o $(@F)

xgemm3m_thread_tn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTN $< -o $(@F)

xgemm3m_thread_tt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTT $< -o $(@F)

xgemm3m_thread_tr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTR $< -o $(@F)

xgemm3m_thread_tc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DTC $< -o $(@F)

xgemm3m_thread_rn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRN $< -o $(@F)

xgemm3m_thread_rt.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRT $< -o $(@F)

xgemm3m_thread_rr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRR $< -o $(@F)

xgemm3m_thread_rc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DRC $< -o $(@F)

xgemm3m_thread_cn.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCN $< -o $(@F)

xgemm3m_thread_ct.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCT $< -o $(@F)

xgemm3m_thread_cr.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCR=CR $< -o $(@F)

xgemm3m_thread_cc.$(PSUFFIX) : gemm3m.c level3_gemm3m_thread.c ../../param.h
	$(CC) $(PFLAGS) $(BLOCKS) -c -DTHREADED_LEVEL3 -DXDOUBLE -DCOMPLEX -DCC $< -o $(@F)

csymm3m_LU.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm3m_LL.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm3m_RU.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm3m_RL.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_LU.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_LL.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_RU.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_RL.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_LU.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_LL.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_RU.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_RL.$(PSUFFIX) : symm3m_k.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

csymm3m_thread_LU.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

csymm3m_thread_LL.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

csymm3m_thread_RU.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

csymm3m_thread_RL.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_thread_LU.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_thread_LL.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zsymm3m_thread_RU.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zsymm3m_thread_RL.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_thread_LU.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_thread_LL.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xsymm3m_thread_RU.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xsymm3m_thread_RL.$(PSUFFIX) : symm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_LU.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm3m_LL.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm3m_RU.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_RL.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_LU.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_LL.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_RU.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_RL.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_LU.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_LL.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_RU.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_RL.$(PSUFFIX) : hemm3m_k.c gemm3m_level3.c ../../param.h
	$(CC) -c $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_thread_LU.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

chemm3m_thread_LL.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

chemm3m_thread_RU.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

chemm3m_thread_RL.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -UDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_thread_LU.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_thread_LL.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

zhemm3m_thread_RU.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

zhemm3m_thread_RL.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_thread_LU.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_thread_LL.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -URSIDE -DNN $< -o $(@F)

xhemm3m_thread_RU.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -ULOWER -DRSIDE -DNN $< -o $(@F)

xhemm3m_thread_RL.$(PSUFFIX) : hemm3m_k.c level3_gemm3m_thread.c ../../param.h
	$(CC) -c -DTHREADED_LEVEL3 $(PFLAGS) -DXDOUBLE -DCOMPLEX -DLOWER -DRSIDE -DNN $< -o $(@F)

strsm_LNUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_LNUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_LNLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_LNLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strsm_LTUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_LTUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_LTLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_LTLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

strsm_RNUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_RNUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_RNLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_RNLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

strsm_RTUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

strsm_RTUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

strsm_RTLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

strsm_RTLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_LNUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_LNUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_LNLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_LNLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_LTUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_LTUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_LTLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_LTLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_RNUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_RNUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_RNLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_RNLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

dtrsm_RTUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

dtrsm_RTUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

dtrsm_RTLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

dtrsm_RTLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_LNUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_LNUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_LNLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_LNLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_LTUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_LTUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_LTLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_LTLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_RNUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_RNUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_RNLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_RNLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT $< -o $(@F)

qtrsm_RTUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT $< -o $(@F)

qtrsm_RTUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT $< -o $(@F)

qtrsm_RTLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT $< -o $(@F)

qtrsm_RTLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -UCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT $< -o $(@F)

ctrsm_LNUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LNUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LNLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LNLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LTUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LTUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LTLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_LTLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_LRUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LRUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_LRLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LRLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_LCUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LCUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_LCLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_LCLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RNUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RNUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RNLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RNLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RTUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RTUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RTLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ctrsm_RTLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ctrsm_RRUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RRUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RRLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RRLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RCUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RCUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ctrsm_RCLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ctrsm_RCLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -UDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)


ztrsm_LNUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LNUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LNLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LNLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LTUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LTUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LTLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_LTLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_LRUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LRUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_LRLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LRLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_LCUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LCUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_LCLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_LCLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RNUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RNUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RNLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RNLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RTUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RTUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RTLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

ztrsm_RTLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

ztrsm_RRUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RRUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RRLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RRLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RCUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RCUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

ztrsm_RCLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

ztrsm_RCLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LNUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LNUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LNLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LNLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LTUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LTUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LTLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_LTLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_LRUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LRUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LRLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LRLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LCUU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LCUN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_LCLU.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_LCLN.$(PSUFFIX) : trsm_L.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RNUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RNUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RNLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RNLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RTUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RTUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RTLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -UCONJ $< -o $(@F)

xtrsm_RTLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -UCONJ $< -o $(@F)

xtrsm_RRUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RRUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RRLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RRLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -UTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RCUU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RCUN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -DUPPER -UUNIT -DCONJ $< -o $(@F)

xtrsm_RCLU.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -DUNIT -DCONJ $< -o $(@F)

xtrsm_RCLN.$(PSUFFIX) : trsm_R.c
	$(CC) -c $(PFLAGS) -DCOMPLEX -DXDOUBLE -DTRANSA -UUPPER -UUNIT -DCONJ $< -o $(@F)

include ../../Makefile.tail
