#ifndef COMMON_D_H
#define COMMON_D_H

#ifndef DYNAMIC_ARCH

#define	DAMAX_K			damax_k
#define	DAMIN_K			damin_k
#define	DMAX_K			dmax_k
#define	DMIN_K			dmin_k
#define	IDAMAX_K		idamax_k
#define	IDAMIN_K		idamin_k
#define	IDMAX_K			idmax_k
#define	IDMIN_K			idmin_k
#define	DASUM_K			dasum_k
#define	DAXPYU_K		daxpy_k
#define	DAXPYC_K		daxpy_k
#define	DCOPY_K			dcopy_k
#define	DDOTU_K			ddot_k
#define	DDOTC_K			ddot_k
#define	DNRM2_K			dnrm2_k
#define	DSCAL_K			dscal_k
#define	DSWAP_K			dswap_k
#define	DROT_K			drot_k

#define	DGEMV_N			dgemv_n
#define	DGEMV_T			dgemv_t
#define	DGEMV_R			dgemv_n
#define	DGEMV_C			dgemv_t
#define	DGEMV_O			dgemv_n
#define	DGEMV_U			dgemv_t
#define	DGEMV_S			dgemv_n
#define	DGEMV_D			dgemv_t

#define	DGERU_K			dger_k
#define	DGERC_K			dger_k
#define	DGERV_K			dger_k
#define	DGERD_K			dger_k

#define DSYMV_U			dsymv_U
#define DSYMV_L			dsymv_L

#define DSYMV_THREAD_U		dsymv_thread_U
#define DSYMV_THREAD_L		dsymv_thread_L

#define	DGEMM_ONCOPY		dgemm_oncopy
#define	DGEMM_OTCOPY		dgemm_otcopy

#if DGEMM_DEFAULT_UNROLL_M == DGEMM_DEFAULT_UNROLL_N
#define	DGEMM_INCOPY		dgemm_oncopy
#define	DGEMM_ITCOPY		dgemm_otcopy
#else
#define	DGEMM_INCOPY		dgemm_incopy
#define	DGEMM_ITCOPY		dgemm_itcopy
#endif

#define	DTRMM_OUNUCOPY		dtrmm_ounucopy
#define	DTRMM_OUNNCOPY		dtrmm_ounncopy
#define	DTRMM_OUTUCOPY		dtrmm_outucopy
#define	DTRMM_OUTNCOPY		dtrmm_outncopy
#define	DTRMM_OLNUCOPY		dtrmm_olnucopy
#define	DTRMM_OLNNCOPY		dtrmm_olnncopy
#define	DTRMM_OLTUCOPY		dtrmm_oltucopy
#define	DTRMM_OLTNCOPY		dtrmm_oltncopy

#define	DTRSM_OUNUCOPY		dtrsm_ounucopy
#define	DTRSM_OUNNCOPY		dtrsm_ounncopy
#define	DTRSM_OUTUCOPY		dtrsm_outucopy
#define	DTRSM_OUTNCOPY		dtrsm_outncopy
#define	DTRSM_OLNUCOPY		dtrsm_olnucopy
#define	DTRSM_OLNNCOPY		dtrsm_olnncopy
#define	DTRSM_OLTUCOPY		dtrsm_oltucopy
#define	DTRSM_OLTNCOPY		dtrsm_oltncopy

#if DGEMM_DEFAULT_UNROLL_M == DGEMM_DEFAULT_UNROLL_N
#define	DTRMM_IUNUCOPY		dtrmm_ounucopy
#define	DTRMM_IUNNCOPY		dtrmm_ounncopy
#define	DTRMM_IUTUCOPY		dtrmm_outucopy
#define	DTRMM_IUTNCOPY		dtrmm_outncopy
#define	DTRMM_ILNUCOPY		dtrmm_olnucopy
#define	DTRMM_ILNNCOPY		dtrmm_olnncopy
#define	DTRMM_ILTUCOPY		dtrmm_oltucopy
#define	DTRMM_ILTNCOPY		dtrmm_oltncopy

#define	DTRSM_IUNUCOPY		dtrsm_ounucopy
#define	DTRSM_IUNNCOPY		dtrsm_ounncopy
#define	DTRSM_IUTUCOPY		dtrsm_outucopy
#define	DTRSM_IUTNCOPY		dtrsm_outncopy
#define	DTRSM_ILNUCOPY		dtrsm_olnucopy
#define	DTRSM_ILNNCOPY		dtrsm_olnncopy
#define	DTRSM_ILTUCOPY		dtrsm_oltucopy
#define	DTRSM_ILTNCOPY		dtrsm_oltncopy
#else
#define	DTRMM_IUNUCOPY		dtrmm_iunucopy
#define	DTRMM_IUNNCOPY		dtrmm_iunncopy
#define	DTRMM_IUTUCOPY		dtrmm_iutucopy
#define	DTRMM_IUTNCOPY		dtrmm_iutncopy
#define	DTRMM_ILNUCOPY		dtrmm_ilnucopy
#define	DTRMM_ILNNCOPY		dtrmm_ilnncopy
#define	DTRMM_ILTUCOPY		dtrmm_iltucopy
#define	DTRMM_ILTNCOPY		dtrmm_iltncopy

#define	DTRSM_IUNUCOPY		dtrsm_iunucopy
#define	DTRSM_IUNNCOPY		dtrsm_iunncopy
#define	DTRSM_IUTUCOPY		dtrsm_iutucopy
#define	DTRSM_IUTNCOPY		dtrsm_iutncopy
#define	DTRSM_ILNUCOPY		dtrsm_ilnucopy
#define	DTRSM_ILNNCOPY		dtrsm_ilnncopy
#define	DTRSM_ILTUCOPY		dtrsm_iltucopy
#define	DTRSM_ILTNCOPY		dtrsm_iltncopy
#endif

#define	DGEMM_BETA		dgemm_beta

#define	DGEMM_KERNEL		dgemm_kernel

#define	DTRMM_KERNEL_LN		dtrmm_kernel_LN
#define	DTRMM_KERNEL_LT		dtrmm_kernel_LT
#define	DTRMM_KERNEL_LR		dtrmm_kernel_LN
#define	DTRMM_KERNEL_LC		dtrmm_kernel_LT
#define	DTRMM_KERNEL_RN		dtrmm_kernel_RN
#define	DTRMM_KERNEL_RT		dtrmm_kernel_RT
#define	DTRMM_KERNEL_RR		dtrmm_kernel_RN
#define	DTRMM_KERNEL_RC		dtrmm_kernel_RT

#define	DTRSM_KERNEL_LN		dtrsm_kernel_LN
#define	DTRSM_KERNEL_LT		dtrsm_kernel_LT
#define	DTRSM_KERNEL_LR		dtrsm_kernel_LN
#define	DTRSM_KERNEL_LC		dtrsm_kernel_LT
#define	DTRSM_KERNEL_RN		dtrsm_kernel_RN
#define	DTRSM_KERNEL_RT		dtrsm_kernel_RT
#define	DTRSM_KERNEL_RR		dtrsm_kernel_RN
#define	DTRSM_KERNEL_RC		dtrsm_kernel_RT

#define	DSYMM_OUTCOPY		dsymm_outcopy
#define	DSYMM_OLTCOPY		dsymm_oltcopy
#if DGEMM_DEFAULT_UNROLL_M == DGEMM_DEFAULT_UNROLL_N
#define	DSYMM_IUTCOPY		dsymm_outcopy
#define	DSYMM_ILTCOPY		dsymm_oltcopy
#else
#define	DSYMM_IUTCOPY		dsymm_iutcopy
#define	DSYMM_ILTCOPY		dsymm_iltcopy
#endif

#define DNEG_TCOPY		dneg_tcopy
#define DLASWP_NCOPY		dlaswp_ncopy

#define	DAXPBY_K		daxpby_k
#define DOMATCOPY_K_CN		domatcopy_k_cn
#define DOMATCOPY_K_RN		domatcopy_k_rn
#define DOMATCOPY_K_CT		domatcopy_k_ct
#define DOMATCOPY_K_RT		domatcopy_k_rt

#define DIMATCOPY_K_CN		dimatcopy_k_cn
#define DIMATCOPY_K_RN		dimatcopy_k_rn
#define DIMATCOPY_K_CT      dimatcopy_k_ct
#define DIMATCOPY_K_RT      dimatcopy_k_rt
#define DGEADD_K                dgeadd_k 

#else

#define	DAMAX_K			gotoblas -> damax_k
#define	DAMIN_K			gotoblas -> damin_k
#define	DMAX_K			gotoblas -> dmax_k
#define	DMIN_K			gotoblas -> dmin_k
#define	IDAMAX_K		gotoblas -> idamax_k
#define	IDAMIN_K		gotoblas -> idamin_k
#define	IDMAX_K			gotoblas -> idmax_k
#define	IDMIN_K			gotoblas -> idmin_k
#define	DASUM_K			gotoblas -> dasum_k
#define	DAXPYU_K		gotoblas -> daxpy_k
#define	DAXPYC_K		gotoblas -> daxpy_k
#define	DCOPY_K			gotoblas -> dcopy_k
#define	DDOTU_K			gotoblas -> ddot_k
#define	DDOTC_K			gotoblas -> ddot_k
#define	DNRM2_K			gotoblas -> dnrm2_k
#define	DSCAL_K			gotoblas -> dscal_k
#define	DSWAP_K			gotoblas -> dswap_k
#define	DROT_K			gotoblas -> drot_k

#define	DGEMV_N			gotoblas -> dgemv_n
#define	DGEMV_T			gotoblas -> dgemv_t
#define	DGEMV_R			gotoblas -> dgemv_n
#define	DGEMV_C			gotoblas -> dgemv_t
#define	DGEMV_O			gotoblas -> dgemv_n
#define	DGEMV_U			gotoblas -> dgemv_t
#define	DGEMV_S			gotoblas -> dgemv_n
#define	DGEMV_D			gotoblas -> dgemv_t

#define	DGERU_K			gotoblas -> dger_k
#define	DGERC_K			gotoblas -> dger_k
#define	DGERV_K			gotoblas -> dger_k
#define	DGERD_K			gotoblas -> dger_k

#define DSYMV_U			gotoblas -> dsymv_U
#define DSYMV_L			gotoblas -> dsymv_L

#define DSYMV_THREAD_U		dsymv_thread_U
#define DSYMV_THREAD_L		dsymv_thread_L

#define	DGEMM_ONCOPY		gotoblas -> dgemm_oncopy
#define	DGEMM_OTCOPY		gotoblas -> dgemm_otcopy
#define	DGEMM_INCOPY		gotoblas -> dgemm_incopy
#define	DGEMM_ITCOPY		gotoblas -> dgemm_itcopy

#define	DTRMM_OUNUCOPY		gotoblas -> dtrmm_ounucopy
#define	DTRMM_OUTUCOPY		gotoblas -> dtrmm_outucopy
#define	DTRMM_OLNUCOPY		gotoblas -> dtrmm_olnucopy
#define	DTRMM_OLTUCOPY		gotoblas -> dtrmm_oltucopy
#define	DTRSM_OUNUCOPY		gotoblas -> dtrsm_ounucopy
#define	DTRSM_OUTUCOPY		gotoblas -> dtrsm_outucopy
#define	DTRSM_OLNUCOPY		gotoblas -> dtrsm_olnucopy
#define	DTRSM_OLTUCOPY		gotoblas -> dtrsm_oltucopy

#define	DTRMM_IUNUCOPY		gotoblas -> dtrmm_iunucopy
#define	DTRMM_IUTUCOPY		gotoblas -> dtrmm_iutucopy
#define	DTRMM_ILNUCOPY		gotoblas -> dtrmm_ilnucopy
#define	DTRMM_ILTUCOPY		gotoblas -> dtrmm_iltucopy
#define	DTRSM_IUNUCOPY		gotoblas -> dtrsm_iunucopy
#define	DTRSM_IUTUCOPY		gotoblas -> dtrsm_iutucopy
#define	DTRSM_ILNUCOPY		gotoblas -> dtrsm_ilnucopy
#define	DTRSM_ILTUCOPY		gotoblas -> dtrsm_iltucopy

#define	DTRMM_OUNNCOPY		gotoblas -> dtrmm_ounncopy
#define	DTRMM_OUTNCOPY		gotoblas -> dtrmm_outncopy
#define	DTRMM_OLNNCOPY		gotoblas -> dtrmm_olnncopy
#define	DTRMM_OLTNCOPY		gotoblas -> dtrmm_oltncopy
#define	DTRSM_OUNNCOPY		gotoblas -> dtrsm_ounncopy
#define	DTRSM_OUTNCOPY		gotoblas -> dtrsm_outncopy
#define	DTRSM_OLNNCOPY		gotoblas -> dtrsm_olnncopy
#define	DTRSM_OLTNCOPY		gotoblas -> dtrsm_oltncopy

#define	DTRMM_IUNNCOPY		gotoblas -> dtrmm_iunncopy
#define	DTRMM_IUTNCOPY		gotoblas -> dtrmm_iutncopy
#define	DTRMM_ILNNCOPY		gotoblas -> dtrmm_ilnncopy
#define	DTRMM_ILTNCOPY		gotoblas -> dtrmm_iltncopy
#define	DTRSM_IUNNCOPY		gotoblas -> dtrsm_iunncopy
#define	DTRSM_IUTNCOPY		gotoblas -> dtrsm_iutncopy
#define	DTRSM_ILNNCOPY		gotoblas -> dtrsm_ilnncopy
#define	DTRSM_ILTNCOPY		gotoblas -> dtrsm_iltncopy

#define	DGEMM_BETA		gotoblas -> dgemm_beta
#define	DGEMM_KERNEL		gotoblas -> dgemm_kernel

#define	DTRMM_KERNEL_LN		gotoblas -> dtrmm_kernel_LN
#define	DTRMM_KERNEL_LT		gotoblas -> dtrmm_kernel_LT
#define	DTRMM_KERNEL_LR		gotoblas -> dtrmm_kernel_LN
#define	DTRMM_KERNEL_LC		gotoblas -> dtrmm_kernel_LT
#define	DTRMM_KERNEL_RN		gotoblas -> dtrmm_kernel_RN
#define	DTRMM_KERNEL_RT		gotoblas -> dtrmm_kernel_RT
#define	DTRMM_KERNEL_RR		gotoblas -> dtrmm_kernel_RN
#define	DTRMM_KERNEL_RC		gotoblas -> dtrmm_kernel_RT

#define	DTRSM_KERNEL_LN		gotoblas -> dtrsm_kernel_LN
#define	DTRSM_KERNEL_LT		gotoblas -> dtrsm_kernel_LT
#define	DTRSM_KERNEL_LR		gotoblas -> dtrsm_kernel_LN
#define	DTRSM_KERNEL_LC		gotoblas -> dtrsm_kernel_LT
#define	DTRSM_KERNEL_RN		gotoblas -> dtrsm_kernel_RN
#define	DTRSM_KERNEL_RT		gotoblas -> dtrsm_kernel_RT
#define	DTRSM_KERNEL_RR		gotoblas -> dtrsm_kernel_RN
#define	DTRSM_KERNEL_RC		gotoblas -> dtrsm_kernel_RT

#define	DSYMM_IUTCOPY		gotoblas -> dsymm_iutcopy
#define	DSYMM_ILTCOPY		gotoblas -> dsymm_iltcopy
#define	DSYMM_OUTCOPY		gotoblas -> dsymm_outcopy
#define	DSYMM_OLTCOPY		gotoblas -> dsymm_oltcopy

#define DNEG_TCOPY		gotoblas -> dneg_tcopy
#define DLASWP_NCOPY		gotoblas -> dlaswp_ncopy

#define	DAXPBY_K		gotoblas -> daxpby_k
#define DOMATCOPY_K_CN		gotoblas -> domatcopy_k_cn
#define DOMATCOPY_K_RN		gotoblas -> domatcopy_k_rn
#define DOMATCOPY_K_CT		gotoblas -> domatcopy_k_ct
#define DOMATCOPY_K_RT		gotoblas -> domatcopy_k_rt
#define DIMATCOPY_K_CN		gotoblas -> dimatcopy_k_cn
#define DIMATCOPY_K_RN		gotoblas -> dimatcopy_k_rn
#define DIMATCOPY_K_CT		gotoblas -> dimatcopy_k_ct
#define DIMATCOPY_K_RT		gotoblas -> dimatcopy_k_rt

#define DGEADD_K                gotoblas -> dgeadd_k 

#endif

#define	DGEMM_NN		dgemm_nn
#define	DGEMM_CN		dgemm_tn
#define	DGEMM_TN		dgemm_tn
#define	DGEMM_NC		dgemm_nt
#define	DGEMM_NT		dgemm_nt
#define	DGEMM_CC		dgemm_tt
#define	DGEMM_CT		dgemm_tt
#define	DGEMM_TC		dgemm_tt
#define	DGEMM_TT		dgemm_tt
#define	DGEMM_NR		dgemm_nn
#define	DGEMM_TR		dgemm_tn
#define	DGEMM_CR		dgemm_tn
#define	DGEMM_RN		dgemm_nn
#define	DGEMM_RT		dgemm_nt
#define	DGEMM_RC		dgemm_nt
#define	DGEMM_RR		dgemm_nn

#define	DSYMM_LU		dsymm_LU
#define	DSYMM_LL		dsymm_LL
#define	DSYMM_RU		dsymm_RU
#define	DSYMM_RL		dsymm_RL

#define	DHEMM_LU		dhemm_LU
#define	DHEMM_LL		dhemm_LL
#define	DHEMM_RU		dhemm_RU
#define	DHEMM_RL		dhemm_RL

#define	DSYRK_UN		dsyrk_UN
#define	DSYRK_UT		dsyrk_UT
#define	DSYRK_LN		dsyrk_LN
#define	DSYRK_LT		dsyrk_LT
#define	DSYRK_UR		dsyrk_UN
#define	DSYRK_UC		dsyrk_UT
#define	DSYRK_LR		dsyrk_LN
#define	DSYRK_LC		dsyrk_LT

#define	DSYRK_KERNEL_U		dsyrk_kernel_U
#define	DSYRK_KERNEL_L		dsyrk_kernel_L

#define	DHERK_UN		dsyrk_UN
#define	DHERK_LN		dsyrk_LN
#define	DHERK_UC		dsyrk_UT
#define	DHERK_LC		dsyrk_LT

#define	DHER2K_UN		dsyr2k_UN
#define	DHER2K_LN		dsyr2k_LN
#define	DHER2K_UC		dsyr2k_UT
#define	DHER2K_LC		dsyr2k_LT

#define	DSYR2K_UN		dsyr2k_UN
#define	DSYR2K_UT		dsyr2k_UT
#define	DSYR2K_LN		dsyr2k_LN
#define	DSYR2K_LT		dsyr2k_LT
#define	DSYR2K_UR		dsyr2k_UN
#define	DSYR2K_UC		dsyr2k_UT
#define	DSYR2K_LR		dsyr2k_LN
#define	DSYR2K_LC		dsyr2k_LT

#define	DSYR2K_KERNEL_U		dsyr2k_kernel_U
#define	DSYR2K_KERNEL_L		dsyr2k_kernel_L

#define	DTRMM_LNUU		dtrmm_LNUU
#define	DTRMM_LNUN		dtrmm_LNUN
#define	DTRMM_LNLU		dtrmm_LNLU
#define	DTRMM_LNLN		dtrmm_LNLN
#define	DTRMM_LTUU		dtrmm_LTUU
#define	DTRMM_LTUN		dtrmm_LTUN
#define	DTRMM_LTLU		dtrmm_LTLU
#define	DTRMM_LTLN		dtrmm_LTLN
#define	DTRMM_LRUU		dtrmm_LNUU
#define	DTRMM_LRUN		dtrmm_LNUN
#define	DTRMM_LRLU		dtrmm_LNLU
#define	DTRMM_LRLN		dtrmm_LNLN
#define	DTRMM_LCUU		dtrmm_LTUU
#define	DTRMM_LCUN		dtrmm_LTUN
#define	DTRMM_LCLU		dtrmm_LTLU
#define	DTRMM_LCLN		dtrmm_LTLN
#define	DTRMM_RNUU		dtrmm_RNUU
#define	DTRMM_RNUN		dtrmm_RNUN
#define	DTRMM_RNLU		dtrmm_RNLU
#define	DTRMM_RNLN		dtrmm_RNLN
#define	DTRMM_RTUU		dtrmm_RTUU
#define	DTRMM_RTUN		dtrmm_RTUN
#define	DTRMM_RTLU		dtrmm_RTLU
#define	DTRMM_RTLN		dtrmm_RTLN
#define	DTRMM_RRUU		dtrmm_RNUU
#define	DTRMM_RRUN		dtrmm_RNUN
#define	DTRMM_RRLU		dtrmm_RNLU
#define	DTRMM_RRLN		dtrmm_RNLN
#define	DTRMM_RCUU		dtrmm_RTUU
#define	DTRMM_RCUN		dtrmm_RTUN
#define	DTRMM_RCLU		dtrmm_RTLU
#define	DTRMM_RCLN		dtrmm_RTLN

#define	DTRSM_LNUU		dtrsm_LNUU
#define	DTRSM_LNUN		dtrsm_LNUN
#define	DTRSM_LNLU		dtrsm_LNLU
#define	DTRSM_LNLN		dtrsm_LNLN
#define	DTRSM_LTUU		dtrsm_LTUU
#define	DTRSM_LTUN		dtrsm_LTUN
#define	DTRSM_LTLU		dtrsm_LTLU
#define	DTRSM_LTLN		dtrsm_LTLN
#define	DTRSM_LRUU		dtrsm_LNUU
#define	DTRSM_LRUN		dtrsm_LNUN
#define	DTRSM_LRLU		dtrsm_LNLU
#define	DTRSM_LRLN		dtrsm_LNLN
#define	DTRSM_LCUU		dtrsm_LTUU
#define	DTRSM_LCUN		dtrsm_LTUN
#define	DTRSM_LCLU		dtrsm_LTLU
#define	DTRSM_LCLN		dtrsm_LTLN
#define	DTRSM_RNUU		dtrsm_RNUU
#define	DTRSM_RNUN		dtrsm_RNUN
#define	DTRSM_RNLU		dtrsm_RNLU
#define	DTRSM_RNLN		dtrsm_RNLN
#define	DTRSM_RTUU		dtrsm_RTUU
#define	DTRSM_RTUN		dtrsm_RTUN
#define	DTRSM_RTLU		dtrsm_RTLU
#define	DTRSM_RTLN		dtrsm_RTLN
#define	DTRSM_RRUU		dtrsm_RNUU
#define	DTRSM_RRUN		dtrsm_RNUN
#define	DTRSM_RRLU		dtrsm_RNLU
#define	DTRSM_RRLN		dtrsm_RNLN
#define	DTRSM_RCUU		dtrsm_RTUU
#define	DTRSM_RCUN		dtrsm_RTUN
#define	DTRSM_RCLU		dtrsm_RTLU
#define	DTRSM_RCLN		dtrsm_RTLN

#define	DGEMM_THREAD_NN		dgemm_thread_nn
#define	DGEMM_THREAD_CN		dgemm_thread_tn
#define	DGEMM_THREAD_TN		dgemm_thread_tn
#define	DGEMM_THREAD_NC		dgemm_thread_nt
#define	DGEMM_THREAD_NT		dgemm_thread_nt
#define	DGEMM_THREAD_CC		dgemm_thread_tt
#define	DGEMM_THREAD_CT		dgemm_thread_tt
#define	DGEMM_THREAD_TC		dgemm_thread_tt
#define	DGEMM_THREAD_TT		dgemm_thread_tt
#define	DGEMM_THREAD_NR		dgemm_thread_nn
#define	DGEMM_THREAD_TR		dgemm_thread_tn
#define	DGEMM_THREAD_CR		dgemm_thread_tn
#define	DGEMM_THREAD_RN		dgemm_thread_nn
#define	DGEMM_THREAD_RT		dgemm_thread_nt
#define	DGEMM_THREAD_RC		dgemm_thread_nt
#define	DGEMM_THREAD_RR		dgemm_thread_nn

#define	DSYMM_THREAD_LU		dsymm_thread_LU
#define	DSYMM_THREAD_LL		dsymm_thread_LL
#define	DSYMM_THREAD_RU		dsymm_thread_RU
#define	DSYMM_THREAD_RL		dsymm_thread_RL

#define	DHEMM_THREAD_LU		dhemm_thread_LU
#define	DHEMM_THREAD_LL		dhemm_thread_LL
#define	DHEMM_THREAD_RU		dhemm_thread_RU
#define	DHEMM_THREAD_RL		dhemm_thread_RL

#define	DSYRK_THREAD_UN		dsyrk_thread_UN
#define	DSYRK_THREAD_UT		dsyrk_thread_UT
#define	DSYRK_THREAD_LN		dsyrk_thread_LN
#define	DSYRK_THREAD_LT		dsyrk_thread_LT
#define	DSYRK_THREAD_UR		dsyrk_thread_UN
#define	DSYRK_THREAD_UC		dsyrk_thread_UT
#define	DSYRK_THREAD_LR		dsyrk_thread_LN
#define	DSYRK_THREAD_LC		dsyrk_thread_LT

#define	DHERK_THREAD_UN		dsyrk_thread_UN
#define	DHERK_THREAD_UT		dsyrk_thread_UT
#define	DHERK_THREAD_LN		dsyrk_thread_LN
#define	DHERK_THREAD_LT		dsyrk_thread_LT
#define	DHERK_THREAD_UR		dsyrk_thread_UN
#define	DHERK_THREAD_UC		dsyrk_thread_UT
#define	DHERK_THREAD_LR		dsyrk_thread_LN
#define	DHERK_THREAD_LC		dsyrk_thread_LT

#endif
