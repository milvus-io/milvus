#ifndef COMMON_S_H
#define COMMON_S_H

#ifndef DYNAMIC_ARCH

#define	SAMAX_K			samax_k
#define	SAMIN_K			samin_k
#define	SMAX_K			smax_k
#define	SMIN_K			smin_k
#define	ISAMAX_K		isamax_k
#define	ISAMIN_K		isamin_k
#define	ISMAX_K			ismax_k
#define	ISMIN_K			ismin_k
#define	SASUM_K			sasum_k
#define	SAXPYU_K		saxpy_k
#define	SAXPYC_K		saxpy_k
#define	SCOPY_K			scopy_k
#define	SDOTU_K			sdot_k
#define	SDOTC_K			sdot_k
#define	SDSDOT_K		dsdot_k
#define	DSDOT_K			dsdot_k
#define	SNRM2_K			snrm2_k
#define	SSCAL_K			sscal_k
#define	SSWAP_K			sswap_k
#define	SROT_K			srot_k

#define	SGEMV_N			sgemv_n
#define	SGEMV_T			sgemv_t
#define	SGEMV_R			sgemv_n
#define	SGEMV_C			sgemv_t
#define	SGEMV_O			sgemv_n
#define	SGEMV_U			sgemv_t
#define	SGEMV_S			sgemv_n
#define	SGEMV_D			sgemv_t

#define	SGERU_K			sger_k
#define	SGERC_K			sger_k
#define	SGERV_K			sger_k
#define	SGERD_K			sger_k

#define SSYMV_U			ssymv_U
#define SSYMV_L			ssymv_L

#define SSYMV_THREAD_U		ssymv_thread_U
#define SSYMV_THREAD_L		ssymv_thread_L

#define	SGEMM_ONCOPY		sgemm_oncopy
#define	SGEMM_OTCOPY		sgemm_otcopy

#if SGEMM_DEFAULT_UNROLL_M == SGEMM_DEFAULT_UNROLL_N
#define	SGEMM_INCOPY		sgemm_oncopy
#define	SGEMM_ITCOPY		sgemm_otcopy
#else
#define	SGEMM_INCOPY		sgemm_incopy
#define	SGEMM_ITCOPY		sgemm_itcopy
#endif

#define	STRMM_OUNUCOPY		strmm_ounucopy
#define	STRMM_OUNNCOPY		strmm_ounncopy
#define	STRMM_OUTUCOPY		strmm_outucopy
#define	STRMM_OUTNCOPY		strmm_outncopy
#define	STRMM_OLNUCOPY		strmm_olnucopy
#define	STRMM_OLNNCOPY		strmm_olnncopy
#define	STRMM_OLTUCOPY		strmm_oltucopy
#define	STRMM_OLTNCOPY		strmm_oltncopy

#define	STRSM_OUNUCOPY		strsm_ounucopy
#define	STRSM_OUNNCOPY		strsm_ounncopy
#define	STRSM_OUTUCOPY		strsm_outucopy
#define	STRSM_OUTNCOPY		strsm_outncopy
#define	STRSM_OLNUCOPY		strsm_olnucopy
#define	STRSM_OLNNCOPY		strsm_olnncopy
#define	STRSM_OLTUCOPY		strsm_oltucopy
#define	STRSM_OLTNCOPY		strsm_oltncopy

#if SGEMM_DEFAULT_UNROLL_M == SGEMM_DEFAULT_UNROLL_N
#define	STRMM_IUNUCOPY		strmm_ounucopy
#define	STRMM_IUNNCOPY		strmm_ounncopy
#define	STRMM_IUTUCOPY		strmm_outucopy
#define	STRMM_IUTNCOPY		strmm_outncopy
#define	STRMM_ILNUCOPY		strmm_olnucopy
#define	STRMM_ILNNCOPY		strmm_olnncopy
#define	STRMM_ILTUCOPY		strmm_oltucopy
#define	STRMM_ILTNCOPY		strmm_oltncopy

#define	STRSM_IUNUCOPY		strsm_ounucopy
#define	STRSM_IUNNCOPY		strsm_ounncopy
#define	STRSM_IUTUCOPY		strsm_outucopy
#define	STRSM_IUTNCOPY		strsm_outncopy
#define	STRSM_ILNUCOPY		strsm_olnucopy
#define	STRSM_ILNNCOPY		strsm_olnncopy
#define	STRSM_ILTUCOPY		strsm_oltucopy
#define	STRSM_ILTNCOPY		strsm_oltncopy
#else
#define	STRMM_IUNUCOPY		strmm_iunucopy
#define	STRMM_IUNNCOPY		strmm_iunncopy
#define	STRMM_IUTUCOPY		strmm_iutucopy
#define	STRMM_IUTNCOPY		strmm_iutncopy
#define	STRMM_ILNUCOPY		strmm_ilnucopy
#define	STRMM_ILNNCOPY		strmm_ilnncopy
#define	STRMM_ILTUCOPY		strmm_iltucopy
#define	STRMM_ILTNCOPY		strmm_iltncopy

#define	STRSM_IUNUCOPY		strsm_iunucopy
#define	STRSM_IUNNCOPY		strsm_iunncopy
#define	STRSM_IUTUCOPY		strsm_iutucopy
#define	STRSM_IUTNCOPY		strsm_iutncopy
#define	STRSM_ILNUCOPY		strsm_ilnucopy
#define	STRSM_ILNNCOPY		strsm_ilnncopy
#define	STRSM_ILTUCOPY		strsm_iltucopy
#define	STRSM_ILTNCOPY		strsm_iltncopy
#endif

#define	SGEMM_BETA		sgemm_beta

#define	SGEMM_KERNEL		sgemm_kernel

#define	STRMM_KERNEL_LN		strmm_kernel_LN
#define	STRMM_KERNEL_LT		strmm_kernel_LT
#define	STRMM_KERNEL_LR		strmm_kernel_LN
#define	STRMM_KERNEL_LC		strmm_kernel_LT
#define	STRMM_KERNEL_RN		strmm_kernel_RN
#define	STRMM_KERNEL_RT		strmm_kernel_RT
#define	STRMM_KERNEL_RR		strmm_kernel_RN
#define	STRMM_KERNEL_RC		strmm_kernel_RT

#define	STRSM_KERNEL_LN		strsm_kernel_LN
#define	STRSM_KERNEL_LT		strsm_kernel_LT
#define	STRSM_KERNEL_LR		strsm_kernel_LN
#define	STRSM_KERNEL_LC		strsm_kernel_LT
#define	STRSM_KERNEL_RN		strsm_kernel_RN
#define	STRSM_KERNEL_RT		strsm_kernel_RT
#define	STRSM_KERNEL_RR		strsm_kernel_RN
#define	STRSM_KERNEL_RC		strsm_kernel_RT

#define	SSYMM_OUTCOPY		ssymm_outcopy
#define	SSYMM_OLTCOPY		ssymm_oltcopy
#if SGEMM_DEFAULT_UNROLL_M == SGEMM_DEFAULT_UNROLL_N
#define	SSYMM_IUTCOPY		ssymm_outcopy
#define	SSYMM_ILTCOPY		ssymm_oltcopy
#else
#define	SSYMM_IUTCOPY		ssymm_iutcopy
#define	SSYMM_ILTCOPY		ssymm_iltcopy
#endif

#define SNEG_TCOPY		sneg_tcopy
#define SLASWP_NCOPY		slaswp_ncopy

#define SAXPBY_K                saxpby_k

#define SOMATCOPY_K_CN          somatcopy_k_cn
#define SOMATCOPY_K_RN          somatcopy_k_rn
#define SOMATCOPY_K_CT          somatcopy_k_ct
#define SOMATCOPY_K_RT          somatcopy_k_rt
#define SIMATCOPY_K_CN          simatcopy_k_cn
#define SIMATCOPY_K_RN          simatcopy_k_rn
#define SIMATCOPY_K_CT          simatcopy_k_ct
#define SIMATCOPY_K_RT          simatcopy_k_rt

#define SGEADD_K                sgeadd_k 

#else

#define	SAMAX_K			gotoblas -> samax_k
#define	SAMIN_K			gotoblas -> samin_k
#define	SMAX_K			gotoblas -> smax_k
#define	SMIN_K			gotoblas -> smin_k
#define	ISAMAX_K		gotoblas -> isamax_k
#define	ISAMIN_K		gotoblas -> isamin_k
#define	ISMAX_K			gotoblas -> ismax_k
#define	ISMIN_K			gotoblas -> ismin_k
#define	SASUM_K			gotoblas -> sasum_k
#define	SAXPYU_K		gotoblas -> saxpy_k
#define	SAXPYC_K		gotoblas -> saxpy_k
#define	SCOPY_K			gotoblas -> scopy_k
#define	SDOTU_K			gotoblas -> sdot_k
#define	SDOTC_K			gotoblas -> sdot_k
#define	SDSDOT_K		gotoblas -> dsdot_k
#define	DSDOT_K			gotoblas -> dsdot_k
#define	SNRM2_K			gotoblas -> snrm2_k
#define	SSCAL_K			gotoblas -> sscal_k
#define	SSWAP_K			gotoblas -> sswap_k
#define	SROT_K			gotoblas -> srot_k

#define	SGEMV_N			gotoblas -> sgemv_n
#define	SGEMV_T			gotoblas -> sgemv_t
#define	SGEMV_R			gotoblas -> sgemv_n
#define	SGEMV_C			gotoblas -> sgemv_t
#define	SGEMV_O			gotoblas -> sgemv_n
#define	SGEMV_U			gotoblas -> sgemv_t
#define	SGEMV_S			gotoblas -> sgemv_n
#define	SGEMV_D			gotoblas -> sgemv_t

#define	SGERU_K			gotoblas -> sger_k
#define	SGERC_K			gotoblas -> sger_k
#define	SGERV_K			gotoblas -> sger_k
#define	SGERD_K			gotoblas -> sger_k

#define SSYMV_U			gotoblas -> ssymv_U
#define SSYMV_L			gotoblas -> ssymv_L

#define SSYMV_THREAD_U		ssymv_thread_U
#define SSYMV_THREAD_L		ssymv_thread_L

#define	SGEMM_ONCOPY		gotoblas -> sgemm_oncopy
#define	SGEMM_OTCOPY		gotoblas -> sgemm_otcopy
#define	SGEMM_INCOPY		gotoblas -> sgemm_incopy
#define	SGEMM_ITCOPY		gotoblas -> sgemm_itcopy

#define	STRMM_OUNUCOPY		gotoblas -> strmm_ounucopy
#define	STRMM_OUTUCOPY		gotoblas -> strmm_outucopy
#define	STRMM_OLNUCOPY		gotoblas -> strmm_olnucopy
#define	STRMM_OLTUCOPY		gotoblas -> strmm_oltucopy
#define	STRSM_OUNUCOPY		gotoblas -> strsm_ounucopy
#define	STRSM_OUTUCOPY		gotoblas -> strsm_outucopy
#define	STRSM_OLNUCOPY		gotoblas -> strsm_olnucopy
#define	STRSM_OLTUCOPY		gotoblas -> strsm_oltucopy

#define	STRMM_IUNUCOPY		gotoblas -> strmm_iunucopy
#define	STRMM_IUTUCOPY		gotoblas -> strmm_iutucopy
#define	STRMM_ILNUCOPY		gotoblas -> strmm_ilnucopy
#define	STRMM_ILTUCOPY		gotoblas -> strmm_iltucopy
#define	STRSM_IUNUCOPY		gotoblas -> strsm_iunucopy
#define	STRSM_IUTUCOPY		gotoblas -> strsm_iutucopy
#define	STRSM_ILNUCOPY		gotoblas -> strsm_ilnucopy
#define	STRSM_ILTUCOPY		gotoblas -> strsm_iltucopy

#define	STRMM_OUNNCOPY		gotoblas -> strmm_ounncopy
#define	STRMM_OUTNCOPY		gotoblas -> strmm_outncopy
#define	STRMM_OLNNCOPY		gotoblas -> strmm_olnncopy
#define	STRMM_OLTNCOPY		gotoblas -> strmm_oltncopy
#define	STRSM_OUNNCOPY		gotoblas -> strsm_ounncopy
#define	STRSM_OUTNCOPY		gotoblas -> strsm_outncopy
#define	STRSM_OLNNCOPY		gotoblas -> strsm_olnncopy
#define	STRSM_OLTNCOPY		gotoblas -> strsm_oltncopy

#define	STRMM_IUNNCOPY		gotoblas -> strmm_iunncopy
#define	STRMM_IUTNCOPY		gotoblas -> strmm_iutncopy
#define	STRMM_ILNNCOPY		gotoblas -> strmm_ilnncopy
#define	STRMM_ILTNCOPY		gotoblas -> strmm_iltncopy
#define	STRSM_IUNNCOPY		gotoblas -> strsm_iunncopy
#define	STRSM_IUTNCOPY		gotoblas -> strsm_iutncopy
#define	STRSM_ILNNCOPY		gotoblas -> strsm_ilnncopy
#define	STRSM_ILTNCOPY		gotoblas -> strsm_iltncopy

#define	SGEMM_BETA		gotoblas -> sgemm_beta
#define	SGEMM_KERNEL		gotoblas -> sgemm_kernel

#define	STRMM_KERNEL_LN		gotoblas -> strmm_kernel_LN
#define	STRMM_KERNEL_LT		gotoblas -> strmm_kernel_LT
#define	STRMM_KERNEL_LR		gotoblas -> strmm_kernel_LN
#define	STRMM_KERNEL_LC		gotoblas -> strmm_kernel_LT
#define	STRMM_KERNEL_RN		gotoblas -> strmm_kernel_RN
#define	STRMM_KERNEL_RT		gotoblas -> strmm_kernel_RT
#define	STRMM_KERNEL_RR		gotoblas -> strmm_kernel_RN
#define	STRMM_KERNEL_RC		gotoblas -> strmm_kernel_RT

#define	STRSM_KERNEL_LN		gotoblas -> strsm_kernel_LN
#define	STRSM_KERNEL_LT		gotoblas -> strsm_kernel_LT
#define	STRSM_KERNEL_LR		gotoblas -> strsm_kernel_LN
#define	STRSM_KERNEL_LC		gotoblas -> strsm_kernel_LT
#define	STRSM_KERNEL_RN		gotoblas -> strsm_kernel_RN
#define	STRSM_KERNEL_RT		gotoblas -> strsm_kernel_RT
#define	STRSM_KERNEL_RR		gotoblas -> strsm_kernel_RN
#define	STRSM_KERNEL_RC		gotoblas -> strsm_kernel_RT

#define	SSYMM_IUTCOPY		gotoblas -> ssymm_iutcopy
#define	SSYMM_ILTCOPY		gotoblas -> ssymm_iltcopy
#define	SSYMM_OUTCOPY		gotoblas -> ssymm_outcopy
#define	SSYMM_OLTCOPY		gotoblas -> ssymm_oltcopy

#define SNEG_TCOPY		gotoblas -> sneg_tcopy
#define SLASWP_NCOPY		gotoblas -> slaswp_ncopy

#define SAXPBY_K                gotoblas -> saxpby_k

#define SOMATCOPY_K_CN          gotoblas -> somatcopy_k_cn
#define SOMATCOPY_K_RN          gotoblas -> somatcopy_k_rn
#define SOMATCOPY_K_CT          gotoblas -> somatcopy_k_ct
#define SOMATCOPY_K_RT          gotoblas -> somatcopy_k_rt
#define SIMATCOPY_K_CN          gotoblas -> simatcopy_k_cn
#define SIMATCOPY_K_RN          gotoblas -> simatcopy_k_rn
#define SIMATCOPY_K_CT          gotoblas -> simatcopy_k_ct
#define SIMATCOPY_K_RT          gotoblas -> simatcopy_k_rt

#define SGEADD_K                gotoblas -> sgeadd_k 

#endif

#define	SGEMM_NN		sgemm_nn
#define	SGEMM_CN		sgemm_tn
#define	SGEMM_TN		sgemm_tn
#define	SGEMM_NC		sgemm_nt
#define	SGEMM_NT		sgemm_nt
#define	SGEMM_CC		sgemm_tt
#define	SGEMM_CT		sgemm_tt
#define	SGEMM_TC		sgemm_tt
#define	SGEMM_TT		sgemm_tt
#define	SGEMM_NR		sgemm_nn
#define	SGEMM_TR		sgemm_tn
#define	SGEMM_CR		sgemm_tn
#define	SGEMM_RN		sgemm_nn
#define	SGEMM_RT		sgemm_nt
#define	SGEMM_RC		sgemm_nt
#define	SGEMM_RR		sgemm_nn

#define	SSYMM_LU		ssymm_LU
#define	SSYMM_LL		ssymm_LL
#define	SSYMM_RU		ssymm_RU
#define	SSYMM_RL		ssymm_RL

#define	SHEMM_LU		shemm_LU
#define	SHEMM_LL		shemm_LL
#define	SHEMM_RU		shemm_RU
#define	SHEMM_RL		shemm_RL

#define	SSYRK_UN		ssyrk_UN
#define	SSYRK_UT		ssyrk_UT
#define	SSYRK_LN		ssyrk_LN
#define	SSYRK_LT		ssyrk_LT
#define	SSYRK_UR		ssyrk_UN
#define	SSYRK_UC		ssyrk_UT
#define	SSYRK_LR		ssyrk_LN
#define	SSYRK_LC		ssyrk_LT

#define	SSYRK_KERNEL_U		ssyrk_kernel_U
#define	SSYRK_KERNEL_L		ssyrk_kernel_L

#define	SHERK_UN		ssyrk_UN
#define	SHERK_LN		ssyrk_LN
#define	SHERK_UC		ssyrk_UT
#define	SHERK_LC		ssyrk_LT

#define	SHER2K_UN		ssyr2k_UN
#define	SHER2K_LN		ssyr2k_LN
#define	SHER2K_UC		ssyr2k_UT
#define	SHER2K_LC		ssyr2k_LT

#define	SSYR2K_UN		ssyr2k_UN
#define	SSYR2K_UT		ssyr2k_UT
#define	SSYR2K_LN		ssyr2k_LN
#define	SSYR2K_LT		ssyr2k_LT
#define	SSYR2K_UR		ssyr2k_UN
#define	SSYR2K_UC		ssyr2k_UT
#define	SSYR2K_LR		ssyr2k_LN
#define	SSYR2K_LC		ssyr2k_LT

#define	SSYR2K_KERNEL_U		ssyr2k_kernel_U
#define	SSYR2K_KERNEL_L		ssyr2k_kernel_L

#define	STRMM_LNUU		strmm_LNUU
#define	STRMM_LNUN		strmm_LNUN
#define	STRMM_LNLU		strmm_LNLU
#define	STRMM_LNLN		strmm_LNLN
#define	STRMM_LTUU		strmm_LTUU
#define	STRMM_LTUN		strmm_LTUN
#define	STRMM_LTLU		strmm_LTLU
#define	STRMM_LTLN		strmm_LTLN
#define	STRMM_LRUU		strmm_LNUU
#define	STRMM_LRUN		strmm_LNUN
#define	STRMM_LRLU		strmm_LNLU
#define	STRMM_LRLN		strmm_LNLN
#define	STRMM_LCUU		strmm_LTUU
#define	STRMM_LCUN		strmm_LTUN
#define	STRMM_LCLU		strmm_LTLU
#define	STRMM_LCLN		strmm_LTLN
#define	STRMM_RNUU		strmm_RNUU
#define	STRMM_RNUN		strmm_RNUN
#define	STRMM_RNLU		strmm_RNLU
#define	STRMM_RNLN		strmm_RNLN
#define	STRMM_RTUU		strmm_RTUU
#define	STRMM_RTUN		strmm_RTUN
#define	STRMM_RTLU		strmm_RTLU
#define	STRMM_RTLN		strmm_RTLN
#define	STRMM_RRUU		strmm_RNUU
#define	STRMM_RRUN		strmm_RNUN
#define	STRMM_RRLU		strmm_RNLU
#define	STRMM_RRLN		strmm_RNLN
#define	STRMM_RCUU		strmm_RTUU
#define	STRMM_RCUN		strmm_RTUN
#define	STRMM_RCLU		strmm_RTLU
#define	STRMM_RCLN		strmm_RTLN

#define	STRSM_LNUU		strsm_LNUU
#define	STRSM_LNUN		strsm_LNUN
#define	STRSM_LNLU		strsm_LNLU
#define	STRSM_LNLN		strsm_LNLN
#define	STRSM_LTUU		strsm_LTUU
#define	STRSM_LTUN		strsm_LTUN
#define	STRSM_LTLU		strsm_LTLU
#define	STRSM_LTLN		strsm_LTLN
#define	STRSM_LRUU		strsm_LNUU
#define	STRSM_LRUN		strsm_LNUN
#define	STRSM_LRLU		strsm_LNLU
#define	STRSM_LRLN		strsm_LNLN
#define	STRSM_LCUU		strsm_LTUU
#define	STRSM_LCUN		strsm_LTUN
#define	STRSM_LCLU		strsm_LTLU
#define	STRSM_LCLN		strsm_LTLN
#define	STRSM_RNUU		strsm_RNUU
#define	STRSM_RNUN		strsm_RNUN
#define	STRSM_RNLU		strsm_RNLU
#define	STRSM_RNLN		strsm_RNLN
#define	STRSM_RTUU		strsm_RTUU
#define	STRSM_RTUN		strsm_RTUN
#define	STRSM_RTLU		strsm_RTLU
#define	STRSM_RTLN		strsm_RTLN
#define	STRSM_RRUU		strsm_RNUU
#define	STRSM_RRUN		strsm_RNUN
#define	STRSM_RRLU		strsm_RNLU
#define	STRSM_RRLN		strsm_RNLN
#define	STRSM_RCUU		strsm_RTUU
#define	STRSM_RCUN		strsm_RTUN
#define	STRSM_RCLU		strsm_RTLU
#define	STRSM_RCLN		strsm_RTLN

#define	SGEMM_THREAD_NN		sgemm_thread_nn
#define	SGEMM_THREAD_CN		sgemm_thread_tn
#define	SGEMM_THREAD_TN		sgemm_thread_tn
#define	SGEMM_THREAD_NC		sgemm_thread_nt
#define	SGEMM_THREAD_NT		sgemm_thread_nt
#define	SGEMM_THREAD_CC		sgemm_thread_tt
#define	SGEMM_THREAD_CT		sgemm_thread_tt
#define	SGEMM_THREAD_TC		sgemm_thread_tt
#define	SGEMM_THREAD_TT		sgemm_thread_tt
#define	SGEMM_THREAD_NR		sgemm_thread_nn
#define	SGEMM_THREAD_TR		sgemm_thread_tn
#define	SGEMM_THREAD_CR		sgemm_thread_tn
#define	SGEMM_THREAD_RN		sgemm_thread_nn
#define	SGEMM_THREAD_RT		sgemm_thread_nt
#define	SGEMM_THREAD_RC		sgemm_thread_nt
#define	SGEMM_THREAD_RR		sgemm_thread_nn

#define	SSYMM_THREAD_LU		ssymm_thread_LU
#define	SSYMM_THREAD_LL		ssymm_thread_LL
#define	SSYMM_THREAD_RU		ssymm_thread_RU
#define	SSYMM_THREAD_RL		ssymm_thread_RL

#define	SHEMM_THREAD_LU		shemm_thread_LU
#define	SHEMM_THREAD_LL		shemm_thread_LL
#define	SHEMM_THREAD_RU		shemm_thread_RU
#define	SHEMM_THREAD_RL		shemm_thread_RL

#define	SSYRK_THREAD_UN		ssyrk_thread_UN
#define	SSYRK_THREAD_UT		ssyrk_thread_UT
#define	SSYRK_THREAD_LN		ssyrk_thread_LN
#define	SSYRK_THREAD_LT		ssyrk_thread_LT
#define	SSYRK_THREAD_UR		ssyrk_thread_UN
#define	SSYRK_THREAD_UC		ssyrk_thread_UT
#define	SSYRK_THREAD_LR		ssyrk_thread_LN
#define	SSYRK_THREAD_LC		ssyrk_thread_LT

#define	SHERK_THREAD_UN		ssyrk_thread_UN
#define	SHERK_THREAD_UT		ssyrk_thread_UT
#define	SHERK_THREAD_LN		ssyrk_thread_LN
#define	SHERK_THREAD_LT		ssyrk_thread_LT
#define	SHERK_THREAD_UR		ssyrk_thread_UN
#define	SHERK_THREAD_UC		ssyrk_thread_UT
#define	SHERK_THREAD_LR		ssyrk_thread_LN
#define	SHERK_THREAD_LC		ssyrk_thread_LT

#endif
