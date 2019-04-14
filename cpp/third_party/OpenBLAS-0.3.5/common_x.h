#ifndef COMMON_X_H
#define COMMON_X_H

#ifndef DYNAMIC_ARCH

#define	XAMAX_K			xamax_k
#define	XAMIN_K			xamin_k
#define	XMAX_K			xmax_k
#define	XMIN_K			xmin_k
#define	IXAMAX_K		ixamax_k
#define	IXAMIN_K		ixamin_k
#define	IXMAX_K			ixmax_k
#define	IXMIN_K			ixmin_k
#define	XASUM_K			xasum_k
#define	XAXPYU_K		xaxpy_k
#define	XAXPYC_K		xaxpyc_k
#define	XCOPY_K			xcopy_k
#define	XDOTU_K			xdotu_k
#define	XDOTC_K			xdotc_k
#define	XNRM2_K			xnrm2_k
#define	XSCAL_K			xscal_k
#define	XSWAP_K			xswap_k
#define	XROT_K			xqrot_k

#define	XGEMV_N			xgemv_n
#define	XGEMV_T			xgemv_t
#define	XGEMV_R			xgemv_r
#define	XGEMV_C			xgemv_c
#define	XGEMV_O			xgemv_o
#define	XGEMV_U			xgemv_u
#define	XGEMV_S			xgemv_s
#define	XGEMV_D			xgemv_d

#define	XGERU_K			xgeru_k
#define	XGERC_K			xgerc_k
#define	XGERV_K			xgerv_k
#define	XGERD_K			xgerd_k

#define XSYMV_U			xsymv_U
#define XSYMV_L			xsymv_L
#define XHEMV_U			xhemv_U
#define XHEMV_L			xhemv_L
#define XHEMV_V			xhemv_V
#define XHEMV_M			xhemv_M

#define XSYMV_THREAD_U		xsymv_thread_U
#define XSYMV_THREAD_L		xsymv_thread_L
#define XHEMV_THREAD_U		xhemv_thread_U
#define XHEMV_THREAD_L		xhemv_thread_L
#define XHEMV_THREAD_V		xhemv_thread_V
#define XHEMV_THREAD_M		xhemv_thread_M

#define	XGEMM_ONCOPY		xgemm_oncopy
#define	XGEMM_OTCOPY		xgemm_otcopy

#if XGEMM_DEFAULT_UNROLL_M == XGEMM_DEFAULT_UNROLL_N
#define	XGEMM_INCOPY		xgemm_oncopy
#define	XGEMM_ITCOPY		xgemm_otcopy
#else
#define	XGEMM_INCOPY		xgemm_incopy
#define	XGEMM_ITCOPY		xgemm_itcopy
#endif

#define	XTRMM_OUNUCOPY		xtrmm_ounucopy
#define	XTRMM_OUNNCOPY		xtrmm_ounncopy
#define	XTRMM_OUTUCOPY		xtrmm_outucopy
#define	XTRMM_OUTNCOPY		xtrmm_outncopy
#define	XTRMM_OLNUCOPY		xtrmm_olnucopy
#define	XTRMM_OLNNCOPY		xtrmm_olnncopy
#define	XTRMM_OLTUCOPY		xtrmm_oltucopy
#define	XTRMM_OLTNCOPY		xtrmm_oltncopy

#define	XTRSM_OUNUCOPY		xtrsm_ounucopy
#define	XTRSM_OUNNCOPY		xtrsm_ounncopy
#define	XTRSM_OUTUCOPY		xtrsm_outucopy
#define	XTRSM_OUTNCOPY		xtrsm_outncopy
#define	XTRSM_OLNUCOPY		xtrsm_olnucopy
#define	XTRSM_OLNNCOPY		xtrsm_olnncopy
#define	XTRSM_OLTUCOPY		xtrsm_oltucopy
#define	XTRSM_OLTNCOPY		xtrsm_oltncopy

#if XGEMM_DEFAULT_UNROLL_M == XGEMM_DEFAULT_UNROLL_N
#define	XTRMM_IUNUCOPY		xtrmm_ounucopy
#define	XTRMM_IUNNCOPY		xtrmm_ounncopy
#define	XTRMM_IUTUCOPY		xtrmm_outucopy
#define	XTRMM_IUTNCOPY		xtrmm_outncopy
#define	XTRMM_ILNUCOPY		xtrmm_olnucopy
#define	XTRMM_ILNNCOPY		xtrmm_olnncopy
#define	XTRMM_ILTUCOPY		xtrmm_oltucopy
#define	XTRMM_ILTNCOPY		xtrmm_oltncopy

#define	XTRSM_IUNUCOPY		xtrsm_ounucopy
#define	XTRSM_IUNNCOPY		xtrsm_ounncopy
#define	XTRSM_IUTUCOPY		xtrsm_outucopy
#define	XTRSM_IUTNCOPY		xtrsm_outncopy
#define	XTRSM_ILNUCOPY		xtrsm_olnucopy
#define	XTRSM_ILNNCOPY		xtrsm_olnncopy
#define	XTRSM_ILTUCOPY		xtrsm_oltucopy
#define	XTRSM_ILTNCOPY		xtrsm_oltncopy
#else
#define	XTRMM_IUNUCOPY		xtrmm_iunucopy
#define	XTRMM_IUNNCOPY		xtrmm_iunncopy
#define	XTRMM_IUTUCOPY		xtrmm_iutucopy
#define	XTRMM_IUTNCOPY		xtrmm_iutncopy
#define	XTRMM_ILNUCOPY		xtrmm_ilnucopy
#define	XTRMM_ILNNCOPY		xtrmm_ilnncopy
#define	XTRMM_ILTUCOPY		xtrmm_iltucopy
#define	XTRMM_ILTNCOPY		xtrmm_iltncopy

#define	XTRSM_IUNUCOPY		xtrsm_iunucopy
#define	XTRSM_IUNNCOPY		xtrsm_iunncopy
#define	XTRSM_IUTUCOPY		xtrsm_iutucopy
#define	XTRSM_IUTNCOPY		xtrsm_iutncopy
#define	XTRSM_ILNUCOPY		xtrsm_ilnucopy
#define	XTRSM_ILNNCOPY		xtrsm_ilnncopy
#define	XTRSM_ILTUCOPY		xtrsm_iltucopy
#define	XTRSM_ILTNCOPY		xtrsm_iltncopy
#endif

#define	XGEMM_BETA		xgemm_beta

#define	XGEMM_KERNEL_N		xgemm_kernel_n
#define	XGEMM_KERNEL_L		xgemm_kernel_l
#define	XGEMM_KERNEL_R		xgemm_kernel_r
#define	XGEMM_KERNEL_B		xgemm_kernel_b

#define	XTRMM_KERNEL_LN		xtrmm_kernel_LN
#define	XTRMM_KERNEL_LT		xtrmm_kernel_LT
#define	XTRMM_KERNEL_LR		xtrmm_kernel_LR
#define	XTRMM_KERNEL_LC		xtrmm_kernel_LC
#define	XTRMM_KERNEL_RN		xtrmm_kernel_RN
#define	XTRMM_KERNEL_RT		xtrmm_kernel_RT
#define	XTRMM_KERNEL_RR		xtrmm_kernel_RR
#define	XTRMM_KERNEL_RC		xtrmm_kernel_RC

#define	XTRSM_KERNEL_LN		xtrsm_kernel_LN
#define	XTRSM_KERNEL_LT		xtrsm_kernel_LT
#define	XTRSM_KERNEL_LR		xtrsm_kernel_LR
#define	XTRSM_KERNEL_LC		xtrsm_kernel_LC
#define	XTRSM_KERNEL_RN		xtrsm_kernel_RN
#define	XTRSM_KERNEL_RT		xtrsm_kernel_RT
#define	XTRSM_KERNEL_RR		xtrsm_kernel_RR
#define	XTRSM_KERNEL_RC		xtrsm_kernel_RC

#define	XSYMM_OUTCOPY		xsymm_outcopy
#define	XSYMM_OLTCOPY		xsymm_oltcopy
#if XGEMM_DEFAULT_UNROLL_M == XGEMM_DEFAULT_UNROLL_N
#define	XSYMM_IUTCOPY		xsymm_outcopy
#define	XSYMM_ILTCOPY		xsymm_oltcopy
#else
#define	XSYMM_IUTCOPY		xsymm_iutcopy
#define	XSYMM_ILTCOPY		xsymm_iltcopy
#endif

#define	XHEMM_OUTCOPY		xhemm_outcopy
#define	XHEMM_OLTCOPY		xhemm_oltcopy
#if XGEMM_DEFAULT_UNROLL_M == XGEMM_DEFAULT_UNROLL_N
#define	XHEMM_IUTCOPY		xhemm_outcopy
#define	XHEMM_ILTCOPY		xhemm_oltcopy
#else
#define	XHEMM_IUTCOPY		xhemm_iutcopy
#define	XHEMM_ILTCOPY		xhemm_iltcopy
#endif

#define	XGEMM3M_ONCOPYB		xgemm3m_oncopyb
#define	XGEMM3M_ONCOPYR		xgemm3m_oncopyr
#define	XGEMM3M_ONCOPYI		xgemm3m_oncopyi
#define	XGEMM3M_OTCOPYB		xgemm3m_otcopyb
#define	XGEMM3M_OTCOPYR		xgemm3m_otcopyr
#define	XGEMM3M_OTCOPYI		xgemm3m_otcopyi

#define	XGEMM3M_INCOPYB		xgemm3m_incopyb
#define	XGEMM3M_INCOPYR		xgemm3m_incopyr
#define	XGEMM3M_INCOPYI		xgemm3m_incopyi
#define	XGEMM3M_ITCOPYB		xgemm3m_itcopyb
#define	XGEMM3M_ITCOPYR		xgemm3m_itcopyr
#define	XGEMM3M_ITCOPYI		xgemm3m_itcopyi

#define	XSYMM3M_ILCOPYB		xsymm3m_ilcopyb
#define	XSYMM3M_IUCOPYB		xsymm3m_iucopyb
#define	XSYMM3M_ILCOPYR		xsymm3m_ilcopyr
#define	XSYMM3M_IUCOPYR		xsymm3m_iucopyr
#define	XSYMM3M_ILCOPYI		xsymm3m_ilcopyi
#define	XSYMM3M_IUCOPYI		xsymm3m_iucopyi

#define	XSYMM3M_OLCOPYB		xsymm3m_olcopyb
#define	XSYMM3M_OUCOPYB		xsymm3m_oucopyb
#define	XSYMM3M_OLCOPYR		xsymm3m_olcopyr
#define	XSYMM3M_OUCOPYR		xsymm3m_oucopyr
#define	XSYMM3M_OLCOPYI		xsymm3m_olcopyi
#define	XSYMM3M_OUCOPYI		xsymm3m_oucopyi

#define	XHEMM3M_ILCOPYB		xhemm3m_ilcopyb
#define	XHEMM3M_IUCOPYB		xhemm3m_iucopyb
#define	XHEMM3M_ILCOPYR		xhemm3m_ilcopyr
#define	XHEMM3M_IUCOPYR		xhemm3m_iucopyr
#define	XHEMM3M_ILCOPYI		xhemm3m_ilcopyi
#define	XHEMM3M_IUCOPYI		xhemm3m_iucopyi

#define	XHEMM3M_OLCOPYB		xhemm3m_olcopyb
#define	XHEMM3M_OUCOPYB		xhemm3m_oucopyb
#define	XHEMM3M_OLCOPYR		xhemm3m_olcopyr
#define	XHEMM3M_OUCOPYR		xhemm3m_oucopyr
#define	XHEMM3M_OLCOPYI		xhemm3m_olcopyi
#define	XHEMM3M_OUCOPYI		xhemm3m_oucopyi

#define	XGEMM3M_KERNEL		xgemm3m_kernel

#define XNEG_TCOPY		xneg_tcopy
#define XLASWP_NCOPY		xlaswp_ncopy

#else

#define	XAMAX_K			gotoblas -> xamax_k
#define	XAMIN_K			gotoblas -> xamin_k
#define	XMAX_K			gotoblas -> xmax_k
#define	XMIN_K			gotoblas -> xmin_k
#define	IXAMAX_K		gotoblas -> ixamax_k
#define	IXAMIN_K		gotoblas -> ixamin_k
#define	IXMAX_K			gotoblas -> ixmax_k
#define	IXMIN_K			gotoblas -> ixmin_k
#define	XASUM_K			gotoblas -> xasum_k
#define	XAXPYU_K		gotoblas -> xaxpy_k
#define	XAXPYC_K		gotoblas -> xaxpyc_k
#define	XCOPY_K			gotoblas -> xcopy_k
#define	XDOTU_K			gotoblas -> xdotu_k
#define	XDOTC_K			gotoblas -> xdotc_k
#define	XNRM2_K			gotoblas -> xnrm2_k
#define	XSCAL_K			gotoblas -> xscal_k
#define	XSWAP_K			gotoblas -> xswap_k
#define	XROT_K			gotoblas -> xqrot_k

#define	XGEMV_N			gotoblas -> xgemv_n
#define	XGEMV_T			gotoblas -> xgemv_t
#define	XGEMV_R			gotoblas -> xgemv_r
#define	XGEMV_C			gotoblas -> xgemv_c
#define	XGEMV_O			gotoblas -> xgemv_o
#define	XGEMV_U			gotoblas -> xgemv_u
#define	XGEMV_S			gotoblas -> xgemv_s
#define	XGEMV_D			gotoblas -> xgemv_d

#define	XGERU_K			gotoblas -> xgeru_k
#define	XGERC_K			gotoblas -> xgerc_k
#define	XGERV_K			gotoblas -> xgerv_k
#define	XGERD_K			gotoblas -> xgerd_k

#define XSYMV_U			gotoblas -> xsymv_U
#define XSYMV_L			gotoblas -> xsymv_L
#define XHEMV_U			gotoblas -> xhemv_U
#define XHEMV_L			gotoblas -> xhemv_L
#define XHEMV_V			gotoblas -> xhemv_V
#define XHEMV_M			gotoblas -> xhemv_M

#define XSYMV_THREAD_U		xsymv_thread_U
#define XSYMV_THREAD_L		xsymv_thread_L
#define XHEMV_THREAD_U		xhemv_thread_U
#define XHEMV_THREAD_L		xhemv_thread_L
#define XHEMV_THREAD_V		xhemv_thread_V
#define XHEMV_THREAD_M		xhemv_thread_M

#define	XGEMM_ONCOPY		gotoblas -> xgemm_oncopy
#define	XGEMM_OTCOPY		gotoblas -> xgemm_otcopy
#define	XGEMM_INCOPY		gotoblas -> xgemm_incopy
#define	XGEMM_ITCOPY		gotoblas -> xgemm_itcopy

#define	XTRMM_OUNUCOPY		gotoblas -> xtrmm_ounucopy
#define	XTRMM_OUTUCOPY		gotoblas -> xtrmm_outucopy
#define	XTRMM_OLNUCOPY		gotoblas -> xtrmm_olnucopy
#define	XTRMM_OLTUCOPY		gotoblas -> xtrmm_oltucopy
#define	XTRSM_OUNUCOPY		gotoblas -> xtrsm_ounucopy
#define	XTRSM_OUTUCOPY		gotoblas -> xtrsm_outucopy
#define	XTRSM_OLNUCOPY		gotoblas -> xtrsm_olnucopy
#define	XTRSM_OLTUCOPY		gotoblas -> xtrsm_oltucopy

#define	XTRMM_IUNUCOPY		gotoblas -> xtrmm_iunucopy
#define	XTRMM_IUTUCOPY		gotoblas -> xtrmm_iutucopy
#define	XTRMM_ILNUCOPY		gotoblas -> xtrmm_ilnucopy
#define	XTRMM_ILTUCOPY		gotoblas -> xtrmm_iltucopy
#define	XTRSM_IUNUCOPY		gotoblas -> xtrsm_iunucopy
#define	XTRSM_IUTUCOPY		gotoblas -> xtrsm_iutucopy
#define	XTRSM_ILNUCOPY		gotoblas -> xtrsm_ilnucopy
#define	XTRSM_ILTUCOPY		gotoblas -> xtrsm_iltucopy

#define	XTRMM_OUNNCOPY		gotoblas -> xtrmm_ounncopy
#define	XTRMM_OUTNCOPY		gotoblas -> xtrmm_outncopy
#define	XTRMM_OLNNCOPY		gotoblas -> xtrmm_olnncopy
#define	XTRMM_OLTNCOPY		gotoblas -> xtrmm_oltncopy
#define	XTRSM_OUNNCOPY		gotoblas -> xtrsm_ounncopy
#define	XTRSM_OUTNCOPY		gotoblas -> xtrsm_outncopy
#define	XTRSM_OLNNCOPY		gotoblas -> xtrsm_olnncopy
#define	XTRSM_OLTNCOPY		gotoblas -> xtrsm_oltncopy

#define	XTRMM_IUNNCOPY		gotoblas -> xtrmm_iunncopy
#define	XTRMM_IUTNCOPY		gotoblas -> xtrmm_iutncopy
#define	XTRMM_ILNNCOPY		gotoblas -> xtrmm_ilnncopy
#define	XTRMM_ILTNCOPY		gotoblas -> xtrmm_iltncopy
#define	XTRSM_IUNNCOPY		gotoblas -> xtrsm_iunncopy
#define	XTRSM_IUTNCOPY		gotoblas -> xtrsm_iutncopy
#define	XTRSM_ILNNCOPY		gotoblas -> xtrsm_ilnncopy
#define	XTRSM_ILTNCOPY		gotoblas -> xtrsm_iltncopy

#define	XGEMM_BETA		gotoblas -> xgemm_beta
#define	XGEMM_KERNEL_N		gotoblas -> xgemm_kernel_n
#define	XGEMM_KERNEL_L		gotoblas -> xgemm_kernel_l
#define	XGEMM_KERNEL_R		gotoblas -> xgemm_kernel_r
#define	XGEMM_KERNEL_B		gotoblas -> xgemm_kernel_b

#define	XTRMM_KERNEL_LN		gotoblas -> xtrmm_kernel_LN
#define	XTRMM_KERNEL_LT		gotoblas -> xtrmm_kernel_LT
#define	XTRMM_KERNEL_LR		gotoblas -> xtrmm_kernel_LR
#define	XTRMM_KERNEL_LC		gotoblas -> xtrmm_kernel_LC
#define	XTRMM_KERNEL_RN		gotoblas -> xtrmm_kernel_RN
#define	XTRMM_KERNEL_RT		gotoblas -> xtrmm_kernel_RT
#define	XTRMM_KERNEL_RR		gotoblas -> xtrmm_kernel_RR
#define	XTRMM_KERNEL_RC		gotoblas -> xtrmm_kernel_RC

#define	XTRSM_KERNEL_LN		gotoblas -> xtrsm_kernel_LN
#define	XTRSM_KERNEL_LT		gotoblas -> xtrsm_kernel_LT
#define	XTRSM_KERNEL_LR		gotoblas -> xtrsm_kernel_LR
#define	XTRSM_KERNEL_LC		gotoblas -> xtrsm_kernel_LC
#define	XTRSM_KERNEL_RN		gotoblas -> xtrsm_kernel_RN
#define	XTRSM_KERNEL_RT		gotoblas -> xtrsm_kernel_RT
#define	XTRSM_KERNEL_RR		gotoblas -> xtrsm_kernel_RR
#define	XTRSM_KERNEL_RC		gotoblas -> xtrsm_kernel_RC

#define	XSYMM_IUTCOPY		gotoblas -> xsymm_iutcopy
#define	XSYMM_ILTCOPY		gotoblas -> xsymm_iltcopy
#define	XSYMM_OUTCOPY		gotoblas -> xsymm_outcopy
#define	XSYMM_OLTCOPY		gotoblas -> xsymm_oltcopy

#define	XHEMM_OUTCOPY		gotoblas -> xhemm_outcopy
#define	XHEMM_OLTCOPY		gotoblas -> xhemm_oltcopy
#define	XHEMM_IUTCOPY		gotoblas -> xhemm_iutcopy
#define	XHEMM_ILTCOPY		gotoblas -> xhemm_iltcopy

#define	XGEMM3M_ONCOPYB		gotoblas -> xgemm3m_oncopyb
#define	XGEMM3M_ONCOPYR		gotoblas -> xgemm3m_oncopyr
#define	XGEMM3M_ONCOPYI		gotoblas -> xgemm3m_oncopyi
#define	XGEMM3M_OTCOPYB		gotoblas -> xgemm3m_otcopyb
#define	XGEMM3M_OTCOPYR		gotoblas -> xgemm3m_otcopyr
#define	XGEMM3M_OTCOPYI		gotoblas -> xgemm3m_otcopyi

#define	XGEMM3M_INCOPYB		gotoblas -> xgemm3m_incopyb
#define	XGEMM3M_INCOPYR		gotoblas -> xgemm3m_incopyr
#define	XGEMM3M_INCOPYI		gotoblas -> xgemm3m_incopyi
#define	XGEMM3M_ITCOPYB		gotoblas -> xgemm3m_itcopyb
#define	XGEMM3M_ITCOPYR		gotoblas -> xgemm3m_itcopyr
#define	XGEMM3M_ITCOPYI		gotoblas -> xgemm3m_itcopyi

#define	XSYMM3M_ILCOPYB		gotoblas -> xsymm3m_ilcopyb
#define	XSYMM3M_IUCOPYB		gotoblas -> xsymm3m_iucopyb
#define	XSYMM3M_ILCOPYR		gotoblas -> xsymm3m_ilcopyr
#define	XSYMM3M_IUCOPYR		gotoblas -> xsymm3m_iucopyr
#define	XSYMM3M_ILCOPYI		gotoblas -> xsymm3m_ilcopyi
#define	XSYMM3M_IUCOPYI		gotoblas -> xsymm3m_iucopyi

#define	XSYMM3M_OLCOPYB		gotoblas -> xsymm3m_olcopyb
#define	XSYMM3M_OUCOPYB		gotoblas -> xsymm3m_oucopyb
#define	XSYMM3M_OLCOPYR		gotoblas -> xsymm3m_olcopyr
#define	XSYMM3M_OUCOPYR		gotoblas -> xsymm3m_oucopyr
#define	XSYMM3M_OLCOPYI		gotoblas -> xsymm3m_olcopyi
#define	XSYMM3M_OUCOPYI		gotoblas -> xsymm3m_oucopyi

#define	XHEMM3M_ILCOPYB		gotoblas -> xhemm3m_ilcopyb
#define	XHEMM3M_IUCOPYB		gotoblas -> xhemm3m_iucopyb
#define	XHEMM3M_ILCOPYR		gotoblas -> xhemm3m_ilcopyr
#define	XHEMM3M_IUCOPYR		gotoblas -> xhemm3m_iucopyr
#define	XHEMM3M_ILCOPYI		gotoblas -> xhemm3m_ilcopyi
#define	XHEMM3M_IUCOPYI		gotoblas -> xhemm3m_iucopyi

#define	XHEMM3M_OLCOPYB		gotoblas -> xhemm3m_olcopyb
#define	XHEMM3M_OUCOPYB		gotoblas -> xhemm3m_oucopyb
#define	XHEMM3M_OLCOPYR		gotoblas -> xhemm3m_olcopyr
#define	XHEMM3M_OUCOPYR		gotoblas -> xhemm3m_oucopyr
#define	XHEMM3M_OLCOPYI		gotoblas -> xhemm3m_olcopyi
#define	XHEMM3M_OUCOPYI		gotoblas -> xhemm3m_oucopyi

#define	XGEMM3M_KERNEL		gotoblas -> xgemm3m_kernel

#define XNEG_TCOPY		gotoblas -> xneg_tcopy
#define XLASWP_NCOPY		gotoblas -> xlaswp_ncopy

#endif

#define	XGEMM_NN		xgemm_nn
#define	XGEMM_CN		xgemm_cn
#define	XGEMM_TN		xgemm_tn
#define	XGEMM_NC		xgemm_nc
#define	XGEMM_NT		xgemm_nt
#define	XGEMM_CC		xgemm_cc
#define	XGEMM_CT		xgemm_ct
#define	XGEMM_TC		xgemm_tc
#define	XGEMM_TT		xgemm_tt
#define	XGEMM_NR		xgemm_nr
#define	XGEMM_TR		xgemm_tr
#define	XGEMM_CR		xgemm_cr
#define	XGEMM_RN		xgemm_rn
#define	XGEMM_RT		xgemm_rt
#define	XGEMM_RC		xgemm_rc
#define	XGEMM_RR		xgemm_rr

#define	XSYMM_LU		xsymm_LU
#define	XSYMM_LL		xsymm_LL
#define	XSYMM_RU		xsymm_RU
#define	XSYMM_RL		xsymm_RL

#define	XHEMM_LU		xhemm_LU
#define	XHEMM_LL		xhemm_LL
#define	XHEMM_RU		xhemm_RU
#define	XHEMM_RL		xhemm_RL

#define	XSYRK_UN		xsyrk_UN
#define	XSYRK_UT		xsyrk_UT
#define	XSYRK_LN		xsyrk_LN
#define	XSYRK_LT		xsyrk_LT
#define	XSYRK_UR		xsyrk_UN
#define	XSYRK_UC		xsyrk_UT
#define	XSYRK_LR		xsyrk_LN
#define	XSYRK_LC		xsyrk_LT

#define	XSYRK_KERNEL_U		xsyrk_kernel_U
#define	XSYRK_KERNEL_L		xsyrk_kernel_L

#define	XHERK_UN		xherk_UN
#define	XHERK_LN		xherk_LN
#define	XHERK_UC		xherk_UC
#define	XHERK_LC		xherk_LC

#define	XHER2K_UN		xher2k_UN
#define	XHER2K_LN		xher2k_LN
#define	XHER2K_UC		xher2k_UC
#define	XHER2K_LC		xher2k_LC

#define	XSYR2K_UN		xsyr2k_UN
#define	XSYR2K_UT		xsyr2k_UT
#define	XSYR2K_LN		xsyr2k_LN
#define	XSYR2K_LT		xsyr2k_LT
#define	XSYR2K_UR		xsyr2k_UN
#define	XSYR2K_UC		xsyr2k_UT
#define	XSYR2K_LR		xsyr2k_LN
#define	XSYR2K_LC		xsyr2k_LT

#define	XSYR2K_KERNEL_U		xsyr2k_kernel_U
#define	XSYR2K_KERNEL_L		xsyr2k_kernel_L

#define	XTRMM_LNUU		xtrmm_LNUU
#define	XTRMM_LNUN		xtrmm_LNUN
#define	XTRMM_LNLU		xtrmm_LNLU
#define	XTRMM_LNLN		xtrmm_LNLN
#define	XTRMM_LTUU		xtrmm_LTUU
#define	XTRMM_LTUN		xtrmm_LTUN
#define	XTRMM_LTLU		xtrmm_LTLU
#define	XTRMM_LTLN		xtrmm_LTLN
#define	XTRMM_LRUU		xtrmm_LRUU
#define	XTRMM_LRUN		xtrmm_LRUN
#define	XTRMM_LRLU		xtrmm_LRLU
#define	XTRMM_LRLN		xtrmm_LRLN
#define	XTRMM_LCUU		xtrmm_LCUU
#define	XTRMM_LCUN		xtrmm_LCUN
#define	XTRMM_LCLU		xtrmm_LCLU
#define	XTRMM_LCLN		xtrmm_LCLN
#define	XTRMM_RNUU		xtrmm_RNUU
#define	XTRMM_RNUN		xtrmm_RNUN
#define	XTRMM_RNLU		xtrmm_RNLU
#define	XTRMM_RNLN		xtrmm_RNLN
#define	XTRMM_RTUU		xtrmm_RTUU
#define	XTRMM_RTUN		xtrmm_RTUN
#define	XTRMM_RTLU		xtrmm_RTLU
#define	XTRMM_RTLN		xtrmm_RTLN
#define	XTRMM_RRUU		xtrmm_RRUU
#define	XTRMM_RRUN		xtrmm_RRUN
#define	XTRMM_RRLU		xtrmm_RRLU
#define	XTRMM_RRLN		xtrmm_RRLN
#define	XTRMM_RCUU		xtrmm_RCUU
#define	XTRMM_RCUN		xtrmm_RCUN
#define	XTRMM_RCLU		xtrmm_RCLU
#define	XTRMM_RCLN		xtrmm_RCLN

#define	XTRSM_LNUU		xtrsm_LNUU
#define	XTRSM_LNUN		xtrsm_LNUN
#define	XTRSM_LNLU		xtrsm_LNLU
#define	XTRSM_LNLN		xtrsm_LNLN
#define	XTRSM_LTUU		xtrsm_LTUU
#define	XTRSM_LTUN		xtrsm_LTUN
#define	XTRSM_LTLU		xtrsm_LTLU
#define	XTRSM_LTLN		xtrsm_LTLN
#define	XTRSM_LRUU		xtrsm_LRUU
#define	XTRSM_LRUN		xtrsm_LRUN
#define	XTRSM_LRLU		xtrsm_LRLU
#define	XTRSM_LRLN		xtrsm_LRLN
#define	XTRSM_LCUU		xtrsm_LCUU
#define	XTRSM_LCUN		xtrsm_LCUN
#define	XTRSM_LCLU		xtrsm_LCLU
#define	XTRSM_LCLN		xtrsm_LCLN
#define	XTRSM_RNUU		xtrsm_RNUU
#define	XTRSM_RNUN		xtrsm_RNUN
#define	XTRSM_RNLU		xtrsm_RNLU
#define	XTRSM_RNLN		xtrsm_RNLN
#define	XTRSM_RTUU		xtrsm_RTUU
#define	XTRSM_RTUN		xtrsm_RTUN
#define	XTRSM_RTLU		xtrsm_RTLU
#define	XTRSM_RTLN		xtrsm_RTLN
#define	XTRSM_RRUU		xtrsm_RRUU
#define	XTRSM_RRUN		xtrsm_RRUN
#define	XTRSM_RRLU		xtrsm_RRLU
#define	XTRSM_RRLN		xtrsm_RRLN
#define	XTRSM_RCUU		xtrsm_RCUU
#define	XTRSM_RCUN		xtrsm_RCUN
#define	XTRSM_RCLU		xtrsm_RCLU
#define	XTRSM_RCLN		xtrsm_RCLN

#define	XGEMM_THREAD_NN		xgemm_thread_nn
#define	XGEMM_THREAD_CN		xgemm_thread_cn
#define	XGEMM_THREAD_TN		xgemm_thread_tn
#define	XGEMM_THREAD_NC		xgemm_thread_nc
#define	XGEMM_THREAD_NT		xgemm_thread_nt
#define	XGEMM_THREAD_CC		xgemm_thread_cc
#define	XGEMM_THREAD_CT		xgemm_thread_ct
#define	XGEMM_THREAD_TC		xgemm_thread_tc
#define	XGEMM_THREAD_TT		xgemm_thread_tt
#define	XGEMM_THREAD_NR		xgemm_thread_nr
#define	XGEMM_THREAD_TR		xgemm_thread_tr
#define	XGEMM_THREAD_CR		xgemm_thread_cr
#define	XGEMM_THREAD_RN		xgemm_thread_rn
#define	XGEMM_THREAD_RT		xgemm_thread_rt
#define	XGEMM_THREAD_RC		xgemm_thread_rc
#define	XGEMM_THREAD_RR		xgemm_thread_rr

#define	XSYMM_THREAD_LU		xsymm_thread_LU
#define	XSYMM_THREAD_LL		xsymm_thread_LL
#define	XSYMM_THREAD_RU		xsymm_thread_RU
#define	XSYMM_THREAD_RL		xsymm_thread_RL

#define	XHEMM_THREAD_LU		xhemm_thread_LU
#define	XHEMM_THREAD_LL		xhemm_thread_LL
#define	XHEMM_THREAD_RU		xhemm_thread_RU
#define	XHEMM_THREAD_RL		xhemm_thread_RL

#define	XSYRK_THREAD_UN		xsyrk_thread_UN
#define	XSYRK_THREAD_UT		xsyrk_thread_UT
#define	XSYRK_THREAD_LN		xsyrk_thread_LN
#define	XSYRK_THREAD_LT		xsyrk_thread_LT
#define	XSYRK_THREAD_UR		xsyrk_thread_UN
#define	XSYRK_THREAD_UC		xsyrk_thread_UT
#define	XSYRK_THREAD_LR		xsyrk_thread_LN
#define	XSYRK_THREAD_LC		xsyrk_thread_LT

#define	XHERK_THREAD_UN		xherk_thread_UN
#define	XHERK_THREAD_UT		xherk_thread_UT
#define	XHERK_THREAD_LN		xherk_thread_LN
#define	XHERK_THREAD_LT		xherk_thread_LT
#define	XHERK_THREAD_UR		xherk_thread_UR
#define	XHERK_THREAD_UC		xherk_thread_UC
#define	XHERK_THREAD_LR		xherk_thread_LR
#define	XHERK_THREAD_LC		xherk_thread_LC

#define	XGEMM3M_NN		xgemm3m_nn
#define	XGEMM3M_CN		xgemm3m_cn
#define	XGEMM3M_TN		xgemm3m_tn
#define	XGEMM3M_NC		xgemm3m_nc
#define	XGEMM3M_NT		xgemm3m_nt
#define	XGEMM3M_CC		xgemm3m_cc
#define	XGEMM3M_CT		xgemm3m_ct
#define	XGEMM3M_TC		xgemm3m_tc
#define	XGEMM3M_TT		xgemm3m_tt
#define	XGEMM3M_NR		xgemm3m_nr
#define	XGEMM3M_TR		xgemm3m_tr
#define	XGEMM3M_CR		xgemm3m_cr
#define	XGEMM3M_RN		xgemm3m_rn
#define	XGEMM3M_RT		xgemm3m_rt
#define	XGEMM3M_RC		xgemm3m_rc
#define	XGEMM3M_RR		xgemm3m_rr

#define	XGEMM3M_THREAD_NN	xgemm3m_thread_nn
#define	XGEMM3M_THREAD_CN	xgemm3m_thread_cn
#define	XGEMM3M_THREAD_TN	xgemm3m_thread_tn
#define	XGEMM3M_THREAD_NC	xgemm3m_thread_nc
#define	XGEMM3M_THREAD_NT	xgemm3m_thread_nt
#define	XGEMM3M_THREAD_CC	xgemm3m_thread_cc
#define	XGEMM3M_THREAD_CT	xgemm3m_thread_ct
#define	XGEMM3M_THREAD_TC	xgemm3m_thread_tc
#define	XGEMM3M_THREAD_TT	xgemm3m_thread_tt
#define	XGEMM3M_THREAD_NR	xgemm3m_thread_nr
#define	XGEMM3M_THREAD_TR	xgemm3m_thread_tr
#define	XGEMM3M_THREAD_CR	xgemm3m_thread_cr
#define	XGEMM3M_THREAD_RN	xgemm3m_thread_rn
#define	XGEMM3M_THREAD_RT	xgemm3m_thread_rt
#define	XGEMM3M_THREAD_RC	xgemm3m_thread_rc
#define	XGEMM3M_THREAD_RR	xgemm3m_thread_rr

#define	XSYMM3M_LU		xsymm3m_LU
#define	XSYMM3M_LL		xsymm3m_LL
#define	XSYMM3M_RU		xsymm3m_RU
#define	XSYMM3M_RL		xsymm3m_RL

#define	XSYMM3M_THREAD_LU	xsymm3m_thread_LU
#define	XSYMM3M_THREAD_LL	xsymm3m_thread_LL
#define	XSYMM3M_THREAD_RU	xsymm3m_thread_RU
#define	XSYMM3M_THREAD_RL	xsymm3m_thread_RL

#define	XHEMM3M_LU		xhemm3m_LU
#define	XHEMM3M_LL		xhemm3m_LL
#define	XHEMM3M_RU		xhemm3m_RU
#define	XHEMM3M_RL		xhemm3m_RL

#define	XHEMM3M_THREAD_LU	xhemm3m_thread_LU
#define	XHEMM3M_THREAD_LL	xhemm3m_thread_LL
#define	XHEMM3M_THREAD_RU	xhemm3m_thread_RU
#define	XHEMM3M_THREAD_RL	xhemm3m_thread_RL

#endif
