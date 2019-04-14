#ifndef COMMON_C_H
#define COMMON_C_H

#ifndef DYNAMIC_ARCH

#define	CAMAX_K			camax_k
#define	CAMIN_K			camin_k
#define	CMAX_K			cmax_k
#define	CMIN_K			cmin_k
#define	ICAMAX_K		icamax_k
#define	ICAMIN_K		icamin_k
#define	ICMAX_K			icmax_k
#define	ICMIN_K			icmin_k
#define	CASUM_K			casum_k
#define	CAXPYU_K		caxpy_k
#define	CAXPYC_K		caxpyc_k
#define	CCOPY_K			ccopy_k
#define	CDOTU_K			cdotu_k
#define	CDOTC_K			cdotc_k
#define	CNRM2_K			cnrm2_k
#define	CSCAL_K			cscal_k
#define	CSWAP_K			cswap_k
#define	CROT_K			csrot_k

#define	CGEMV_N			cgemv_n
#define	CGEMV_T			cgemv_t
#define	CGEMV_R			cgemv_r
#define	CGEMV_C			cgemv_c
#define	CGEMV_O			cgemv_o
#define	CGEMV_U			cgemv_u
#define	CGEMV_S			cgemv_s
#define	CGEMV_D			cgemv_d

#define	CGERU_K			cgeru_k
#define	CGERC_K			cgerc_k
#define	CGERV_K			cgerv_k
#define	CGERD_K			cgerd_k

#define CSYMV_U			csymv_U
#define CSYMV_L			csymv_L
#define CHEMV_U			chemv_U
#define CHEMV_L			chemv_L
#define CHEMV_V			chemv_V
#define CHEMV_M			chemv_M

#define CSYMV_THREAD_U		csymv_thread_U
#define CSYMV_THREAD_L		csymv_thread_L
#define CHEMV_THREAD_U		chemv_thread_U
#define CHEMV_THREAD_L		chemv_thread_L
#define CHEMV_THREAD_V		chemv_thread_V
#define CHEMV_THREAD_M		chemv_thread_M

#define	CGEMM_ONCOPY		cgemm_oncopy
#define	CGEMM_OTCOPY		cgemm_otcopy

#if CGEMM_DEFAULT_UNROLL_M == CGEMM_DEFAULT_UNROLL_N
#define	CGEMM_INCOPY		cgemm_oncopy
#define	CGEMM_ITCOPY		cgemm_otcopy
#else
#define	CGEMM_INCOPY		cgemm_incopy
#define	CGEMM_ITCOPY		cgemm_itcopy
#endif

#define	CTRMM_OUNUCOPY		ctrmm_ounucopy
#define	CTRMM_OUNNCOPY		ctrmm_ounncopy
#define	CTRMM_OUTUCOPY		ctrmm_outucopy
#define	CTRMM_OUTNCOPY		ctrmm_outncopy
#define	CTRMM_OLNUCOPY		ctrmm_olnucopy
#define	CTRMM_OLNNCOPY		ctrmm_olnncopy
#define	CTRMM_OLTUCOPY		ctrmm_oltucopy
#define	CTRMM_OLTNCOPY		ctrmm_oltncopy

#define	CTRSM_OUNUCOPY		ctrsm_ounucopy
#define	CTRSM_OUNNCOPY		ctrsm_ounncopy
#define	CTRSM_OUTUCOPY		ctrsm_outucopy
#define	CTRSM_OUTNCOPY		ctrsm_outncopy
#define	CTRSM_OLNUCOPY		ctrsm_olnucopy
#define	CTRSM_OLNNCOPY		ctrsm_olnncopy
#define	CTRSM_OLTUCOPY		ctrsm_oltucopy
#define	CTRSM_OLTNCOPY		ctrsm_oltncopy

#if CGEMM_DEFAULT_UNROLL_M == CGEMM_DEFAULT_UNROLL_N
#define	CTRMM_IUNUCOPY		ctrmm_ounucopy
#define	CTRMM_IUNNCOPY		ctrmm_ounncopy
#define	CTRMM_IUTUCOPY		ctrmm_outucopy
#define	CTRMM_IUTNCOPY		ctrmm_outncopy
#define	CTRMM_ILNUCOPY		ctrmm_olnucopy
#define	CTRMM_ILNNCOPY		ctrmm_olnncopy
#define	CTRMM_ILTUCOPY		ctrmm_oltucopy
#define	CTRMM_ILTNCOPY		ctrmm_oltncopy

#define	CTRSM_IUNUCOPY		ctrsm_ounucopy
#define	CTRSM_IUNNCOPY		ctrsm_ounncopy
#define	CTRSM_IUTUCOPY		ctrsm_outucopy
#define	CTRSM_IUTNCOPY		ctrsm_outncopy
#define	CTRSM_ILNUCOPY		ctrsm_olnucopy
#define	CTRSM_ILNNCOPY		ctrsm_olnncopy
#define	CTRSM_ILTUCOPY		ctrsm_oltucopy
#define	CTRSM_ILTNCOPY		ctrsm_oltncopy
#else
#define	CTRMM_IUNUCOPY		ctrmm_iunucopy
#define	CTRMM_IUNNCOPY		ctrmm_iunncopy
#define	CTRMM_IUTUCOPY		ctrmm_iutucopy
#define	CTRMM_IUTNCOPY		ctrmm_iutncopy
#define	CTRMM_ILNUCOPY		ctrmm_ilnucopy
#define	CTRMM_ILNNCOPY		ctrmm_ilnncopy
#define	CTRMM_ILTUCOPY		ctrmm_iltucopy
#define	CTRMM_ILTNCOPY		ctrmm_iltncopy

#define	CTRSM_IUNUCOPY		ctrsm_iunucopy
#define	CTRSM_IUNNCOPY		ctrsm_iunncopy
#define	CTRSM_IUTUCOPY		ctrsm_iutucopy
#define	CTRSM_IUTNCOPY		ctrsm_iutncopy
#define	CTRSM_ILNUCOPY		ctrsm_ilnucopy
#define	CTRSM_ILNNCOPY		ctrsm_ilnncopy
#define	CTRSM_ILTUCOPY		ctrsm_iltucopy
#define	CTRSM_ILTNCOPY		ctrsm_iltncopy
#endif

#define	CGEMM_BETA		cgemm_beta

#define	CGEMM_KERNEL_N		cgemm_kernel_n
#define	CGEMM_KERNEL_L		cgemm_kernel_l
#define	CGEMM_KERNEL_R		cgemm_kernel_r
#define	CGEMM_KERNEL_B		cgemm_kernel_b

#define	CTRMM_KERNEL_LN		ctrmm_kernel_LN
#define	CTRMM_KERNEL_LT		ctrmm_kernel_LT
#define	CTRMM_KERNEL_LR		ctrmm_kernel_LR
#define	CTRMM_KERNEL_LC		ctrmm_kernel_LC
#define	CTRMM_KERNEL_RN		ctrmm_kernel_RN
#define	CTRMM_KERNEL_RT		ctrmm_kernel_RT
#define	CTRMM_KERNEL_RR		ctrmm_kernel_RR
#define	CTRMM_KERNEL_RC		ctrmm_kernel_RC

#define	CTRSM_KERNEL_LN		ctrsm_kernel_LN
#define	CTRSM_KERNEL_LT		ctrsm_kernel_LT
#define	CTRSM_KERNEL_LR		ctrsm_kernel_LR
#define	CTRSM_KERNEL_LC		ctrsm_kernel_LC
#define	CTRSM_KERNEL_RN		ctrsm_kernel_RN
#define	CTRSM_KERNEL_RT		ctrsm_kernel_RT
#define	CTRSM_KERNEL_RR		ctrsm_kernel_RR
#define	CTRSM_KERNEL_RC		ctrsm_kernel_RC

#define	CSYMM_OUTCOPY		csymm_outcopy
#define	CSYMM_OLTCOPY		csymm_oltcopy
#if CGEMM_DEFAULT_UNROLL_M == CGEMM_DEFAULT_UNROLL_N
#define	CSYMM_IUTCOPY		csymm_outcopy
#define	CSYMM_ILTCOPY		csymm_oltcopy
#else
#define	CSYMM_IUTCOPY		csymm_iutcopy
#define	CSYMM_ILTCOPY		csymm_iltcopy
#endif

#define	CHEMM_OUTCOPY		chemm_outcopy
#define	CHEMM_OLTCOPY		chemm_oltcopy
#if CGEMM_DEFAULT_UNROLL_M == CGEMM_DEFAULT_UNROLL_N
#define	CHEMM_IUTCOPY		chemm_outcopy
#define	CHEMM_ILTCOPY		chemm_oltcopy
#else
#define	CHEMM_IUTCOPY		chemm_iutcopy
#define	CHEMM_ILTCOPY		chemm_iltcopy
#endif

#define	CGEMM3M_ONCOPYB		cgemm3m_oncopyb
#define	CGEMM3M_ONCOPYR		cgemm3m_oncopyr
#define	CGEMM3M_ONCOPYI		cgemm3m_oncopyi
#define	CGEMM3M_OTCOPYB		cgemm3m_otcopyb
#define	CGEMM3M_OTCOPYR		cgemm3m_otcopyr
#define	CGEMM3M_OTCOPYI		cgemm3m_otcopyi

#define	CGEMM3M_INCOPYB		cgemm3m_incopyb
#define	CGEMM3M_INCOPYR		cgemm3m_incopyr
#define	CGEMM3M_INCOPYI		cgemm3m_incopyi
#define	CGEMM3M_ITCOPYB		cgemm3m_itcopyb
#define	CGEMM3M_ITCOPYR		cgemm3m_itcopyr
#define	CGEMM3M_ITCOPYI		cgemm3m_itcopyi

#define	CSYMM3M_ILCOPYB		csymm3m_ilcopyb
#define	CSYMM3M_IUCOPYB		csymm3m_iucopyb
#define	CSYMM3M_ILCOPYR		csymm3m_ilcopyr
#define	CSYMM3M_IUCOPYR		csymm3m_iucopyr
#define	CSYMM3M_ILCOPYI		csymm3m_ilcopyi
#define	CSYMM3M_IUCOPYI		csymm3m_iucopyi

#define	CSYMM3M_OLCOPYB		csymm3m_olcopyb
#define	CSYMM3M_OUCOPYB		csymm3m_oucopyb
#define	CSYMM3M_OLCOPYR		csymm3m_olcopyr
#define	CSYMM3M_OUCOPYR		csymm3m_oucopyr
#define	CSYMM3M_OLCOPYI		csymm3m_olcopyi
#define	CSYMM3M_OUCOPYI		csymm3m_oucopyi

#define	CHEMM3M_ILCOPYB		chemm3m_ilcopyb
#define	CHEMM3M_IUCOPYB		chemm3m_iucopyb
#define	CHEMM3M_ILCOPYR		chemm3m_ilcopyr
#define	CHEMM3M_IUCOPYR		chemm3m_iucopyr
#define	CHEMM3M_ILCOPYI		chemm3m_ilcopyi
#define	CHEMM3M_IUCOPYI		chemm3m_iucopyi

#define	CHEMM3M_OLCOPYB		chemm3m_olcopyb
#define	CHEMM3M_OUCOPYB		chemm3m_oucopyb
#define	CHEMM3M_OLCOPYR		chemm3m_olcopyr
#define	CHEMM3M_OUCOPYR		chemm3m_oucopyr
#define	CHEMM3M_OLCOPYI		chemm3m_olcopyi
#define	CHEMM3M_OUCOPYI		chemm3m_oucopyi

#define	CGEMM3M_KERNEL		cgemm3m_kernel

#define CNEG_TCOPY		cneg_tcopy
#define CLASWP_NCOPY		claswp_ncopy

#define CAXPBY_K                caxpby_k

#define COMATCOPY_K_CN          comatcopy_k_cn
#define COMATCOPY_K_RN          comatcopy_k_rn
#define COMATCOPY_K_CT          comatcopy_k_ct
#define COMATCOPY_K_RT          comatcopy_k_rt
#define COMATCOPY_K_CNC         comatcopy_k_cnc
#define COMATCOPY_K_RNC         comatcopy_k_rnc
#define COMATCOPY_K_CTC         comatcopy_k_ctc
#define COMATCOPY_K_RTC         comatcopy_k_rtc

#define CIMATCOPY_K_CN          cimatcopy_k_cn
#define CIMATCOPY_K_RN          cimatcopy_k_rn
#define CIMATCOPY_K_CT          cimatcopy_k_ct
#define CIMATCOPY_K_RT          cimatcopy_k_rt
#define CIMATCOPY_K_CNC         cimatcopy_k_cnc
#define CIMATCOPY_K_RNC         cimatcopy_k_rnc
#define CIMATCOPY_K_CTC         cimatcopy_k_ctc
#define CIMATCOPY_K_RTC         cimatcopy_k_rtc

#define CGEADD_K                cgeadd_k 

#else

#define	CAMAX_K			gotoblas -> camax_k
#define	CAMIN_K			gotoblas -> camin_k
#define	CMAX_K			gotoblas -> cmax_k
#define	CMIN_K			gotoblas -> cmin_k
#define	ICAMAX_K		gotoblas -> icamax_k
#define	ICAMIN_K		gotoblas -> icamin_k
#define	ICMAX_K			gotoblas -> icmax_k
#define	ICMIN_K			gotoblas -> icmin_k
#define	CASUM_K			gotoblas -> casum_k
#define	CAXPYU_K		gotoblas -> caxpy_k
#define	CAXPYC_K		gotoblas -> caxpyc_k
#define	CCOPY_K			gotoblas -> ccopy_k
#define	CDOTU_K			gotoblas -> cdotu_k
#define	CDOTC_K			gotoblas -> cdotc_k
#define	CNRM2_K			gotoblas -> cnrm2_k
#define	CSCAL_K			gotoblas -> cscal_k
#define	CSWAP_K			gotoblas -> cswap_k
#define	CROT_K			gotoblas -> csrot_k

#define	CGEMV_N			gotoblas -> cgemv_n
#define	CGEMV_T			gotoblas -> cgemv_t
#define	CGEMV_R			gotoblas -> cgemv_r
#define	CGEMV_C			gotoblas -> cgemv_c
#define	CGEMV_O			gotoblas -> cgemv_o
#define	CGEMV_U			gotoblas -> cgemv_u
#define	CGEMV_S			gotoblas -> cgemv_s
#define	CGEMV_D			gotoblas -> cgemv_d

#define	CGERU_K			gotoblas -> cgeru_k
#define	CGERC_K			gotoblas -> cgerc_k
#define	CGERV_K			gotoblas -> cgerv_k
#define	CGERD_K			gotoblas -> cgerd_k

#define CSYMV_U			gotoblas -> csymv_U
#define CSYMV_L			gotoblas -> csymv_L
#define CHEMV_U			gotoblas -> chemv_U
#define CHEMV_L			gotoblas -> chemv_L
#define CHEMV_V			gotoblas -> chemv_V
#define CHEMV_M			gotoblas -> chemv_M

#define CSYMV_THREAD_U		csymv_thread_U
#define CSYMV_THREAD_L		csymv_thread_L
#define CHEMV_THREAD_U		chemv_thread_U
#define CHEMV_THREAD_L		chemv_thread_L
#define CHEMV_THREAD_V		chemv_thread_V
#define CHEMV_THREAD_M		chemv_thread_M

#define	CGEMM_ONCOPY		gotoblas -> cgemm_oncopy
#define	CGEMM_OTCOPY		gotoblas -> cgemm_otcopy
#define	CGEMM_INCOPY		gotoblas -> cgemm_incopy
#define	CGEMM_ITCOPY		gotoblas -> cgemm_itcopy

#define	CTRMM_OUNUCOPY		gotoblas -> ctrmm_ounucopy
#define	CTRMM_OUTUCOPY		gotoblas -> ctrmm_outucopy
#define	CTRMM_OLNUCOPY		gotoblas -> ctrmm_olnucopy
#define	CTRMM_OLTUCOPY		gotoblas -> ctrmm_oltucopy
#define	CTRSM_OUNUCOPY		gotoblas -> ctrsm_ounucopy
#define	CTRSM_OUTUCOPY		gotoblas -> ctrsm_outucopy
#define	CTRSM_OLNUCOPY		gotoblas -> ctrsm_olnucopy
#define	CTRSM_OLTUCOPY		gotoblas -> ctrsm_oltucopy

#define	CTRMM_IUNUCOPY		gotoblas -> ctrmm_iunucopy
#define	CTRMM_IUTUCOPY		gotoblas -> ctrmm_iutucopy
#define	CTRMM_ILNUCOPY		gotoblas -> ctrmm_ilnucopy
#define	CTRMM_ILTUCOPY		gotoblas -> ctrmm_iltucopy
#define	CTRSM_IUNUCOPY		gotoblas -> ctrsm_iunucopy
#define	CTRSM_IUTUCOPY		gotoblas -> ctrsm_iutucopy
#define	CTRSM_ILNUCOPY		gotoblas -> ctrsm_ilnucopy
#define	CTRSM_ILTUCOPY		gotoblas -> ctrsm_iltucopy

#define	CTRMM_OUNNCOPY		gotoblas -> ctrmm_ounncopy
#define	CTRMM_OUTNCOPY		gotoblas -> ctrmm_outncopy
#define	CTRMM_OLNNCOPY		gotoblas -> ctrmm_olnncopy
#define	CTRMM_OLTNCOPY		gotoblas -> ctrmm_oltncopy
#define	CTRSM_OUNNCOPY		gotoblas -> ctrsm_ounncopy
#define	CTRSM_OUTNCOPY		gotoblas -> ctrsm_outncopy
#define	CTRSM_OLNNCOPY		gotoblas -> ctrsm_olnncopy
#define	CTRSM_OLTNCOPY		gotoblas -> ctrsm_oltncopy

#define	CTRMM_IUNNCOPY		gotoblas -> ctrmm_iunncopy
#define	CTRMM_IUTNCOPY		gotoblas -> ctrmm_iutncopy
#define	CTRMM_ILNNCOPY		gotoblas -> ctrmm_ilnncopy
#define	CTRMM_ILTNCOPY		gotoblas -> ctrmm_iltncopy
#define	CTRSM_IUNNCOPY		gotoblas -> ctrsm_iunncopy
#define	CTRSM_IUTNCOPY		gotoblas -> ctrsm_iutncopy
#define	CTRSM_ILNNCOPY		gotoblas -> ctrsm_ilnncopy
#define	CTRSM_ILTNCOPY		gotoblas -> ctrsm_iltncopy

#define	CGEMM_BETA		gotoblas -> cgemm_beta
#define	CGEMM_KERNEL_N		gotoblas -> cgemm_kernel_n
#define	CGEMM_KERNEL_L		gotoblas -> cgemm_kernel_l
#define	CGEMM_KERNEL_R		gotoblas -> cgemm_kernel_r
#define	CGEMM_KERNEL_B		gotoblas -> cgemm_kernel_b

#define	CTRMM_KERNEL_LN		gotoblas -> ctrmm_kernel_LN
#define	CTRMM_KERNEL_LT		gotoblas -> ctrmm_kernel_LT
#define	CTRMM_KERNEL_LR		gotoblas -> ctrmm_kernel_LR
#define	CTRMM_KERNEL_LC		gotoblas -> ctrmm_kernel_LC
#define	CTRMM_KERNEL_RN		gotoblas -> ctrmm_kernel_RN
#define	CTRMM_KERNEL_RT		gotoblas -> ctrmm_kernel_RT
#define	CTRMM_KERNEL_RR		gotoblas -> ctrmm_kernel_RR
#define	CTRMM_KERNEL_RC		gotoblas -> ctrmm_kernel_RC

#define	CTRSM_KERNEL_LN		gotoblas -> ctrsm_kernel_LN
#define	CTRSM_KERNEL_LT		gotoblas -> ctrsm_kernel_LT
#define	CTRSM_KERNEL_LR		gotoblas -> ctrsm_kernel_LR
#define	CTRSM_KERNEL_LC		gotoblas -> ctrsm_kernel_LC
#define	CTRSM_KERNEL_RN		gotoblas -> ctrsm_kernel_RN
#define	CTRSM_KERNEL_RT		gotoblas -> ctrsm_kernel_RT
#define	CTRSM_KERNEL_RR		gotoblas -> ctrsm_kernel_RR
#define	CTRSM_KERNEL_RC		gotoblas -> ctrsm_kernel_RC

#define	CSYMM_IUTCOPY		gotoblas -> csymm_iutcopy
#define	CSYMM_ILTCOPY		gotoblas -> csymm_iltcopy
#define	CSYMM_OUTCOPY		gotoblas -> csymm_outcopy
#define	CSYMM_OLTCOPY		gotoblas -> csymm_oltcopy

#define	CHEMM_OUTCOPY		gotoblas -> chemm_outcopy
#define	CHEMM_OLTCOPY		gotoblas -> chemm_oltcopy
#define	CHEMM_IUTCOPY		gotoblas -> chemm_iutcopy
#define	CHEMM_ILTCOPY		gotoblas -> chemm_iltcopy

#define	CGEMM3M_ONCOPYB		gotoblas -> cgemm3m_oncopyb
#define	CGEMM3M_ONCOPYR		gotoblas -> cgemm3m_oncopyr
#define	CGEMM3M_ONCOPYI		gotoblas -> cgemm3m_oncopyi
#define	CGEMM3M_OTCOPYB		gotoblas -> cgemm3m_otcopyb
#define	CGEMM3M_OTCOPYR		gotoblas -> cgemm3m_otcopyr
#define	CGEMM3M_OTCOPYI		gotoblas -> cgemm3m_otcopyi

#define	CGEMM3M_INCOPYB		gotoblas -> cgemm3m_incopyb
#define	CGEMM3M_INCOPYR		gotoblas -> cgemm3m_incopyr
#define	CGEMM3M_INCOPYI		gotoblas -> cgemm3m_incopyi
#define	CGEMM3M_ITCOPYB		gotoblas -> cgemm3m_itcopyb
#define	CGEMM3M_ITCOPYR		gotoblas -> cgemm3m_itcopyr
#define	CGEMM3M_ITCOPYI		gotoblas -> cgemm3m_itcopyi

#define	CSYMM3M_ILCOPYB		gotoblas -> csymm3m_ilcopyb
#define	CSYMM3M_IUCOPYB		gotoblas -> csymm3m_iucopyb
#define	CSYMM3M_ILCOPYR		gotoblas -> csymm3m_ilcopyr
#define	CSYMM3M_IUCOPYR		gotoblas -> csymm3m_iucopyr
#define	CSYMM3M_ILCOPYI		gotoblas -> csymm3m_ilcopyi
#define	CSYMM3M_IUCOPYI		gotoblas -> csymm3m_iucopyi

#define	CSYMM3M_OLCOPYB		gotoblas -> csymm3m_olcopyb
#define	CSYMM3M_OUCOPYB		gotoblas -> csymm3m_oucopyb
#define	CSYMM3M_OLCOPYR		gotoblas -> csymm3m_olcopyr
#define	CSYMM3M_OUCOPYR		gotoblas -> csymm3m_oucopyr
#define	CSYMM3M_OLCOPYI		gotoblas -> csymm3m_olcopyi
#define	CSYMM3M_OUCOPYI		gotoblas -> csymm3m_oucopyi

#define	CHEMM3M_ILCOPYB		gotoblas -> chemm3m_ilcopyb
#define	CHEMM3M_IUCOPYB		gotoblas -> chemm3m_iucopyb
#define	CHEMM3M_ILCOPYR		gotoblas -> chemm3m_ilcopyr
#define	CHEMM3M_IUCOPYR		gotoblas -> chemm3m_iucopyr
#define	CHEMM3M_ILCOPYI		gotoblas -> chemm3m_ilcopyi
#define	CHEMM3M_IUCOPYI		gotoblas -> chemm3m_iucopyi

#define	CHEMM3M_OLCOPYB		gotoblas -> chemm3m_olcopyb
#define	CHEMM3M_OUCOPYB		gotoblas -> chemm3m_oucopyb
#define	CHEMM3M_OLCOPYR		gotoblas -> chemm3m_olcopyr
#define	CHEMM3M_OUCOPYR		gotoblas -> chemm3m_oucopyr
#define	CHEMM3M_OLCOPYI		gotoblas -> chemm3m_olcopyi
#define	CHEMM3M_OUCOPYI		gotoblas -> chemm3m_oucopyi

#define	CGEMM3M_KERNEL		gotoblas -> cgemm3m_kernel

#define CNEG_TCOPY		gotoblas -> cneg_tcopy
#define CLASWP_NCOPY		gotoblas -> claswp_ncopy

#define CAXPBY_K                gotoblas -> caxpby_k

#define COMATCOPY_K_CN          gotoblas -> comatcopy_k_cn
#define COMATCOPY_K_RN          gotoblas -> comatcopy_k_rn
#define COMATCOPY_K_CT          gotoblas -> comatcopy_k_ct
#define COMATCOPY_K_RT          gotoblas -> comatcopy_k_rt
#define COMATCOPY_K_CNC         gotoblas -> comatcopy_k_cnc
#define COMATCOPY_K_RNC         gotoblas -> comatcopy_k_rnc
#define COMATCOPY_K_CTC         gotoblas -> comatcopy_k_ctc
#define COMATCOPY_K_RTC         gotoblas -> comatcopy_k_rtc

#define CIMATCOPY_K_CN          gotoblas -> cimatcopy_k_cn
#define CIMATCOPY_K_RN          gotoblas -> cimatcopy_k_rn
#define CIMATCOPY_K_CT          gotoblas -> cimatcopy_k_ct
#define CIMATCOPY_K_RT          gotoblas -> cimatcopy_k_rt
#define CIMATCOPY_K_CNC         gotoblas -> cimatcopy_k_cnc
#define CIMATCOPY_K_RNC         gotoblas -> cimatcopy_k_rnc
#define CIMATCOPY_K_CTC         gotoblas -> cimatcopy_k_ctc
#define CIMATCOPY_K_RTC         gotoblas -> cimatcopy_k_rtc

#define CGEADD_K                gotoblas -> cgeadd_k 

#endif

#define	CGEMM_NN		cgemm_nn
#define	CGEMM_CN		cgemm_cn
#define	CGEMM_TN		cgemm_tn
#define	CGEMM_NC		cgemm_nc
#define	CGEMM_NT		cgemm_nt
#define	CGEMM_CC		cgemm_cc
#define	CGEMM_CT		cgemm_ct
#define	CGEMM_TC		cgemm_tc
#define	CGEMM_TT		cgemm_tt
#define	CGEMM_NR		cgemm_nr
#define	CGEMM_TR		cgemm_tr
#define	CGEMM_CR		cgemm_cr
#define	CGEMM_RN		cgemm_rn
#define	CGEMM_RT		cgemm_rt
#define	CGEMM_RC		cgemm_rc
#define	CGEMM_RR		cgemm_rr

#define	CSYMM_LU		csymm_LU
#define	CSYMM_LL		csymm_LL
#define	CSYMM_RU		csymm_RU
#define	CSYMM_RL		csymm_RL

#define	CHEMM_LU		chemm_LU
#define	CHEMM_LL		chemm_LL
#define	CHEMM_RU		chemm_RU
#define	CHEMM_RL		chemm_RL

#define	CSYRK_UN		csyrk_UN
#define	CSYRK_UT		csyrk_UT
#define	CSYRK_LN		csyrk_LN
#define	CSYRK_LT		csyrk_LT
#define	CSYRK_UR		csyrk_UN
#define	CSYRK_UC		csyrk_UT
#define	CSYRK_LR		csyrk_LN
#define	CSYRK_LC		csyrk_LT

#define	CSYRK_KERNEL_U		csyrk_kernel_U
#define	CSYRK_KERNEL_L		csyrk_kernel_L

#define	CHERK_UN		cherk_UN
#define	CHERK_LN		cherk_LN
#define	CHERK_UC		cherk_UC
#define	CHERK_LC		cherk_LC

#define	CHER2K_UN		cher2k_UN
#define	CHER2K_LN		cher2k_LN
#define	CHER2K_UC		cher2k_UC
#define	CHER2K_LC		cher2k_LC

#define	CSYR2K_UN		csyr2k_UN
#define	CSYR2K_UT		csyr2k_UT
#define	CSYR2K_LN		csyr2k_LN
#define	CSYR2K_LT		csyr2k_LT
#define	CSYR2K_UR		csyr2k_UN
#define	CSYR2K_UC		csyr2k_UT
#define	CSYR2K_LR		csyr2k_LN
#define	CSYR2K_LC		csyr2k_LT

#define	CSYR2K_KERNEL_U		csyr2k_kernel_U
#define	CSYR2K_KERNEL_L		csyr2k_kernel_L

#define	CTRMM_LNUU		ctrmm_LNUU
#define	CTRMM_LNUN		ctrmm_LNUN
#define	CTRMM_LNLU		ctrmm_LNLU
#define	CTRMM_LNLN		ctrmm_LNLN
#define	CTRMM_LTUU		ctrmm_LTUU
#define	CTRMM_LTUN		ctrmm_LTUN
#define	CTRMM_LTLU		ctrmm_LTLU
#define	CTRMM_LTLN		ctrmm_LTLN
#define	CTRMM_LRUU		ctrmm_LRUU
#define	CTRMM_LRUN		ctrmm_LRUN
#define	CTRMM_LRLU		ctrmm_LRLU
#define	CTRMM_LRLN		ctrmm_LRLN
#define	CTRMM_LCUU		ctrmm_LCUU
#define	CTRMM_LCUN		ctrmm_LCUN
#define	CTRMM_LCLU		ctrmm_LCLU
#define	CTRMM_LCLN		ctrmm_LCLN
#define	CTRMM_RNUU		ctrmm_RNUU
#define	CTRMM_RNUN		ctrmm_RNUN
#define	CTRMM_RNLU		ctrmm_RNLU
#define	CTRMM_RNLN		ctrmm_RNLN
#define	CTRMM_RTUU		ctrmm_RTUU
#define	CTRMM_RTUN		ctrmm_RTUN
#define	CTRMM_RTLU		ctrmm_RTLU
#define	CTRMM_RTLN		ctrmm_RTLN
#define	CTRMM_RRUU		ctrmm_RRUU
#define	CTRMM_RRUN		ctrmm_RRUN
#define	CTRMM_RRLU		ctrmm_RRLU
#define	CTRMM_RRLN		ctrmm_RRLN
#define	CTRMM_RCUU		ctrmm_RCUU
#define	CTRMM_RCUN		ctrmm_RCUN
#define	CTRMM_RCLU		ctrmm_RCLU
#define	CTRMM_RCLN		ctrmm_RCLN

#define	CTRSM_LNUU		ctrsm_LNUU
#define	CTRSM_LNUN		ctrsm_LNUN
#define	CTRSM_LNLU		ctrsm_LNLU
#define	CTRSM_LNLN		ctrsm_LNLN
#define	CTRSM_LTUU		ctrsm_LTUU
#define	CTRSM_LTUN		ctrsm_LTUN
#define	CTRSM_LTLU		ctrsm_LTLU
#define	CTRSM_LTLN		ctrsm_LTLN
#define	CTRSM_LRUU		ctrsm_LRUU
#define	CTRSM_LRUN		ctrsm_LRUN
#define	CTRSM_LRLU		ctrsm_LRLU
#define	CTRSM_LRLN		ctrsm_LRLN
#define	CTRSM_LCUU		ctrsm_LCUU
#define	CTRSM_LCUN		ctrsm_LCUN
#define	CTRSM_LCLU		ctrsm_LCLU
#define	CTRSM_LCLN		ctrsm_LCLN
#define	CTRSM_RNUU		ctrsm_RNUU
#define	CTRSM_RNUN		ctrsm_RNUN
#define	CTRSM_RNLU		ctrsm_RNLU
#define	CTRSM_RNLN		ctrsm_RNLN
#define	CTRSM_RTUU		ctrsm_RTUU
#define	CTRSM_RTUN		ctrsm_RTUN
#define	CTRSM_RTLU		ctrsm_RTLU
#define	CTRSM_RTLN		ctrsm_RTLN
#define	CTRSM_RRUU		ctrsm_RRUU
#define	CTRSM_RRUN		ctrsm_RRUN
#define	CTRSM_RRLU		ctrsm_RRLU
#define	CTRSM_RRLN		ctrsm_RRLN
#define	CTRSM_RCUU		ctrsm_RCUU
#define	CTRSM_RCUN		ctrsm_RCUN
#define	CTRSM_RCLU		ctrsm_RCLU
#define	CTRSM_RCLN		ctrsm_RCLN

#define	CGEMM_THREAD_NN		cgemm_thread_nn
#define	CGEMM_THREAD_CN		cgemm_thread_cn
#define	CGEMM_THREAD_TN		cgemm_thread_tn
#define	CGEMM_THREAD_NC		cgemm_thread_nc
#define	CGEMM_THREAD_NT		cgemm_thread_nt
#define	CGEMM_THREAD_CC		cgemm_thread_cc
#define	CGEMM_THREAD_CT		cgemm_thread_ct
#define	CGEMM_THREAD_TC		cgemm_thread_tc
#define	CGEMM_THREAD_TT		cgemm_thread_tt
#define	CGEMM_THREAD_NR		cgemm_thread_nr
#define	CGEMM_THREAD_TR		cgemm_thread_tr
#define	CGEMM_THREAD_CR		cgemm_thread_cr
#define	CGEMM_THREAD_RN		cgemm_thread_rn
#define	CGEMM_THREAD_RT		cgemm_thread_rt
#define	CGEMM_THREAD_RC		cgemm_thread_rc
#define	CGEMM_THREAD_RR		cgemm_thread_rr

#define	CSYMM_THREAD_LU		csymm_thread_LU
#define	CSYMM_THREAD_LL		csymm_thread_LL
#define	CSYMM_THREAD_RU		csymm_thread_RU
#define	CSYMM_THREAD_RL		csymm_thread_RL

#define	CHEMM_THREAD_LU		chemm_thread_LU
#define	CHEMM_THREAD_LL		chemm_thread_LL
#define	CHEMM_THREAD_RU		chemm_thread_RU
#define	CHEMM_THREAD_RL		chemm_thread_RL

#define	CSYRK_THREAD_UN		csyrk_thread_UN
#define	CSYRK_THREAD_UT		csyrk_thread_UT
#define	CSYRK_THREAD_LN		csyrk_thread_LN
#define	CSYRK_THREAD_LT		csyrk_thread_LT
#define	CSYRK_THREAD_UR		csyrk_thread_UN
#define	CSYRK_THREAD_UC		csyrk_thread_UT
#define	CSYRK_THREAD_LR		csyrk_thread_LN
#define	CSYRK_THREAD_LC		csyrk_thread_LT

#define	CHERK_THREAD_UN		cherk_thread_UN
#define	CHERK_THREAD_UT		cherk_thread_UT
#define	CHERK_THREAD_LN		cherk_thread_LN
#define	CHERK_THREAD_LT		cherk_thread_LT
#define	CHERK_THREAD_UR		cherk_thread_UR
#define	CHERK_THREAD_UC		cherk_thread_UC
#define	CHERK_THREAD_LR		cherk_thread_LR
#define	CHERK_THREAD_LC		cherk_thread_LC

#define	CGEMM3M_NN		cgemm3m_nn
#define	CGEMM3M_CN		cgemm3m_cn
#define	CGEMM3M_TN		cgemm3m_tn
#define	CGEMM3M_NC		cgemm3m_nc
#define	CGEMM3M_NT		cgemm3m_nt
#define	CGEMM3M_CC		cgemm3m_cc
#define	CGEMM3M_CT		cgemm3m_ct
#define	CGEMM3M_TC		cgemm3m_tc
#define	CGEMM3M_TT		cgemm3m_tt
#define	CGEMM3M_NR		cgemm3m_nr
#define	CGEMM3M_TR		cgemm3m_tr
#define	CGEMM3M_CR		cgemm3m_cr
#define	CGEMM3M_RN		cgemm3m_rn
#define	CGEMM3M_RT		cgemm3m_rt
#define	CGEMM3M_RC		cgemm3m_rc
#define	CGEMM3M_RR		cgemm3m_rr

#define	CGEMM3M_THREAD_NN	cgemm3m_thread_nn
#define	CGEMM3M_THREAD_CN	cgemm3m_thread_cn
#define	CGEMM3M_THREAD_TN	cgemm3m_thread_tn
#define	CGEMM3M_THREAD_NC	cgemm3m_thread_nc
#define	CGEMM3M_THREAD_NT	cgemm3m_thread_nt
#define	CGEMM3M_THREAD_CC	cgemm3m_thread_cc
#define	CGEMM3M_THREAD_CT	cgemm3m_thread_ct
#define	CGEMM3M_THREAD_TC	cgemm3m_thread_tc
#define	CGEMM3M_THREAD_TT	cgemm3m_thread_tt
#define	CGEMM3M_THREAD_NR	cgemm3m_thread_nr
#define	CGEMM3M_THREAD_TR	cgemm3m_thread_tr
#define	CGEMM3M_THREAD_CR	cgemm3m_thread_cr
#define	CGEMM3M_THREAD_RN	cgemm3m_thread_rn
#define	CGEMM3M_THREAD_RT	cgemm3m_thread_rt
#define	CGEMM3M_THREAD_RC	cgemm3m_thread_rc
#define	CGEMM3M_THREAD_RR	cgemm3m_thread_rr

#define	CSYMM3M_LU		csymm3m_LU
#define	CSYMM3M_LL		csymm3m_LL
#define	CSYMM3M_RU		csymm3m_RU
#define	CSYMM3M_RL		csymm3m_RL

#define	CSYMM3M_THREAD_LU	csymm3m_thread_LU
#define	CSYMM3M_THREAD_LL	csymm3m_thread_LL
#define	CSYMM3M_THREAD_RU	csymm3m_thread_RU
#define	CSYMM3M_THREAD_RL	csymm3m_thread_RL

#define	CHEMM3M_LU		chemm3m_LU
#define	CHEMM3M_LL		chemm3m_LL
#define	CHEMM3M_RU		chemm3m_RU
#define	CHEMM3M_RL		chemm3m_RL

#define	CHEMM3M_THREAD_LU	chemm3m_thread_LU
#define	CHEMM3M_THREAD_LL	chemm3m_thread_LL
#define	CHEMM3M_THREAD_RU	chemm3m_thread_RU
#define	CHEMM3M_THREAD_RL	chemm3m_thread_RL

#endif
