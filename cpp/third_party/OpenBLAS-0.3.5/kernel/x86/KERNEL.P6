SGEMMKERNEL    =  gemm_kernel_2x2.S
SGEMMINCOPY    =
SGEMMITCOPY    =
SGEMMONCOPY    =  ../generic/gemm_ncopy_2.c
SGEMMOTCOPY    =  ../generic/gemm_tcopy_2.c
SGEMMINCOPYOBJ =
SGEMMITCOPYOBJ =
SGEMMONCOPYOBJ =  sgemm_oncopy$(TSUFFIX).$(SUFFIX)
SGEMMOTCOPYOBJ =  sgemm_otcopy$(TSUFFIX).$(SUFFIX)
DGEMMKERNEL    =  gemm_kernel_2x2.S
DGEMMINCOPY    =
DGEMMITCOPY    =
DGEMMONCOPY    =  ../generic/gemm_ncopy_2.c
DGEMMOTCOPY    =  ../generic/gemm_tcopy_2.c
DGEMMINCOPYOBJ =
DGEMMITCOPYOBJ =
DGEMMONCOPYOBJ =  dgemm_oncopy$(TSUFFIX).$(SUFFIX)
DGEMMOTCOPYOBJ =  dgemm_otcopy$(TSUFFIX).$(SUFFIX)
CGEMMKERNEL    =  zgemm_kernel_1x1.S
CGEMMINCOPY    =
CGEMMITCOPY    =
CGEMMONCOPY    =  ../generic/zgemm_ncopy_1.c
CGEMMOTCOPY    =  ../generic/zgemm_tcopy_1.c
CGEMMINCOPYOBJ =
CGEMMITCOPYOBJ =
CGEMMONCOPYOBJ =  cgemm_oncopy$(TSUFFIX).$(SUFFIX)
CGEMMOTCOPYOBJ =  cgemm_otcopy$(TSUFFIX).$(SUFFIX)
ZGEMMKERNEL    =  zgemm_kernel_1x1.S
ZGEMMINCOPY    =
ZGEMMITCOPY    =
ZGEMMONCOPY    =  ../generic/zgemm_ncopy_1.c
ZGEMMOTCOPY    =  ../generic/zgemm_tcopy_1.c
ZGEMMINCOPYOBJ =
ZGEMMITCOPYOBJ =
ZGEMMONCOPYOBJ =  zgemm_oncopy$(TSUFFIX).$(SUFFIX)
ZGEMMOTCOPYOBJ =  zgemm_otcopy$(TSUFFIX).$(SUFFIX)

STRSMKERNEL_LN	=  trsm_kernel_LN_2x2.S
STRSMKERNEL_LT	=  trsm_kernel_LT_2x2.S
STRSMKERNEL_RN	=  trsm_kernel_LT_2x2.S
STRSMKERNEL_RT	=  trsm_kernel_RT_2x2.S

DTRSMKERNEL_LN	=  trsm_kernel_LN_2x2.S
DTRSMKERNEL_LT	=  trsm_kernel_LT_2x2.S
DTRSMKERNEL_RN	=  trsm_kernel_LT_2x2.S
DTRSMKERNEL_RT	=  trsm_kernel_RT_2x2.S

CTRSMKERNEL_LN	=  ztrsm_kernel_LT_1x1.S
CTRSMKERNEL_LT	=  ztrsm_kernel_LT_1x1.S
CTRSMKERNEL_RN	=  ztrsm_kernel_LT_1x1.S
CTRSMKERNEL_RT	=  ztrsm_kernel_LT_1x1.S

ZTRSMKERNEL_LN	=  ztrsm_kernel_LT_1x1.S
ZTRSMKERNEL_LT	=  ztrsm_kernel_LT_1x1.S
ZTRSMKERNEL_RN	=  ztrsm_kernel_LT_1x1.S
ZTRSMKERNEL_RT	=  ztrsm_kernel_LT_1x1.S

CGEMM3MKERNEL    =  zgemm3m_kernel_2x2_coppermine.S
ZGEMM3MKERNEL    =  zgemm3m_kernel_2x2_coppermine.S

