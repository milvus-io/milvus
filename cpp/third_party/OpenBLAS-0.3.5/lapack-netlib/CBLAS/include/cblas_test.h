/*
 * cblas_test.h
 * Written by Keita Teranishi
 */
#ifndef CBLAS_TEST_H
#define CBLAS_TEST_H
#include "cblas.h"
#include "cblas_mangling.h"

#define  TRUE           1
#define  PASSED         1
#define  TEST_ROW_MJR	1

#define  FALSE          0
#define  FAILED         0
#define  TEST_COL_MJR	0

#define  INVALID       -1
#define  UNDEFINED     -1

typedef struct { float real; float imag; } CBLAS_TEST_COMPLEX;
typedef struct { double real; double imag; } CBLAS_TEST_ZOMPLEX;

#define F77_xerbla 		F77_GLOBAL(xerbla,XERBLA)
/*
 * Level 1 BLAS
 */
#define F77_srotg 		F77_GLOBAL(srotgtest,SROTGTEST)
#define F77_srotmg 		F77_GLOBAL(srotmgtest,SROTMGTEST)
#define F77_srot 		F77_GLOBAL(srottest,SROTTEST)
#define F77_srotm 		F77_GLOBAL(srotmtest,SROTMTEST)
#define F77_drotg 		F77_GLOBAL(drotgtest,DROTGTEST)
#define F77_drotmg 		F77_GLOBAL(drotmgtest,DROTMGTEST)
#define F77_drot 		F77_GLOBAL(drottest,DROTTEST)
#define F77_drotm 		F77_GLOBAL(drotmtest,DROTMTEST)
#define F77_sswap 		F77_GLOBAL(sswaptest,SSWAPTEST)
#define F77_scopy 		F77_GLOBAL(scopytest,SCOPYTEST)
#define F77_saxpy 		F77_GLOBAL(saxpytest,SAXPYTEST)
#define F77_isamax 		F77_GLOBAL(isamaxtest,ISAMAXTEST)
#define F77_dswap 		F77_GLOBAL(dswaptest,DSWAPTEST)
#define F77_dcopy 		F77_GLOBAL(dcopytest,DCOPYTEST)
#define F77_daxpy 		F77_GLOBAL(daxpytest,DAXPYTEST)
#define F77_idamax 		F77_GLOBAL(idamaxtest,IDAMAXTEST)
#define F77_cswap 		F77_GLOBAL(cswaptest,CSWAPTEST)
#define F77_ccopy 		F77_GLOBAL(ccopytest,CCOPYTEST)
#define F77_caxpy 		F77_GLOBAL(caxpytest,CAXPYTEST)
#define F77_icamax 		F77_GLOBAL(icamaxtest,ICAMAXTEST)
#define F77_zswap 		F77_GLOBAL(zswaptest,ZSWAPTEST)
#define F77_zcopy 		F77_GLOBAL(zcopytest,ZCOPYTEST)
#define F77_zaxpy 		F77_GLOBAL(zaxpytest,ZAXPYTEST)
#define F77_izamax 		F77_GLOBAL(izamaxtest,IZAMAXTEST)
#define F77_sdot 		F77_GLOBAL(sdottest,SDOTTEST)
#define F77_ddot 		F77_GLOBAL(ddottest,DDOTTEST)
#define F77_dsdot 		F77_GLOBAL(dsdottest,DSDOTTEST)
#define F77_sscal 		F77_GLOBAL(sscaltest,SSCALTEST)
#define F77_dscal 		F77_GLOBAL(dscaltest,DSCALTEST)
#define F77_cscal 		F77_GLOBAL(cscaltest,CSCALTEST)
#define F77_zscal 		F77_GLOBAL(zscaltest,ZSCALTEST)
#define F77_csscal 		F77_GLOBAL(csscaltest,CSSCALTEST)
#define F77_zdscal 		F77_GLOBAL(zdscaltest,ZDSCALTEST)
#define F77_cdotu 		F77_GLOBAL(cdotutest,CDOTUTEST)
#define F77_cdotc 		F77_GLOBAL(cdotctest,CDOTCTEST)
#define F77_zdotu 		F77_GLOBAL(zdotutest,ZDOTUTEST)
#define F77_zdotc 		F77_GLOBAL(zdotctest,ZDOTCTEST)
#define F77_snrm2 		F77_GLOBAL(snrm2test,SNRM2TEST)
#define F77_sasum 		F77_GLOBAL(sasumtest,SASUMTEST)
#define F77_dnrm2 		F77_GLOBAL(dnrm2test,DNRM2TEST)
#define F77_dasum 		F77_GLOBAL(dasumtest,DASUMTEST)
#define F77_scnrm2 		F77_GLOBAL(scnrm2test,SCNRM2TEST)
#define F77_scasum 		F77_GLOBAL(scasumtest,SCASUMTEST)
#define F77_dznrm2 		F77_GLOBAL(dznrm2test,DZNRM2TEST)
#define F77_dzasum 		F77_GLOBAL(dzasumtest,DZASUMTEST)
#define F77_sdsdot 		F77_GLOBAL(sdsdottest, SDSDOTTEST)
/*
 * Level 2 BLAS
 */
#define F77_s2chke 		F77_GLOBAL(cs2chke,CS2CHKE)
#define F77_d2chke 		F77_GLOBAL(cd2chke,CD2CHKE)
#define F77_c2chke 		F77_GLOBAL(cc2chke,CC2CHKE)
#define F77_z2chke 		F77_GLOBAL(cz2chke,CZ2CHKE)
#define F77_ssymv 		F77_GLOBAL(cssymv,CSSYMV)
#define F77_ssbmv 		F77_GLOBAL(cssbmv,CSSBMV)
#define F77_sspmv 		F77_GLOBAL(csspmv,CSSPMV)
#define F77_sger 		F77_GLOBAL(csger,CSGER)
#define F77_ssyr 		F77_GLOBAL(cssyr,CSSYR)
#define F77_sspr 		F77_GLOBAL(csspr,CSSPR)
#define F77_ssyr2 		F77_GLOBAL(cssyr2,CSSYR2)
#define F77_sspr2 		F77_GLOBAL(csspr2,CSSPR2)
#define F77_dsymv 		F77_GLOBAL(cdsymv,CDSYMV)
#define F77_dsbmv 		F77_GLOBAL(cdsbmv,CDSBMV)
#define F77_dspmv 		F77_GLOBAL(cdspmv,CDSPMV)
#define F77_dger 		F77_GLOBAL(cdger,CDGER)
#define F77_dsyr 		F77_GLOBAL(cdsyr,CDSYR)
#define F77_dspr 		F77_GLOBAL(cdspr,CDSPR)
#define F77_dsyr2 		F77_GLOBAL(cdsyr2,CDSYR2)
#define F77_dspr2 		F77_GLOBAL(cdspr2,CDSPR2)
#define F77_chemv 		F77_GLOBAL(cchemv,CCHEMV)
#define F77_chbmv 		F77_GLOBAL(cchbmv,CCHBMV)
#define F77_chpmv 		F77_GLOBAL(cchpmv,CCHPMV)
#define F77_cgeru 		F77_GLOBAL(ccgeru,CCGERU)
#define F77_cgerc 		F77_GLOBAL(ccgerc,CCGERC)
#define F77_cher 		F77_GLOBAL(ccher,CCHER)
#define F77_chpr 		F77_GLOBAL(cchpr,CCHPR)
#define F77_cher2 		F77_GLOBAL(ccher2,CCHER2)
#define F77_chpr2 		F77_GLOBAL(cchpr2,CCHPR2)
#define F77_zhemv 		F77_GLOBAL(czhemv,CZHEMV)
#define F77_zhbmv 		F77_GLOBAL(czhbmv,CZHBMV)
#define F77_zhpmv 		F77_GLOBAL(czhpmv,CZHPMV)
#define F77_zgeru 		F77_GLOBAL(czgeru,CZGERU)
#define F77_zgerc 		F77_GLOBAL(czgerc,CZGERC)
#define F77_zher 		F77_GLOBAL(czher,CZHER)
#define F77_zhpr 		F77_GLOBAL(czhpr,CZHPR)
#define F77_zher2 		F77_GLOBAL(czher2,CZHER2)
#define F77_zhpr2 		F77_GLOBAL(czhpr2,CZHPR2)
#define F77_sgemv 		F77_GLOBAL(csgemv,CSGEMV)
#define F77_sgbmv 		F77_GLOBAL(csgbmv,CSGBMV)
#define F77_strmv 		F77_GLOBAL(cstrmv,CSTRMV)
#define F77_stbmv 		F77_GLOBAL(cstbmv,CSTBMV)
#define F77_stpmv 		F77_GLOBAL(cstpmv,CSTPMV)
#define F77_strsv 		F77_GLOBAL(cstrsv,CSTRSV)
#define F77_stbsv 		F77_GLOBAL(cstbsv,CSTBSV)
#define F77_stpsv 		F77_GLOBAL(cstpsv,CSTPSV)
#define F77_dgemv 		F77_GLOBAL(cdgemv,CDGEMV)
#define F77_dgbmv 		F77_GLOBAL(cdgbmv,CDGBMV)
#define F77_dtrmv 		F77_GLOBAL(cdtrmv,CDTRMV)
#define F77_dtbmv 		F77_GLOBAL(cdtbmv,CDTBMV)
#define F77_dtpmv 		F77_GLOBAL(cdtpmv,CDTPMV)
#define F77_dtrsv 		F77_GLOBAL(cdtrsv,CDTRSV)
#define F77_dtbsv 		F77_GLOBAL(cdtbsv,CDTBSV)
#define F77_dtpsv 		F77_GLOBAL(cdtpsv,CDTPSV)
#define F77_cgemv 		F77_GLOBAL(ccgemv,CCGEMV)
#define F77_cgbmv 		F77_GLOBAL(ccgbmv,CCGBMV)
#define F77_ctrmv 		F77_GLOBAL(cctrmv,CCTRMV)
#define F77_ctbmv 		F77_GLOBAL(cctbmv,CCTBMV)
#define F77_ctpmv 		F77_GLOBAL(cctpmv,CCTPMV)
#define F77_ctrsv 		F77_GLOBAL(cctrsv,CCTRSV)
#define F77_ctbsv 		F77_GLOBAL(cctbsv,CCTBSV)
#define F77_ctpsv 		F77_GLOBAL(cctpsv,CCTPSV)
#define F77_zgemv 		F77_GLOBAL(czgemv,CZGEMV)
#define F77_zgbmv 		F77_GLOBAL(czgbmv,CZGBMV)
#define F77_ztrmv 		F77_GLOBAL(cztrmv,CZTRMV)
#define F77_ztbmv 		F77_GLOBAL(cztbmv,CZTBMV)
#define F77_ztpmv 		F77_GLOBAL(cztpmv,CZTPMV)
#define F77_ztrsv 		F77_GLOBAL(cztrsv,CZTRSV)
#define F77_ztbsv 		F77_GLOBAL(cztbsv,CZTBSV)
#define F77_ztpsv 		F77_GLOBAL(cztpsv,CZTPSV)
/*
 * Level 3 BLAS
 */
#define F77_s3chke 		F77_GLOBAL(cs3chke,CS3CHKE)
#define F77_d3chke 		F77_GLOBAL(cd3chke,CD3CHKE)
#define F77_c3chke 		F77_GLOBAL(cc3chke,CC3CHKE)
#define F77_z3chke 		F77_GLOBAL(cz3chke,CZ3CHKE)
#define F77_chemm 		F77_GLOBAL(cchemm,CCHEMM)
#define F77_cherk 		F77_GLOBAL(ccherk,CCHERK)
#define F77_cher2k 		F77_GLOBAL(ccher2k,CCHER2K)
#define F77_zhemm 		F77_GLOBAL(czhemm,CZHEMM)
#define F77_zherk 		F77_GLOBAL(czherk,CZHERK)
#define F77_zher2k 		F77_GLOBAL(czher2k,CZHER2K)
#define F77_sgemm 		F77_GLOBAL(csgemm,CSGEMM)
#define F77_ssymm 		F77_GLOBAL(cssymm,CSSYMM)
#define F77_ssyrk 		F77_GLOBAL(cssyrk,CSSYRK)
#define F77_ssyr2k 		F77_GLOBAL(cssyr2k,CSSYR2K)
#define F77_strmm 		F77_GLOBAL(cstrmm,CSTRMM)
#define F77_strsm 		F77_GLOBAL(cstrsm,CSTRSM)
#define F77_dgemm 		F77_GLOBAL(cdgemm,CDGEMM)
#define F77_dsymm 		F77_GLOBAL(cdsymm,CDSYMM)
#define F77_dsyrk 		F77_GLOBAL(cdsyrk,CDSYRK)
#define F77_dsyr2k 		F77_GLOBAL(cdsyr2k,CDSYR2K)
#define F77_dtrmm 		F77_GLOBAL(cdtrmm,CDTRMM)
#define F77_dtrsm 		F77_GLOBAL(cdtrsm,CDTRSM)
#define F77_cgemm 		F77_GLOBAL(ccgemm,CCGEMM)
#define F77_csymm 		F77_GLOBAL(ccsymm,CCSYMM)
#define F77_csyrk 		F77_GLOBAL(ccsyrk,CCSYRK)
#define F77_csyr2k 		F77_GLOBAL(ccsyr2k,CCSYR2K)
#define F77_ctrmm 		F77_GLOBAL(cctrmm,CCTRMM)
#define F77_ctrsm 		F77_GLOBAL(cctrsm,CCTRSM)
#define F77_zgemm 		F77_GLOBAL(czgemm,CZGEMM)
#define F77_zsymm 		F77_GLOBAL(czsymm,CZSYMM)
#define F77_zsyrk 		F77_GLOBAL(czsyrk,CZSYRK)
#define F77_zsyr2k 		F77_GLOBAL(czsyr2k,CZSYR2K)
#define F77_ztrmm 		F77_GLOBAL(cztrmm,CZTRMM)
#define F77_ztrsm 		F77_GLOBAL(cztrsm, CZTRSM)

void get_transpose_type(char *type, CBLAS_TRANSPOSE *trans);
void get_uplo_type(char *type, CBLAS_UPLO *uplo);
void get_diag_type(char *type, CBLAS_DIAG *diag);
void get_side_type(char *type, CBLAS_SIDE *side);

#endif /* CBLAS_TEST_H */
