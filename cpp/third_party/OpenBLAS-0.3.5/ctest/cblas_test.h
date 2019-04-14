/*
 * cblas_test.h
 * Written by Keita Teranishi
 */
#ifndef CBLAS_TEST_H
#define CBLAS_TEST_H
#include "cblas.h"

#ifdef USE64BITINT
#define int long
#endif

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

#if defined(ADD_)
/*
 * Level 1 BLAS
 */
   #define F77_srotg      srotgtest_
   #define F77_srotmg     srotmgtest_
   #define F77_srot       srottest_
   #define F77_srotm      srotmtest_
   #define F77_drotg      drotgtest_
   #define F77_drotmg     drotmgtest_
   #define F77_drot       drottest_
   #define F77_drotm      drotmtest_
   #define F77_sswap      sswaptest_
   #define F77_scopy      scopytest_
   #define F77_saxpy      saxpytest_
   #define F77_isamax     isamaxtest_
   #define F77_dswap      dswaptest_
   #define F77_dcopy      dcopytest_
   #define F77_daxpy      daxpytest_
   #define F77_idamax     idamaxtest_
   #define F77_cswap      cswaptest_
   #define F77_ccopy      ccopytest_
   #define F77_caxpy      caxpytest_
   #define F77_icamax     icamaxtest_
   #define F77_zswap      zswaptest_
   #define F77_zcopy      zcopytest_
   #define F77_zaxpy      zaxpytest_
   #define F77_izamax     izamaxtest_
   #define F77_sdot       sdottest_
   #define F77_ddot       ddottest_
   #define F77_dsdot      dsdottest_
   #define F77_sscal      sscaltest_
   #define F77_dscal      dscaltest_
   #define F77_cscal      cscaltest_
   #define F77_zscal      zscaltest_
   #define F77_csscal     csscaltest_
   #define F77_zdscal      zdscaltest_
   #define F77_cdotu      cdotutest_
   #define F77_cdotc      cdotctest_
   #define F77_zdotu      zdotutest_
   #define F77_zdotc      zdotctest_
   #define F77_snrm2      snrm2test_
   #define F77_sasum      sasumtest_
   #define F77_dnrm2      dnrm2test_
   #define F77_dasum      dasumtest_
   #define F77_scnrm2     scnrm2test_
   #define F77_scasum     scasumtest_
   #define F77_dznrm2     dznrm2test_
   #define F77_dzasum     dzasumtest_
   #define F77_sdsdot     sdsdottest_
/*
 * Level 2 BLAS
 */
   #define F77_s2chke     cs2chke_
   #define F77_d2chke     cd2chke_
   #define F77_c2chke     cc2chke_
   #define F77_z2chke     cz2chke_
   #define F77_ssymv      cssymv_
   #define F77_ssbmv      cssbmv_
   #define F77_sspmv      csspmv_
   #define F77_sger       csger_
   #define F77_ssyr       cssyr_
   #define F77_sspr       csspr_
   #define F77_ssyr2      cssyr2_
   #define F77_sspr2      csspr2_
   #define F77_dsymv      cdsymv_
   #define F77_dsbmv      cdsbmv_
   #define F77_dspmv      cdspmv_
   #define F77_dger       cdger_
   #define F77_dsyr       cdsyr_
   #define F77_dspr       cdspr_
   #define F77_dsyr2      cdsyr2_
   #define F77_dspr2      cdspr2_
   #define F77_chemv      cchemv_
   #define F77_chbmv      cchbmv_
   #define F77_chpmv      cchpmv_
   #define F77_cgeru      ccgeru_
   #define F77_cgerc      ccgerc_
   #define F77_cher       ccher_
   #define F77_chpr       cchpr_
   #define F77_cher2      ccher2_
   #define F77_chpr2      cchpr2_
   #define F77_zhemv      czhemv_
   #define F77_zhbmv      czhbmv_
   #define F77_zhpmv      czhpmv_
   #define F77_zgeru      czgeru_
   #define F77_zgerc      czgerc_
   #define F77_zher       czher_
   #define F77_zhpr       czhpr_
   #define F77_zher2      czher2_
   #define F77_zhpr2      czhpr2_
   #define F77_sgemv      csgemv_
   #define F77_sgbmv      csgbmv_
   #define F77_strmv      cstrmv_
   #define F77_stbmv      cstbmv_
   #define F77_stpmv      cstpmv_
   #define F77_strsv      cstrsv_
   #define F77_stbsv      cstbsv_
   #define F77_stpsv      cstpsv_
   #define F77_dgemv      cdgemv_
   #define F77_dgbmv      cdgbmv_
   #define F77_dtrmv      cdtrmv_
   #define F77_dtbmv      cdtbmv_
   #define F77_dtpmv      cdtpmv_
   #define F77_dtrsv      cdtrsv_
   #define F77_dtbsv      cdtbsv_
   #define F77_dtpsv      cdtpsv_
   #define F77_cgemv      ccgemv_
   #define F77_cgbmv      ccgbmv_
   #define F77_ctrmv      cctrmv_
   #define F77_ctbmv      cctbmv_
   #define F77_ctpmv      cctpmv_
   #define F77_ctrsv      cctrsv_
   #define F77_ctbsv      cctbsv_
   #define F77_ctpsv      cctpsv_
   #define F77_zgemv      czgemv_
   #define F77_zgbmv      czgbmv_
   #define F77_ztrmv      cztrmv_
   #define F77_ztbmv      cztbmv_
   #define F77_ztpmv      cztpmv_
   #define F77_ztrsv      cztrsv_
   #define F77_ztbsv      cztbsv_
   #define F77_ztpsv      cztpsv_
/*
 * Level 3 BLAS
 */
   #define F77_s3chke     cs3chke_
   #define F77_d3chke     cd3chke_
   #define F77_c3chke     cc3chke_
   #define F77_z3chke     cz3chke_
   #define F77_chemm      cchemm_
   #define F77_cherk      ccherk_
   #define F77_cher2k     ccher2k_
   #define F77_zhemm      czhemm_
   #define F77_zherk      czherk_
   #define F77_zher2k     czher2k_
   #define F77_sgemm      csgemm_
   #define F77_ssymm      cssymm_
   #define F77_ssyrk      cssyrk_
   #define F77_ssyr2k     cssyr2k_
   #define F77_strmm      cstrmm_
   #define F77_strsm      cstrsm_
   #define F77_dgemm      cdgemm_
   #define F77_dsymm      cdsymm_
   #define F77_dsyrk      cdsyrk_
   #define F77_dsyr2k     cdsyr2k_
   #define F77_dtrmm      cdtrmm_
   #define F77_dtrsm      cdtrsm_
   #define F77_cgemm      ccgemm_
   #define F77_cgemm3m    ccgemm3m_
   #define F77_csymm      ccsymm_
   #define F77_csyrk      ccsyrk_
   #define F77_csyr2k     ccsyr2k_
   #define F77_ctrmm      cctrmm_
   #define F77_ctrsm      cctrsm_
   #define F77_zgemm      czgemm_
   #define F77_zgemm3m    czgemm3m_
   #define F77_zsymm      czsymm_
   #define F77_zsyrk      czsyrk_
   #define F77_zsyr2k     czsyr2k_
   #define F77_ztrmm      cztrmm_
   #define F77_ztrsm      cztrsm_
#elif defined(UPCASE)
/*
 * Level 1 BLAS
 */
   #define F77_srotg      SROTGTEST
   #define F77_srotmg     SROTMGTEST
   #define F77_srot       SROTCTEST
   #define F77_srotm      SROTMTEST
   #define F77_drotg      DROTGTEST
   #define F77_drotmg     DROTMGTEST
   #define F77_drot       DROTTEST
   #define F77_drotm      DROTMTEST
   #define F77_sswap      SSWAPTEST
   #define F77_scopy      SCOPYTEST
   #define F77_saxpy      SAXPYTEST
   #define F77_isamax     ISAMAXTEST
   #define F77_dswap      DSWAPTEST
   #define F77_dcopy      DCOPYTEST
   #define F77_daxpy      DAXPYTEST
   #define F77_idamax     IDAMAXTEST
   #define F77_cswap      CSWAPTEST
   #define F77_ccopy      CCOPYTEST
   #define F77_caxpy      CAXPYTEST
   #define F77_icamax     ICAMAXTEST
   #define F77_zswap      ZSWAPTEST
   #define F77_zcopy      ZCOPYTEST
   #define F77_zaxpy      ZAXPYTEST
   #define F77_izamax     IZAMAXTEST
   #define F77_sdot       SDOTTEST
   #define F77_ddot       DDOTTEST
   #define F77_dsdot       DSDOTTEST
   #define F77_sscal      SSCALTEST
   #define F77_dscal      DSCALTEST
   #define F77_cscal      CSCALTEST
   #define F77_zscal      ZSCALTEST
   #define F77_csscal      CSSCALTEST
   #define F77_zdscal      ZDSCALTEST
   #define F77_cdotu      CDOTUTEST
   #define F77_cdotc      CDOTCTEST
   #define F77_zdotu      ZDOTUTEST
   #define F77_zdotc      ZDOTCTEST
   #define F77_snrm2      SNRM2TEST
   #define F77_sasum      SASUMTEST
   #define F77_dnrm2      DNRM2TEST
   #define F77_dasum      DASUMTEST
   #define F77_scnrm2      SCNRM2TEST
   #define F77_scasum      SCASUMTEST
   #define F77_dznrm2      DZNRM2TEST
   #define F77_dzasum      DZASUMTEST
   #define F77_sdsdot       SDSDOTTEST
/*
 * Level 2 BLAS
 */
   #define F77_s2chke     CS2CHKE
   #define F77_d2chke     CD2CHKE
   #define F77_c2chke     CC2CHKE
   #define F77_z2chke     CZ2CHKE
   #define F77_ssymv      CSSYMV
   #define F77_ssbmv      CSSBMV
   #define F77_sspmv      CSSPMV
   #define F77_sger       CSGER
   #define F77_ssyr       CSSYR
   #define F77_sspr       CSSPR
   #define F77_ssyr2      CSSYR2
   #define F77_sspr2      CSSPR2
   #define F77_dsymv      CDSYMV
   #define F77_dsbmv      CDSBMV
   #define F77_dspmv      CDSPMV
   #define F77_dger       CDGER
   #define F77_dsyr       CDSYR
   #define F77_dspr       CDSPR
   #define F77_dsyr2      CDSYR2
   #define F77_dspr2      CDSPR2
   #define F77_chemv      CCHEMV
   #define F77_chbmv      CCHBMV
   #define F77_chpmv      CCHPMV
   #define F77_cgeru      CCGERU
   #define F77_cgerc      CCGERC
   #define F77_cher       CCHER
   #define F77_chpr       CCHPR
   #define F77_cher2      CCHER2
   #define F77_chpr2      CCHPR2
   #define F77_zhemv      CZHEMV
   #define F77_zhbmv      CZHBMV
   #define F77_zhpmv      CZHPMV
   #define F77_zgeru      CZGERU
   #define F77_zgerc      CZGERC
   #define F77_zher       CZHER
   #define F77_zhpr       CZHPR
   #define F77_zher2      CZHER2
   #define F77_zhpr2      CZHPR2
   #define F77_sgemv      CSGEMV
   #define F77_sgbmv      CSGBMV
   #define F77_strmv      CSTRMV
   #define F77_stbmv      CSTBMV
   #define F77_stpmv      CSTPMV
   #define F77_strsv      CSTRSV
   #define F77_stbsv      CSTBSV
   #define F77_stpsv      CSTPSV
   #define F77_dgemv      CDGEMV
   #define F77_dgbmv      CDGBMV
   #define F77_dtrmv      CDTRMV
   #define F77_dtbmv      CDTBMV
   #define F77_dtpmv      CDTPMV
   #define F77_dtrsv      CDTRSV
   #define F77_dtbsv      CDTBSV
   #define F77_dtpsv      CDTPSV
   #define F77_cgemv      CCGEMV
   #define F77_cgbmv      CCGBMV
   #define F77_ctrmv      CCTRMV
   #define F77_ctbmv      CCTBMV
   #define F77_ctpmv      CCTPMV
   #define F77_ctrsv      CCTRSV
   #define F77_ctbsv      CCTBSV
   #define F77_ctpsv      CCTPSV
   #define F77_zgemv      CZGEMV
   #define F77_zgbmv      CZGBMV
   #define F77_ztrmv      CZTRMV
   #define F77_ztbmv      CZTBMV
   #define F77_ztpmv      CZTPMV
   #define F77_ztrsv      CZTRSV
   #define F77_ztbsv      CZTBSV
   #define F77_ztpsv      CZTPSV
/*
 * Level 3 BLAS
 */
   #define F77_s3chke     CS3CHKE
   #define F77_d3chke     CD3CHKE
   #define F77_c3chke     CC3CHKE
   #define F77_z3chke     CZ3CHKE
   #define F77_chemm      CCHEMM
   #define F77_cherk      CCHERK
   #define F77_cher2k     CCHER2K
   #define F77_zhemm      CZHEMM
   #define F77_zherk      CZHERK
   #define F77_zher2k     CZHER2K
   #define F77_sgemm      CSGEMM
   #define F77_ssymm      CSSYMM
   #define F77_ssyrk      CSSYRK
   #define F77_ssyr2k     CSSYR2K
   #define F77_strmm      CSTRMM
   #define F77_strsm      CSTRSM
   #define F77_dgemm      CDGEMM
   #define F77_dsymm      CDSYMM
   #define F77_dsyrk      CDSYRK
   #define F77_dsyr2k     CDSYR2K
   #define F77_dtrmm      CDTRMM
   #define F77_dtrsm      CDTRSM
   #define F77_cgemm      CCGEMM
   #define F77_cgemm3m    CCGEMM3M
   #define F77_csymm      CCSYMM
   #define F77_csyrk      CCSYRK
   #define F77_csyr2k     CCSYR2K
   #define F77_ctrmm      CCTRMM
   #define F77_ctrsm      CCTRSM
   #define F77_zgemm      CZGEMM
   #define F77_zgemm3m    CZGEMM3M
   #define F77_zsymm      CZSYMM
   #define F77_zsyrk      CZSYRK
   #define F77_zsyr2k     CZSYR2K
   #define F77_ztrmm      CZTRMM
   #define F77_ztrsm      CZTRSM
#elif defined(NOCHANGE)
/*
 * Level 1 BLAS
 */
   #define F77_srotg      srotgtest
   #define F77_srotmg     srotmgtest
   #define F77_srot       srottest
   #define F77_srotm      srotmtest
   #define F77_drotg      drotgtest
   #define F77_drotmg     drotmgtest
   #define F77_drot       drottest
   #define F77_drotm      drotmtest
   #define F77_sswap      sswaptest
   #define F77_scopy      scopytest
   #define F77_saxpy      saxpytest
   #define F77_isamax     isamaxtest
   #define F77_dswap      dswaptest
   #define F77_dcopy      dcopytest
   #define F77_daxpy      daxpytest
   #define F77_idamax     idamaxtest
   #define F77_cswap      cswaptest
   #define F77_ccopy      ccopytest
   #define F77_caxpy      caxpytest
   #define F77_icamax     icamaxtest
   #define F77_zswap      zswaptest
   #define F77_zcopy      zcopytest
   #define F77_zaxpy      zaxpytest
   #define F77_izamax     izamaxtest
   #define F77_sdot       sdottest
   #define F77_ddot       ddottest
   #define F77_dsdot       dsdottest
   #define F77_sscal      sscaltest
   #define F77_dscal      dscaltest
   #define F77_cscal      cscaltest
   #define F77_zscal      zscaltest
   #define F77_csscal      csscaltest
   #define F77_zdscal      zdscaltest
   #define F77_cdotu  cdotutest
   #define F77_cdotc  cdotctest
   #define F77_zdotu  zdotutest
   #define F77_zdotc  zdotctest
   #define F77_snrm2  snrm2test
   #define F77_sasum  sasumtest
   #define F77_dnrm2  dnrm2test
   #define F77_dasum  dasumtest
   #define F77_scnrm2  scnrm2test
   #define F77_scasum  scasumtest
   #define F77_dznrm2  dznrm2test
   #define F77_dzasum  dzasumtest
   #define F77_sdsdot   sdsdottest
/*
 * Level 2 BLAS
 */
   #define F77_s2chke     cs2chke
   #define F77_d2chke     cd2chke
   #define F77_c2chke     cc2chke
   #define F77_z2chke     cz2chke
   #define F77_ssymv      cssymv
   #define F77_ssbmv      cssbmv
   #define F77_sspmv      csspmv
   #define F77_sger       csger
   #define F77_ssyr       cssyr
   #define F77_sspr       csspr
   #define F77_ssyr2      cssyr2
   #define F77_sspr2      csspr2
   #define F77_dsymv      cdsymv
   #define F77_dsbmv      cdsbmv
   #define F77_dspmv      cdspmv
   #define F77_dger       cdger
   #define F77_dsyr       cdsyr
   #define F77_dspr       cdspr
   #define F77_dsyr2      cdsyr2
   #define F77_dspr2      cdspr2
   #define F77_chemv      cchemv
   #define F77_chbmv      cchbmv
   #define F77_chpmv      cchpmv
   #define F77_cgeru      ccgeru
   #define F77_cgerc      ccgerc
   #define F77_cher       ccher
   #define F77_chpr       cchpr
   #define F77_cher2      ccher2
   #define F77_chpr2      cchpr2
   #define F77_zhemv      czhemv
   #define F77_zhbmv      czhbmv
   #define F77_zhpmv      czhpmv
   #define F77_zgeru      czgeru
   #define F77_zgerc      czgerc
   #define F77_zher       czher
   #define F77_zhpr       czhpr
   #define F77_zher2      czher2
   #define F77_zhpr2      czhpr2
   #define F77_sgemv      csgemv
   #define F77_sgbmv      csgbmv
   #define F77_strmv      cstrmv
   #define F77_stbmv      cstbmv
   #define F77_stpmv      cstpmv
   #define F77_strsv      cstrsv
   #define F77_stbsv      cstbsv
   #define F77_stpsv      cstpsv
   #define F77_dgemv      cdgemv
   #define F77_dgbmv      cdgbmv
   #define F77_dtrmv      cdtrmv
   #define F77_dtbmv      cdtbmv
   #define F77_dtpmv      cdtpmv
   #define F77_dtrsv      cdtrsv
   #define F77_dtbsv      cdtbsv
   #define F77_dtpsv      cdtpsv
   #define F77_cgemv      ccgemv
   #define F77_cgbmv      ccgbmv
   #define F77_ctrmv      cctrmv
   #define F77_ctbmv      cctbmv
   #define F77_ctpmv      cctpmv
   #define F77_ctrsv      cctrsv
   #define F77_ctbsv      cctbsv
   #define F77_ctpsv      cctpsv
   #define F77_zgemv      czgemv
   #define F77_zgbmv      czgbmv
   #define F77_ztrmv      cztrmv
   #define F77_ztbmv      cztbmv
   #define F77_ztpmv      cztpmv
   #define F77_ztrsv      cztrsv
   #define F77_ztbsv      cztbsv
   #define F77_ztpsv      cztpsv
/*
 * Level 3 BLAS
 */
   #define F77_s3chke     cs3chke
   #define F77_d3chke     cd3chke
   #define F77_c3chke     cc3chke
   #define F77_z3chke     cz3chke
   #define F77_chemm      cchemm
   #define F77_cherk      ccherk
   #define F77_cher2k     ccher2k
   #define F77_zhemm      czhemm
   #define F77_zherk      czherk
   #define F77_zher2k     czher2k
   #define F77_sgemm      csgemm
   #define F77_ssymm      cssymm
   #define F77_ssyrk      cssyrk
   #define F77_ssyr2k     cssyr2k
   #define F77_strmm      cstrmm
   #define F77_strsm      cstrsm
   #define F77_dgemm      cdgemm
   #define F77_dsymm      cdsymm
   #define F77_dsyrk      cdsyrk
   #define F77_dsyr2k     cdsyr2k
   #define F77_dtrmm      cdtrmm
   #define F77_dtrsm      cdtrsm
   #define F77_cgemm      ccgemm
   #define F77_cgemm3m    ccgemm3m
   #define F77_csymm      ccsymm
   #define F77_csyrk      ccsyrk
   #define F77_csyr2k     ccsyr2k
   #define F77_ctrmm      cctrmm
   #define F77_ctrsm      cctrsm
   #define F77_zgemm      czgemm
   #define F77_zgemm3m    czgemm3m
   #define F77_zsymm      czsymm
   #define F77_zsyrk      czsyrk
   #define F77_zsyr2k     czsyr2k
   #define F77_ztrmm      cztrmm
   #define F77_ztrsm      cztrsm
#endif

void get_transpose_type(char *type, enum CBLAS_TRANSPOSE *trans);
void get_uplo_type(char *type, enum CBLAS_UPLO *uplo);
void get_diag_type(char *type, enum CBLAS_DIAG *diag);
void get_side_type(char *type, enum CBLAS_SIDE *side);

#endif /* CBLAS_TEST_H */
