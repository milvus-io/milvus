/*
 * cblas_f77.h
 * Written by Keita Teranishi
 *
 * Updated by Jeff Horner
 * Merged cblas_f77.h and cblas_fortran_header.h
 */

#ifndef CBLAS_F77_H
#define CBLAS_F77_H

#ifdef CRAY
   #include <fortran.h>
   #define F77_CHAR _fcd
   #define C2F_CHAR(a) ( _cptofcd( (a), 1 ) )
   #define C2F_STR(a, i) ( _cptofcd( (a), (i) ) )
   #define F77_STRLEN(a) (_fcdlen)
#endif

#ifdef WeirdNEC
   #define F77_INT long
#endif

#ifdef  F77_CHAR
   #define FCHAR F77_CHAR
#else
   #define FCHAR char *
#endif

#ifdef F77_INT
   #define FINT const F77_INT *
   #define FINT2 F77_INT *
#else
   #define FINT const int *
   #define FINT2 int *
#endif

/*
 * Level 1 BLAS
 */

#define F77_xerbla 		F77_GLOBAL(xerbla,XERBLA)
#define F77_srotg 		F77_GLOBAL(srotg,SROTG)
#define F77_srotmg 		F77_GLOBAL(srotmg,SROTMG)
#define F77_srot 		F77_GLOBAL(srot,SROT)
#define F77_srotm 		F77_GLOBAL(srotm,SROTM)
#define F77_drotg 		F77_GLOBAL(drotg,DROTG)
#define F77_drotmg 		F77_GLOBAL(drotmg,DROTMG)
#define F77_drot 		F77_GLOBAL(drot,DROT)
#define F77_drotm 		F77_GLOBAL(drotm,DROTM)
#define F77_sswap 		F77_GLOBAL(sswap,SSWAP)
#define F77_scopy 		F77_GLOBAL(scopy,SCOPY)
#define F77_saxpy 		F77_GLOBAL(saxpy,SAXPY)
#define F77_isamax_sub 	F77_GLOBAL(isamaxsub,ISAMAXSUB)
#define F77_dswap 		F77_GLOBAL(dswap,DSWAP)
#define F77_dcopy 		F77_GLOBAL(dcopy,DCOPY)
#define F77_daxpy 		F77_GLOBAL(daxpy,DAXPY)
#define F77_idamax_sub 	F77_GLOBAL(idamaxsub,IDAMAXSUB)
#define F77_cswap 		F77_GLOBAL(cswap,CSWAP)
#define F77_ccopy 		F77_GLOBAL(ccopy,CCOPY)
#define F77_caxpy 		F77_GLOBAL(caxpy,CAXPY)
#define F77_icamax_sub 	F77_GLOBAL(icamaxsub,ICAMAXSUB)
#define F77_zswap 		F77_GLOBAL(zswap,ZSWAP)
#define F77_zcopy 		F77_GLOBAL(zcopy,ZCOPY)
#define F77_zaxpy 		F77_GLOBAL(zaxpy,ZAXPY)
#define F77_izamax_sub 	F77_GLOBAL(izamaxsub,IZAMAXSUB)
#define F77_sdot_sub 	F77_GLOBAL(sdotsub,SDOTSUB)
#define F77_ddot_sub 	F77_GLOBAL(ddotsub,DDOTSUB)
#define F77_dsdot_sub 	F77_GLOBAL(dsdotsub,DSDOTSUB)
#define F77_sscal 		F77_GLOBAL(sscal,SSCAL)
#define F77_dscal 		F77_GLOBAL(dscal,DSCAL)
#define F77_cscal 		F77_GLOBAL(cscal,CSCAL)
#define F77_zscal 		F77_GLOBAL(zscal,ZSCAL)
#define F77_csscal 		F77_GLOBAL(csscal,CSSCAL)
#define F77_zdscal 		F77_GLOBAL(zdscal,ZDSCAL)
#define F77_cdotu_sub 	F77_GLOBAL(cdotusub,CDOTUSUB)
#define F77_cdotc_sub 	F77_GLOBAL(cdotcsub,CDOTCSUB)
#define F77_zdotu_sub 	F77_GLOBAL(zdotusub,ZDOTUSUB)
#define F77_zdotc_sub 	F77_GLOBAL(zdotcsub,ZDOTCSUB)
#define F77_snrm2_sub 	F77_GLOBAL(snrm2sub,SNRM2SUB)
#define F77_sasum_sub 	F77_GLOBAL(sasumsub,SASUMSUB)
#define F77_dnrm2_sub 	F77_GLOBAL(dnrm2sub,DNRM2SUB)
#define F77_dasum_sub 	F77_GLOBAL(dasumsub,DASUMSUB)
#define F77_scnrm2_sub 	F77_GLOBAL(scnrm2sub,SCNRM2SUB)
#define F77_scasum_sub 	F77_GLOBAL(scasumsub,SCASUMSUB)
#define F77_dznrm2_sub 	F77_GLOBAL(dznrm2sub,DZNRM2SUB)
#define F77_dzasum_sub 	F77_GLOBAL(dzasumsub,DZASUMSUB)
#define F77_sdsdot_sub 	F77_GLOBAL(sdsdotsub,SDSDOTSUB)
/*
 * Level 2 BLAS
 */
#define F77_ssymv 		F77_GLOBAL(ssymv,SSYMV)
#define F77_ssbmv 		F77_GLOBAL(ssbmv,SSBMV)
#define F77_sspmv 		F77_GLOBAL(sspmv,SSPMV)
#define F77_sger 		F77_GLOBAL(sger,SGER)
#define F77_ssyr 		F77_GLOBAL(ssyr,SSYR)
#define F77_sspr 		F77_GLOBAL(sspr,SSPR)
#define F77_ssyr2 		F77_GLOBAL(ssyr2,SSYR2)
#define F77_sspr2 		F77_GLOBAL(sspr2,SSPR2)
#define F77_dsymv 		F77_GLOBAL(dsymv,DSYMV)
#define F77_dsbmv 		F77_GLOBAL(dsbmv,DSBMV)
#define F77_dspmv 		F77_GLOBAL(dspmv,DSPMV)
#define F77_dger 		F77_GLOBAL(dger,DGER)
#define F77_dsyr 		F77_GLOBAL(dsyr,DSYR)
#define F77_dspr 		F77_GLOBAL(dspr,DSPR)
#define F77_dsyr2 		F77_GLOBAL(dsyr2,DSYR2)
#define F77_dspr2 		F77_GLOBAL(dspr2,DSPR2)
#define F77_chemv 		F77_GLOBAL(chemv,CHEMV)
#define F77_chbmv 		F77_GLOBAL(chbmv,CHBMV)
#define F77_chpmv 		F77_GLOBAL(chpmv,CHPMV)
#define F77_cgeru 		F77_GLOBAL(cgeru,CGERU)
#define F77_cgerc 		F77_GLOBAL(cgerc,CGERC)
#define F77_cher 		F77_GLOBAL(cher,CHER)
#define F77_chpr 		F77_GLOBAL(chpr,CHPR)
#define F77_cher2 		F77_GLOBAL(cher2,CHER2)
#define F77_chpr2 		F77_GLOBAL(chpr2,CHPR2)
#define F77_zhemv 		F77_GLOBAL(zhemv,ZHEMV)
#define F77_zhbmv 		F77_GLOBAL(zhbmv,ZHBMV)
#define F77_zhpmv 		F77_GLOBAL(zhpmv,ZHPMV)
#define F77_zgeru 		F77_GLOBAL(zgeru,ZGERU)
#define F77_zgerc 		F77_GLOBAL(zgerc,ZGERC)
#define F77_zher 		F77_GLOBAL(zher,ZHER)
#define F77_zhpr 		F77_GLOBAL(zhpr,ZHPR)
#define F77_zher2 		F77_GLOBAL(zher2,ZHER2)
#define F77_zhpr2 		F77_GLOBAL(zhpr2,ZHPR2)
#define F77_sgemv 		F77_GLOBAL(sgemv,SGEMV)
#define F77_sgbmv 		F77_GLOBAL(sgbmv,SGBMV)
#define F77_strmv 		F77_GLOBAL(strmv,STRMV)
#define F77_stbmv 		F77_GLOBAL(stbmv,STBMV)
#define F77_stpmv 		F77_GLOBAL(stpmv,STPMV)
#define F77_strsv 		F77_GLOBAL(strsv,STRSV)
#define F77_stbsv 		F77_GLOBAL(stbsv,STBSV)
#define F77_stpsv 		F77_GLOBAL(stpsv,STPSV)
#define F77_dgemv 		F77_GLOBAL(dgemv,DGEMV)
#define F77_dgbmv 		F77_GLOBAL(dgbmv,DGBMV)
#define F77_dtrmv 		F77_GLOBAL(dtrmv,DTRMV)
#define F77_dtbmv 		F77_GLOBAL(dtbmv,DTBMV)
#define F77_dtpmv 		F77_GLOBAL(dtpmv,DTPMV)
#define F77_dtrsv 		F77_GLOBAL(dtrsv,DTRSV)
#define F77_dtbsv 		F77_GLOBAL(dtbsv,DTBSV)
#define F77_dtpsv 		F77_GLOBAL(dtpsv,DTPSV)
#define F77_cgemv 		F77_GLOBAL(cgemv,CGEMV)
#define F77_cgbmv 		F77_GLOBAL(cgbmv,CGBMV)
#define F77_ctrmv 		F77_GLOBAL(ctrmv,CTRMV)
#define F77_ctbmv 		F77_GLOBAL(ctbmv,CTBMV)
#define F77_ctpmv 		F77_GLOBAL(ctpmv,CTPMV)
#define F77_ctrsv 		F77_GLOBAL(ctrsv,CTRSV)
#define F77_ctbsv 		F77_GLOBAL(ctbsv,CTBSV)
#define F77_ctpsv 		F77_GLOBAL(ctpsv,CTPSV)
#define F77_zgemv 		F77_GLOBAL(zgemv,ZGEMV)
#define F77_zgbmv 		F77_GLOBAL(zgbmv,ZGBMV)
#define F77_ztrmv 		F77_GLOBAL(ztrmv,ZTRMV)
#define F77_ztbmv 		F77_GLOBAL(ztbmv,ZTBMV)
#define F77_ztpmv 		F77_GLOBAL(ztpmv,ZTPMV)
#define F77_ztrsv 		F77_GLOBAL(ztrsv,ZTRSV)
#define F77_ztbsv 		F77_GLOBAL(ztbsv,ZTBSV)
#define F77_ztpsv 		F77_GLOBAL(ztpsv,ZTPSV)
/*
 * Level 3 BLAS
 */
#define F77_chemm 		F77_GLOBAL(chemm,CHEMM)
#define F77_cherk 		F77_GLOBAL(cherk,CHERK)
#define F77_cher2k 		F77_GLOBAL(cher2k,CHER2K)
#define F77_zhemm 		F77_GLOBAL(zhemm,ZHEMM)
#define F77_zherk 		F77_GLOBAL(zherk,ZHERK)
#define F77_zher2k 		F77_GLOBAL(zher2k,ZHER2K)
#define F77_sgemm 		F77_GLOBAL(sgemm,SGEMM)
#define F77_ssymm 		F77_GLOBAL(ssymm,SSYMM)
#define F77_ssyrk 		F77_GLOBAL(ssyrk,SSYRK)
#define F77_ssyr2k 		F77_GLOBAL(ssyr2k,SSYR2K)
#define F77_strmm 		F77_GLOBAL(strmm,STRMM)
#define F77_strsm 		F77_GLOBAL(strsm,STRSM)
#define F77_dgemm 		F77_GLOBAL(dgemm,DGEMM)
#define F77_dsymm 		F77_GLOBAL(dsymm,DSYMM)
#define F77_dsyrk 		F77_GLOBAL(dsyrk,DSYRK)
#define F77_dsyr2k 		F77_GLOBAL(dsyr2k,DSYR2K)
#define F77_dtrmm 		F77_GLOBAL(dtrmm,DTRMM)
#define F77_dtrsm 		F77_GLOBAL(dtrsm,DTRSM)
#define F77_cgemm 		F77_GLOBAL(cgemm,CGEMM)
#define F77_csymm 		F77_GLOBAL(csymm,CSYMM)
#define F77_csyrk 		F77_GLOBAL(csyrk,CSYRK)
#define F77_csyr2k 		F77_GLOBAL(csyr2k,CSYR2K)
#define F77_ctrmm 		F77_GLOBAL(ctrmm,CTRMM)
#define F77_ctrsm 		F77_GLOBAL(ctrsm,CTRSM)
#define F77_zgemm 		F77_GLOBAL(zgemm,ZGEMM)
#define F77_zsymm 		F77_GLOBAL(zsymm,ZSYMM)
#define F77_zsyrk 		F77_GLOBAL(zsyrk,ZSYRK)
#define F77_zsyr2k 		F77_GLOBAL(zsyr2k,ZSYR2K)
#define F77_ztrmm 		F77_GLOBAL(ztrmm,ZTRMM)
#define F77_ztrsm 		F77_GLOBAL(ztrsm,ZTRSM)

#ifdef __cplusplus
extern "C" {
#endif

void F77_xerbla(FCHAR, void *);
/*
 * Level 1 Fortran Prototypes
 */

/* Single Precision */

   void F77_srot(FINT, float *, FINT, float *, FINT, const float *, const float *);
   void F77_srotg(float *,float *,float *,float *);
   void F77_srotm( FINT, float *, FINT, float *, FINT, const float *);
   void F77_srotmg(float *,float *,float *,const float *, float *);
   void F77_sswap( FINT, float *, FINT, float *, FINT);
   void F77_scopy( FINT, const float *, FINT, float *, FINT);
   void F77_saxpy( FINT, const float *, const float *, FINT, float *, FINT);
   void F77_sdot_sub(FINT, const float *, FINT, const float *, FINT, float *);
   void F77_sdsdot_sub( FINT, const float *, const float *, FINT, const float *, FINT, float *);
   void F77_sscal( FINT, const float *, float *, FINT);
   void F77_snrm2_sub( FINT, const float *, FINT, float *);
   void F77_sasum_sub( FINT, const float *, FINT, float *);
   void F77_isamax_sub( FINT, const float * , FINT, FINT2);

/* Double Precision */

   void F77_drot(FINT, double *, FINT, double *, FINT, const double *, const double *);
   void F77_drotg(double *,double *,double *,double *);
   void F77_drotm( FINT, double *, FINT, double *, FINT, const double *);
   void F77_drotmg(double *,double *,double *,const double *, double *);
   void F77_dswap( FINT, double *, FINT, double *, FINT);
   void F77_dcopy( FINT, const double *, FINT, double *, FINT);
   void F77_daxpy( FINT, const double *, const double *, FINT, double *, FINT);
   void F77_dswap( FINT, double *, FINT, double *, FINT);
   void F77_dsdot_sub(FINT, const float *, FINT, const float *, FINT, double *);
   void F77_ddot_sub( FINT, const double *, FINT, const double *, FINT, double *);
   void F77_dscal( FINT, const double *, double *, FINT);
   void F77_dnrm2_sub( FINT, const double *, FINT, double *);
   void F77_dasum_sub( FINT, const double *, FINT, double *);
   void F77_idamax_sub( FINT, const double * , FINT, FINT2);

/* Single Complex Precision */

   void F77_cswap( FINT, void *, FINT, void *, FINT);
   void F77_ccopy( FINT, const void *, FINT, void *, FINT);
   void F77_caxpy( FINT, const void *, const void *, FINT, void *, FINT);
   void F77_cswap( FINT, void *, FINT, void *, FINT);
   void F77_cdotc_sub( FINT, const void *, FINT, const void *, FINT, void *);
   void F77_cdotu_sub( FINT, const void *, FINT, const void *, FINT, void *);
   void F77_cscal( FINT, const void *, void *, FINT);
   void F77_icamax_sub( FINT, const void *, FINT, FINT2);
   void F77_csscal( FINT, const float *, void *, FINT);
   void F77_scnrm2_sub( FINT, const void *, FINT, float *);
   void F77_scasum_sub( FINT, const void *, FINT, float *);

/* Double Complex Precision */

   void F77_zswap( FINT, void *, FINT, void *, FINT);
   void F77_zcopy( FINT, const void *, FINT, void *, FINT);
   void F77_zaxpy( FINT, const void *, const void *, FINT, void *, FINT);
   void F77_zswap( FINT, void *, FINT, void *, FINT);
   void F77_zdotc_sub( FINT, const void *, FINT, const void *, FINT, void *);
   void F77_zdotu_sub( FINT, const void *, FINT, const void *, FINT, void *);
   void F77_zdscal( FINT, const double *, void *, FINT);
   void F77_zscal( FINT, const void *, void *, FINT);
   void F77_dznrm2_sub( FINT, const void *, FINT, double *);
   void F77_dzasum_sub( FINT, const void *, FINT, double *);
   void F77_izamax_sub( FINT, const void *, FINT, FINT2);

/*
 * Level 2 Fortran Prototypes
 */

/* Single Precision */

   void F77_sgemv(FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_sgbmv(FCHAR, FINT, FINT, FINT, FINT, const float *,  const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_ssymv(FCHAR, FINT, const float *, const float *, FINT, const float *,  FINT, const float *, float *, FINT);
   void F77_ssbmv(FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_sspmv(FCHAR, FINT, const float *, const float *, const float *, FINT, const float *, float *, FINT);
   void F77_strmv( FCHAR, FCHAR, FCHAR, FINT, const float *, FINT, float *, FINT);
   void F77_stbmv( FCHAR, FCHAR, FCHAR, FINT, FINT, const float *, FINT, float *, FINT);
   void F77_strsv( FCHAR, FCHAR, FCHAR, FINT, const float *, FINT, float *, FINT);
   void F77_stbsv( FCHAR, FCHAR, FCHAR, FINT, FINT, const float *, FINT, float *, FINT);
   void F77_stpmv( FCHAR, FCHAR, FCHAR, FINT, const float *, float *, FINT);
   void F77_stpsv( FCHAR, FCHAR, FCHAR, FINT, const float *, float *, FINT);
   void F77_sger( FINT, FINT, const float *, const float *, FINT, const float *, FINT, float *, FINT);
   void F77_ssyr(FCHAR, FINT, const float *, const float *, FINT, float *, FINT);
   void F77_sspr(FCHAR, FINT, const float *, const float *, FINT, float *);
   void F77_sspr2(FCHAR, FINT, const float *, const float *, FINT, const float *, FINT,  float *);
   void F77_ssyr2(FCHAR, FINT, const float *, const float *, FINT, const float *, FINT,  float *, FINT);

/* Double Precision */

   void F77_dgemv(FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_dgbmv(FCHAR, FINT, FINT, FINT, FINT, const double *,  const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_dsymv(FCHAR, FINT, const double *, const double *, FINT, const double *,  FINT, const double *, double *, FINT);
   void F77_dsbmv(FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_dspmv(FCHAR, FINT, const double *, const double *, const double *, FINT, const double *, double *, FINT);
   void F77_dtrmv( FCHAR, FCHAR, FCHAR, FINT, const double *, FINT, double *, FINT);
   void F77_dtbmv( FCHAR, FCHAR, FCHAR, FINT, FINT, const double *, FINT, double *, FINT);
   void F77_dtrsv( FCHAR, FCHAR, FCHAR, FINT, const double *, FINT, double *, FINT);
   void F77_dtbsv( FCHAR, FCHAR, FCHAR, FINT, FINT, const double *, FINT, double *, FINT);
   void F77_dtpmv( FCHAR, FCHAR, FCHAR, FINT, const double *, double *, FINT);
   void F77_dtpsv( FCHAR, FCHAR, FCHAR, FINT, const double *, double *, FINT);
   void F77_dger( FINT, FINT, const double *, const double *, FINT, const double *, FINT, double *, FINT);
   void F77_dsyr(FCHAR, FINT, const double *, const double *, FINT, double *, FINT);
   void F77_dspr(FCHAR, FINT, const double *, const double *, FINT, double *);
   void F77_dspr2(FCHAR, FINT, const double *, const double *, FINT, const double *, FINT,  double *);
   void F77_dsyr2(FCHAR, FINT, const double *, const double *, FINT, const double *, FINT,  double *, FINT);

/* Single Complex Precision */

   void F77_cgemv(FCHAR, FINT, FINT, const void *, const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_cgbmv(FCHAR, FINT, FINT, FINT, FINT, const void *,  const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_chemv(FCHAR, FINT, const void *, const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_chbmv(FCHAR, FINT, FINT, const void *, const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_chpmv(FCHAR, FINT, const void *, const void *, const void *, FINT, const void *, void *, FINT);
   void F77_ctrmv( FCHAR, FCHAR, FCHAR, FINT, const void *, FINT, void *, FINT);
   void F77_ctbmv( FCHAR, FCHAR, FCHAR, FINT, FINT, const void *, FINT, void *, FINT);
   void F77_ctpmv( FCHAR, FCHAR, FCHAR, FINT, const void *, void *, FINT);
   void F77_ctrsv( FCHAR, FCHAR, FCHAR, FINT, const void *, FINT, void *, FINT);
   void F77_ctbsv( FCHAR, FCHAR, FCHAR, FINT, FINT, const void *, FINT, void *, FINT);
   void F77_ctpsv( FCHAR, FCHAR, FCHAR, FINT, const void *, void *,FINT);
   void F77_cgerc( FINT, FINT, const void *, const void *, FINT, const void *, FINT, void *, FINT);
   void F77_cgeru( FINT, FINT, const void *, const void *, FINT, const void *, FINT, void *,  FINT);
   void F77_cher(FCHAR, FINT, const float *, const void *, FINT, void *, FINT);
   void F77_cher2(FCHAR, FINT, const void *, const void *, FINT, const void *, FINT, void *, FINT);
   void F77_chpr(FCHAR, FINT, const float *, const void *, FINT, void *);
   void F77_chpr2(FCHAR, FINT, const float *, const void *, FINT, const void *, FINT, void *);

/* Double Complex Precision */

   void F77_zgemv(FCHAR, FINT, FINT, const void *, const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_zgbmv(FCHAR, FINT, FINT, FINT, FINT, const void *,  const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_zhemv(FCHAR, FINT, const void *, const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_zhbmv(FCHAR, FINT, FINT, const void *, const void *, FINT, const void *, FINT, const void *, void *, FINT);
   void F77_zhpmv(FCHAR, FINT, const void *, const void *, const void *, FINT, const void *, void *, FINT);
   void F77_ztrmv( FCHAR, FCHAR, FCHAR, FINT, const void *, FINT, void *, FINT);
   void F77_ztbmv( FCHAR, FCHAR, FCHAR, FINT, FINT, const void *, FINT, void *, FINT);
   void F77_ztpmv( FCHAR, FCHAR, FCHAR, FINT, const void *, void *, FINT);
   void F77_ztrsv( FCHAR, FCHAR, FCHAR, FINT, const void *, FINT, void *, FINT);
   void F77_ztbsv( FCHAR, FCHAR, FCHAR, FINT, FINT, const void *, FINT, void *, FINT);
   void F77_ztpsv( FCHAR, FCHAR, FCHAR, FINT, const void *, void *,FINT);
   void F77_zgerc( FINT, FINT, const void *, const void *, FINT, const void *, FINT, void *, FINT);
   void F77_zgeru( FINT, FINT, const void *, const void *, FINT, const void *, FINT, void *,  FINT);
   void F77_zher(FCHAR, FINT, const double *, const void *, FINT, void *, FINT);
   void F77_zher2(FCHAR, FINT, const void *, const void *, FINT, const void *, FINT, void *, FINT);
   void F77_zhpr(FCHAR, FINT, const double *, const void *, FINT, void *);
   void F77_zhpr2(FCHAR, FINT, const double *, const void *, FINT, const void *, FINT, void *);

/*
 * Level 3 Fortran Prototypes
 */

/* Single Precision */

   void F77_sgemm(FCHAR, FCHAR, FINT, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_ssymm(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_ssyrk(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, float *, FINT);
   void F77_ssyr2k(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_strmm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, float *, FINT);
   void F77_strsm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, float *, FINT);

/* Double Precision */

   void F77_dgemm(FCHAR, FCHAR, FINT, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_dsymm(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_dsyrk(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, double *, FINT);
   void F77_dsyr2k(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_dtrmm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, double *, FINT);
   void F77_dtrsm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, double *, FINT);

/* Single Complex Precision */

   void F77_cgemm(FCHAR, FCHAR, FINT, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_csymm(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_chemm(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_csyrk(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, float *, FINT);
   void F77_cherk(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, float *, FINT);
   void F77_csyr2k(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_cher2k(FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, const float *, FINT, const float *, float *, FINT);
   void F77_ctrmm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, float *, FINT);
   void F77_ctrsm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const float *, const float *, FINT, float *, FINT);

/* Double Complex Precision */

   void F77_zgemm(FCHAR, FCHAR, FINT, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_zsymm(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_zhemm(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_zsyrk(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, double *, FINT);
   void F77_zherk(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, double *, FINT);
   void F77_zsyr2k(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_zher2k(FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, const double *, FINT, const double *, double *, FINT);
   void F77_ztrmm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, double *, FINT);
   void F77_ztrsm(FCHAR, FCHAR, FCHAR, FCHAR, FINT, FINT, const double *, const double *, FINT, double *, FINT);

#ifdef __cplusplus
}
#endif

#endif /*  CBLAS_F77_H */
