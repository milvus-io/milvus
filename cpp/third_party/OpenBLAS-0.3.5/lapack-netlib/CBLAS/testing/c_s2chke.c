#include <stdio.h>
#include <string.h>
#include "cblas.h"
#include "cblas_test.h"

int cblas_ok, cblas_lerr, cblas_info;
int link_xerbla=TRUE;
char *cblas_rout;

#ifdef F77_Char
void F77_xerbla(F77_Char F77_srname, void *vinfo);
#else
void F77_xerbla(char *srname, void *vinfo);
#endif

void chkxer(void) {
   extern int cblas_ok, cblas_lerr, cblas_info;
   extern int link_xerbla;
   extern char *cblas_rout;
   if (cblas_lerr == 1 ) {
      printf("***** ILLEGAL VALUE OF PARAMETER NUMBER %d NOT DETECTED BY %s *****\n", cblas_info, cblas_rout);
      cblas_ok = 0 ;
   }
   cblas_lerr = 1 ;
}

void F77_s2chke(char *rout) {
   char *sf = ( rout ) ;
   float  A[2] = {0.0,0.0},
          X[2] = {0.0,0.0},
          Y[2] = {0.0,0.0},
          ALPHA=0.0, BETA=0.0;
   extern int cblas_info, cblas_lerr, cblas_ok;
   extern int RowMajorStrg;
   extern char *cblas_rout;

   if (link_xerbla) /* call these first to link */
   {
      cblas_xerbla(cblas_info,cblas_rout,"");
      F77_xerbla(cblas_rout,&cblas_info);
   }

   cblas_ok = TRUE ;
   cblas_lerr = PASSED ;

   if (strncmp( sf,"cblas_sgemv",11)==0) {
      cblas_rout = "cblas_sgemv";
      cblas_info = 1;
      cblas_sgemv(INVALID, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sgemv(CblasColMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sgemv(CblasColMajor, CblasNoTrans, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_sgemv(CblasColMajor, CblasNoTrans, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_sgemv(CblasColMajor, CblasNoTrans, 2, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_sgemv(CblasColMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_sgemv(CblasColMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();

      cblas_info = 2; RowMajorStrg = TRUE; RowMajorStrg = TRUE;
      cblas_sgemv(CblasRowMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_sgemv(CblasRowMajor, CblasNoTrans, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_sgemv(CblasRowMajor, CblasNoTrans, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_sgemv(CblasRowMajor, CblasNoTrans, 0, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_sgemv(CblasRowMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_sgemv(CblasRowMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_sgbmv",11)==0) {
      cblas_rout = "cblas_sgbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_sgbmv(INVALID, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, INVALID, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, INVALID, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, 0, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, 0, 0, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, 2, 0, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, 0, 0, 1, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_sgbmv(CblasColMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, INVALID, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, INVALID, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, 0, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, 0, 0, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, 2, 0, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 1, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_sgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ssymv",11)==0) {
      cblas_rout = "cblas_ssymv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ssymv(INVALID, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssymv(CblasColMajor, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssymv(CblasColMajor, CblasUpper, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ssymv(CblasColMajor, CblasUpper, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssymv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_ssymv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ssymv(CblasRowMajor, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ssymv(CblasRowMajor, CblasUpper, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ssymv(CblasRowMajor, CblasUpper, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssymv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_ssymv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ssbmv",11)==0) {
      cblas_rout = "cblas_ssbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ssbmv(INVALID, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssbmv(CblasColMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssbmv(CblasColMajor, CblasUpper, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssbmv(CblasColMajor, CblasUpper, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_ssbmv(CblasColMajor, CblasUpper, 0, 1,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_ssbmv(CblasColMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_ssbmv(CblasColMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ssbmv(CblasRowMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ssbmv(CblasRowMajor, CblasUpper, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ssbmv(CblasRowMajor, CblasUpper, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_ssbmv(CblasRowMajor, CblasUpper, 0, 1,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_ssbmv(CblasRowMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_ssbmv(CblasRowMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_sspmv",11)==0) {
      cblas_rout = "cblas_sspmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_sspmv(INVALID, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sspmv(CblasColMajor, INVALID, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sspmv(CblasColMajor, CblasUpper, INVALID,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_sspmv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_sspmv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_sspmv(CblasRowMajor, INVALID, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_sspmv(CblasRowMajor, CblasUpper, INVALID,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_sspmv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_sspmv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_strmv",11)==0) {
      cblas_rout = "cblas_strmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_strmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_strmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_strmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_strmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_strmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_strmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_strmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_strmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_strmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_strmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_strmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_stbmv",11)==0) {
      cblas_rout = "cblas_stbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_stbmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_stbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_stbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_stpmv",11)==0) {
      cblas_rout = "cblas_stpmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_stpmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_stpmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_stpmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_stpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_stpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_stpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_stpmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_stpmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_stpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_stpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_stpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_strsv",11)==0) {
      cblas_rout = "cblas_strsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_strsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_strsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_strsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_strsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_strsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_strsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_strsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_strsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_strsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_strsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_strsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_stbsv",11)==0) {
      cblas_rout = "cblas_stbsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_stbsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_stbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_stbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_stpsv",11)==0) {
      cblas_rout = "cblas_stpsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_stpsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_stpsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_stpsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_stpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_stpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_stpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_stpsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_stpsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_stpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_stpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_stpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_sger",10)==0) {
      cblas_rout = "cblas_sger";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_sger(INVALID, 0, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sger(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sger(CblasColMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sger(CblasColMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_sger(CblasColMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_sger(CblasColMajor, 2, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_sger(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_sger(CblasRowMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sger(CblasRowMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_sger(CblasRowMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_sger(CblasRowMajor, 0, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_ssyr2",11)==0) {
      cblas_rout = "cblas_ssyr2";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ssyr2(INVALID, CblasUpper, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssyr2(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssyr2(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ssyr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssyr2(CblasColMajor, CblasUpper, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ssyr2(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ssyr2(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ssyr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssyr2(CblasRowMajor, CblasUpper, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_sspr2",11)==0) {
      cblas_rout = "cblas_sspr2";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_sspr2(INVALID, CblasUpper, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sspr2(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sspr2(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sspr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_sspr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_sspr2(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_sspr2(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sspr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_sspr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A );
      chkxer();
   } else if (strncmp( sf,"cblas_ssyr",10)==0) {
      cblas_rout = "cblas_ssyr";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ssyr(INVALID, CblasUpper, 0, ALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssyr(CblasColMajor, INVALID, 0, ALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssyr(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ssyr(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyr(CblasColMajor, CblasUpper, 2, ALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ssyr(CblasRowMajor, INVALID, 0, ALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ssyr(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ssyr(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyr(CblasRowMajor, CblasUpper, 2, ALPHA, X, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_sspr",10)==0) {
      cblas_rout = "cblas_sspr";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_sspr(INVALID, CblasUpper, 0, ALPHA, X, 1, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sspr(CblasColMajor, INVALID, 0, ALPHA, X, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sspr(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sspr(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sspr(CblasColMajor, INVALID, 0, ALPHA, X, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sspr(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sspr(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, A );
      chkxer();
   }
   if (cblas_ok == TRUE)
       printf(" %-12s PASSED THE TESTS OF ERROR-EXITS\n", cblas_rout);
   else
       printf("******* %s FAILED THE TESTS OF ERROR-EXITS *******\n",cblas_rout);
}
