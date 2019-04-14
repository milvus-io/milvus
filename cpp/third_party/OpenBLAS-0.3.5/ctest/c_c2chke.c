#include <stdio.h>
#include <string.h>
#include "common.h"
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

void F77_c2chke(char *rout) {
   char *sf = ( rout ) ;
   float  A[2] = {0.0,0.0},
          X[2] = {0.0,0.0},
          Y[2] = {0.0,0.0},
          ALPHA[2] = {0.0,0.0},
          BETA[2]  = {0.0,0.0},
          RALPHA = 0.0;
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

   if (strncmp( sf,"cblas_cgemv",11)==0) {
      cblas_rout = "cblas_cgemv";
      cblas_info = 1;
      cblas_cgemv(INVALID, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_cgemv(CblasColMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_cgemv(CblasColMajor, CblasNoTrans, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_cgemv(CblasColMajor, CblasNoTrans, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_cgemv(CblasColMajor, CblasNoTrans, 2, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_cgemv(CblasColMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_cgemv(CblasColMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();

      cblas_info = 2; RowMajorStrg = TRUE; RowMajorStrg = TRUE;
      cblas_cgemv(CblasRowMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_cgemv(CblasRowMajor, CblasNoTrans, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_cgemv(CblasRowMajor, CblasNoTrans, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_cgemv(CblasRowMajor, CblasNoTrans, 0, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_cgemv(CblasRowMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_cgemv(CblasRowMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_cgbmv",11)==0) {
      cblas_rout = "cblas_cgbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_cgbmv(INVALID, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, INVALID, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, INVALID, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, 0, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, 0, 0, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, 2, 0, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, 0, 0, 1, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_cgbmv(CblasColMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, INVALID, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, INVALID, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, 0, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, 0, 0, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, 2, 0, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 1, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_cgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_chemv",11)==0) {
      cblas_rout = "cblas_chemv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_chemv(INVALID, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_chemv(CblasColMajor, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_chemv(CblasColMajor, CblasUpper, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_chemv(CblasColMajor, CblasUpper, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_chemv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_chemv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_chemv(CblasRowMajor, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_chemv(CblasRowMajor, CblasUpper, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_chemv(CblasRowMajor, CblasUpper, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_chemv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_chemv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_chbmv",11)==0) {
      cblas_rout = "cblas_chbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_chbmv(INVALID, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_chbmv(CblasColMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_chbmv(CblasColMajor, CblasUpper, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_chbmv(CblasColMajor, CblasUpper, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_chbmv(CblasColMajor, CblasUpper, 0, 1,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_chbmv(CblasColMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_chbmv(CblasColMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_chbmv(CblasRowMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_chbmv(CblasRowMajor, CblasUpper, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_chbmv(CblasRowMajor, CblasUpper, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_chbmv(CblasRowMajor, CblasUpper, 0, 1,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_chbmv(CblasRowMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_chbmv(CblasRowMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_chpmv",11)==0) {
      cblas_rout = "cblas_chpmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_chpmv(INVALID, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_chpmv(CblasColMajor, INVALID, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_chpmv(CblasColMajor, CblasUpper, INVALID,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_chpmv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_chpmv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_chpmv(CblasRowMajor, INVALID, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_chpmv(CblasRowMajor, CblasUpper, INVALID,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_chpmv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_chpmv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ctrmv",11)==0) {
      cblas_rout = "cblas_ctrmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ctrmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ctrmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ctrmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ctrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ctrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_ctrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_ctrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ctrmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ctrmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ctrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ctrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_ctrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_ctrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ctbmv",11)==0) {
      cblas_rout = "cblas_ctbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ctbmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ctbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ctbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ctpmv",11)==0) {
      cblas_rout = "cblas_ctpmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ctpmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ctpmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ctpmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ctpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ctpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ctpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ctpmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ctpmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ctpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ctpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ctpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ctrsv",11)==0) {
      cblas_rout = "cblas_ctrsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ctrsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ctrsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ctrsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ctrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ctrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_ctrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_ctrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ctrsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ctrsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ctrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ctrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_ctrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_ctrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ctbsv",11)==0) {
      cblas_rout = "cblas_ctbsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ctbsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ctbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ctbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ctpsv",11)==0) {
      cblas_rout = "cblas_ctpsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ctpsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ctpsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ctpsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ctpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ctpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ctpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ctpsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ctpsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ctpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ctpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ctpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_cgeru",10)==0) {
      cblas_rout = "cblas_cgeru";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_cgeru(INVALID, 0, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_cgeru(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_cgeru(CblasColMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_cgeru(CblasColMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_cgeru(CblasColMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_cgeru(CblasColMajor, 2, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_cgeru(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_cgeru(CblasRowMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_cgeru(CblasRowMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_cgeru(CblasRowMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_cgeru(CblasRowMajor, 0, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_cgerc",10)==0) {
      cblas_rout = "cblas_cgerc";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_cgerc(INVALID, 0, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_cgerc(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_cgerc(CblasColMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_cgerc(CblasColMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_cgerc(CblasColMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_cgerc(CblasColMajor, 2, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_cgerc(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_cgerc(CblasRowMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_cgerc(CblasRowMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_cgerc(CblasRowMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_cgerc(CblasRowMajor, 0, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_cher2",11)==0) {
      cblas_rout = "cblas_cher2";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_cher2(INVALID, CblasUpper, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_cher2(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_cher2(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_cher2(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_cher2(CblasColMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_cher2(CblasColMajor, CblasUpper, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_cher2(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_cher2(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_cher2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_cher2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_cher2(CblasRowMajor, CblasUpper, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_chpr2",11)==0) {
      cblas_rout = "cblas_chpr2";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_chpr2(INVALID, CblasUpper, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_chpr2(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_chpr2(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_chpr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_chpr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_chpr2(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_chpr2(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_chpr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_chpr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A );
      chkxer();
   } else if (strncmp( sf,"cblas_cher",10)==0) {
      cblas_rout = "cblas_cher";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_cher(INVALID, CblasUpper, 0, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_cher(CblasColMajor, INVALID, 0, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_cher(CblasColMajor, CblasUpper, INVALID, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_cher(CblasColMajor, CblasUpper, 0, RALPHA, X, 0, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_cher(CblasColMajor, CblasUpper, 2, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_cher(CblasRowMajor, INVALID, 0, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_cher(CblasRowMajor, CblasUpper, INVALID, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_cher(CblasRowMajor, CblasUpper, 0, RALPHA, X, 0, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_cher(CblasRowMajor, CblasUpper, 2, RALPHA, X, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_chpr",10)==0) {
      cblas_rout = "cblas_chpr";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_chpr(INVALID, CblasUpper, 0, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_chpr(CblasColMajor, INVALID, 0, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_chpr(CblasColMajor, CblasUpper, INVALID, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_chpr(CblasColMajor, CblasUpper, 0, RALPHA, X, 0, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_chpr(CblasColMajor, INVALID, 0, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_chpr(CblasColMajor, CblasUpper, INVALID, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_chpr(CblasColMajor, CblasUpper, 0, RALPHA, X, 0, A );
      chkxer();
   }
   if (cblas_ok == TRUE)
       printf(" %-12s PASSED THE TESTS OF ERROR-EXITS\n", cblas_rout);
   else
       printf("******* %s FAILED THE TESTS OF ERROR-EXITS *******\n",cblas_rout);
}
