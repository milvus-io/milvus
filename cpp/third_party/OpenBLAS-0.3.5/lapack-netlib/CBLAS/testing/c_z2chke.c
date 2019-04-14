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

void F77_z2chke(char *rout) {
   char *sf = ( rout ) ;
   double  A[2] = {0.0,0.0},
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

   if (strncmp( sf,"cblas_zgemv",11)==0) {
      cblas_rout = "cblas_zgemv";
      cblas_info = 1;
      cblas_zgemv(INVALID, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zgemv(CblasColMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zgemv(CblasColMajor, CblasNoTrans, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_zgemv(CblasColMajor, CblasNoTrans, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_zgemv(CblasColMajor, CblasNoTrans, 2, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_zgemv(CblasColMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_zgemv(CblasColMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();

      cblas_info = 2; RowMajorStrg = TRUE; RowMajorStrg = TRUE;
      cblas_zgemv(CblasRowMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zgemv(CblasRowMajor, CblasNoTrans, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_zgemv(CblasRowMajor, CblasNoTrans, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_zgemv(CblasRowMajor, CblasNoTrans, 0, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_zgemv(CblasRowMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_zgemv(CblasRowMajor, CblasNoTrans, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_zgbmv",11)==0) {
      cblas_rout = "cblas_zgbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zgbmv(INVALID, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, INVALID, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, INVALID, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, 0, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, 0, 0, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, 2, 0, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, 0, 0, 1, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_zgbmv(CblasColMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, INVALID, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, INVALID, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, 0, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, 0, 0, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, 2, 0, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 1, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_zgbmv(CblasRowMajor, CblasNoTrans, 0, 0, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_zhemv",11)==0) {
      cblas_rout = "cblas_zhemv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zhemv(INVALID, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zhemv(CblasColMajor, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zhemv(CblasColMajor, CblasUpper, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zhemv(CblasColMajor, CblasUpper, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_zhemv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_zhemv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zhemv(CblasRowMajor, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zhemv(CblasRowMajor, CblasUpper, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zhemv(CblasRowMajor, CblasUpper, 2,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_zhemv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_zhemv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_zhbmv",11)==0) {
      cblas_rout = "cblas_zhbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zhbmv(INVALID, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zhbmv(CblasColMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zhbmv(CblasColMajor, CblasUpper, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_zhbmv(CblasColMajor, CblasUpper, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_zhbmv(CblasColMajor, CblasUpper, 0, 1,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_zhbmv(CblasColMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_zhbmv(CblasColMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zhbmv(CblasRowMajor, INVALID, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zhbmv(CblasRowMajor, CblasUpper, INVALID, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_zhbmv(CblasRowMajor, CblasUpper, 0, INVALID,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_zhbmv(CblasRowMajor, CblasUpper, 0, 1,
                  ALPHA, A, 1, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_zhbmv(CblasRowMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_zhbmv(CblasRowMajor, CblasUpper, 0, 0,
                  ALPHA, A, 1, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_zhpmv",11)==0) {
      cblas_rout = "cblas_zhpmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zhpmv(INVALID, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zhpmv(CblasColMajor, INVALID, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zhpmv(CblasColMajor, CblasUpper, INVALID,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_zhpmv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_zhpmv(CblasColMajor, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zhpmv(CblasRowMajor, INVALID, 0,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zhpmv(CblasRowMajor, CblasUpper, INVALID,
                  ALPHA, A, X, 1, BETA, Y, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_zhpmv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, X, 0, BETA, Y, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_zhpmv(CblasRowMajor, CblasUpper, 0,
                  ALPHA, A, X, 1, BETA, Y, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ztrmv",11)==0) {
      cblas_rout = "cblas_ztrmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ztrmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ztrmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ztrmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ztrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ztrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_ztrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_ztrmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ztrmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ztrmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ztrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ztrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_ztrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_ztrmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ztbmv",11)==0) {
      cblas_rout = "cblas_ztbmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ztbmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ztbmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ztbmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ztpmv",11)==0) {
      cblas_rout = "cblas_ztpmv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ztpmv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ztpmv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ztpmv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ztpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ztpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ztpmv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ztpmv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ztpmv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ztpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ztpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ztpmv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ztrsv",11)==0) {
      cblas_rout = "cblas_ztrsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ztrsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ztrsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ztrsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ztrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ztrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_ztrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_ztrsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ztrsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ztrsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ztrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ztrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_ztrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 2, A, 1, X, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_ztrsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ztbsv",11)==0) {
      cblas_rout = "cblas_ztbsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ztbsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ztbsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, 0, A, 1, X, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, INVALID, A, 1, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 1, A, 1, X, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ztbsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, 0, A, 1, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_ztpsv",11)==0) {
      cblas_rout = "cblas_ztpsv";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_ztpsv(INVALID, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ztpsv(CblasColMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ztpsv(CblasColMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ztpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ztpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ztpsv(CblasColMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_ztpsv(CblasRowMajor, INVALID, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_ztpsv(CblasRowMajor, CblasUpper, INVALID,
                  CblasNonUnit, 0, A, X, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ztpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  INVALID, 0, A, X, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ztpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, INVALID, A, X, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ztpsv(CblasRowMajor, CblasUpper, CblasNoTrans,
                  CblasNonUnit, 0, A, X, 0 );
      chkxer();
   } else if (strncmp( sf,"cblas_zgeru",10)==0) {
      cblas_rout = "cblas_zgeru";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zgeru(INVALID, 0, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zgeru(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zgeru(CblasColMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zgeru(CblasColMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_zgeru(CblasColMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_zgeru(CblasColMajor, 2, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zgeru(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zgeru(CblasRowMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zgeru(CblasRowMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_zgeru(CblasRowMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_zgeru(CblasRowMajor, 0, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_zgerc",10)==0) {
      cblas_rout = "cblas_zgerc";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zgerc(INVALID, 0, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zgerc(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zgerc(CblasColMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zgerc(CblasColMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_zgerc(CblasColMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_zgerc(CblasColMajor, 2, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zgerc(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zgerc(CblasRowMajor, 0, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zgerc(CblasRowMajor, 0, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_zgerc(CblasRowMajor, 0, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_zgerc(CblasRowMajor, 0, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_zher2",11)==0) {
      cblas_rout = "cblas_zher2";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zher2(INVALID, CblasUpper, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zher2(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zher2(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zher2(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_zher2(CblasColMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_zher2(CblasColMajor, CblasUpper, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zher2(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zher2(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zher2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_zher2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_zher2(CblasRowMajor, CblasUpper, 2, ALPHA, X, 1, Y, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_zhpr2",11)==0) {
      cblas_rout = "cblas_zhpr2";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zhpr2(INVALID, CblasUpper, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zhpr2(CblasColMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zhpr2(CblasColMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zhpr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_zhpr2(CblasColMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zhpr2(CblasRowMajor, INVALID, 0, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zhpr2(CblasRowMajor, CblasUpper, INVALID, ALPHA, X, 1, Y, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zhpr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 0, Y, 1, A );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_zhpr2(CblasRowMajor, CblasUpper, 0, ALPHA, X, 1, Y, 0, A );
      chkxer();
   } else if (strncmp( sf,"cblas_zher",10)==0) {
      cblas_rout = "cblas_zher";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zher(INVALID, CblasUpper, 0, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zher(CblasColMajor, INVALID, 0, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zher(CblasColMajor, CblasUpper, INVALID, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zher(CblasColMajor, CblasUpper, 0, RALPHA, X, 0, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_zher(CblasColMajor, CblasUpper, 2, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = TRUE;
      cblas_zher(CblasRowMajor, INVALID, 0, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = TRUE;
      cblas_zher(CblasRowMajor, CblasUpper, INVALID, RALPHA, X, 1, A, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_zher(CblasRowMajor, CblasUpper, 0, RALPHA, X, 0, A, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_zher(CblasRowMajor, CblasUpper, 2, RALPHA, X, 1, A, 1 );
      chkxer();
   } else if (strncmp( sf,"cblas_zhpr",10)==0) {
      cblas_rout = "cblas_zhpr";
      cblas_info = 1; RowMajorStrg = FALSE;
      cblas_zhpr(INVALID, CblasUpper, 0, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zhpr(CblasColMajor, INVALID, 0, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zhpr(CblasColMajor, CblasUpper, INVALID, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zhpr(CblasColMajor, CblasUpper, 0, RALPHA, X, 0, A );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_zhpr(CblasColMajor, INVALID, 0, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_zhpr(CblasColMajor, CblasUpper, INVALID, RALPHA, X, 1, A );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_zhpr(CblasColMajor, CblasUpper, 0, RALPHA, X, 0, A );
      chkxer();
   }
   if (cblas_ok == TRUE)
       printf(" %-12s PASSED THE TESTS OF ERROR-EXITS\n", cblas_rout);
   else
       printf("******* %s FAILED THE TESTS OF ERROR-EXITS *******\n",cblas_rout);
}
