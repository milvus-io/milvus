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

void F77_d3chke(char *rout) {
   char *sf = ( rout ) ;
   double A[2] = {0.0,0.0},
          B[2] = {0.0,0.0},
          C[2] = {0.0,0.0},
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

   if (strncmp( sf,"cblas_dgemm"   ,11)==0) {
      cblas_rout = "cblas_dgemm"   ;

      cblas_info = 1;
      cblas_dgemm( INVALID,  CblasNoTrans, CblasNoTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 1;
      cblas_dgemm( INVALID,  CblasNoTrans, CblasTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 1;
      cblas_dgemm( INVALID,  CblasTrans, CblasNoTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 1;
      cblas_dgemm( INVALID,  CblasTrans, CblasTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  INVALID, CblasNoTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  INVALID, CblasTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, INVALID, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, INVALID, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_dgemm( CblasColMajor,  CblasTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 9;  RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_dgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_dsymm"   ,11)==0) {
      cblas_rout = "cblas_dsymm"   ;

      cblas_info = 1;
      cblas_dsymm( INVALID,  CblasRight, CblasLower, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  INVALID, CblasUpper, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsymm( CblasColMajor,  CblasRight, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasUpper, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasLower, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasUpper, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasLeft, CblasLower, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsymm( CblasRowMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_dtrmm"   ,11)==0) {
      cblas_rout = "cblas_dtrmm"   ;

      cblas_info = 1;
      cblas_dtrmm( INVALID,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  INVALID, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, INVALID, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, INVALID,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   INVALID, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_dtrsm"   ,11)==0) {
      cblas_rout = "cblas_dtrsm"   ;

      cblas_info = 1;
      cblas_dtrsm( INVALID,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  INVALID, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, INVALID, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, INVALID,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   INVALID, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_dtrsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();

      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_dtrsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_dsyrk"   ,11)==0) {
      cblas_rout = "cblas_dsyrk"   ;

      cblas_info = 1;
      cblas_dsyrk( INVALID,  CblasUpper, CblasNoTrans,
                   0, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  INVALID, CblasNoTrans,
                   0, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, INVALID,
                   0, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasUpper, CblasNoTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasUpper, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasLower, CblasNoTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasLower, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasUpper, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasUpper, CblasTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasLower, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_dsyrk( CblasRowMajor,  CblasLower, CblasTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_dsyrk( CblasColMajor,  CblasLower, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_dsyr2k"   ,12)==0) {
      cblas_rout = "cblas_dsyr2k"   ;

      cblas_info = 1;
      cblas_dsyr2k( INVALID,  CblasUpper, CblasNoTrans,
                    0, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  INVALID, CblasNoTrans,
                    0, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, INVALID,
                    0, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasUpper, CblasNoTrans,
                    0, 2, ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasLower, CblasNoTrans,
                    0, 2, ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    0, 2, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    0, 2, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasUpper, CblasNoTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasLower, CblasNoTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_dsyr2k( CblasRowMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_dsyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
   }
   if (cblas_ok == TRUE )
       printf(" %-12s PASSED THE TESTS OF ERROR-EXITS\n", cblas_rout);
   else
       printf("***** %s FAILED THE TESTS OF ERROR-EXITS *******\n",cblas_rout);
}
