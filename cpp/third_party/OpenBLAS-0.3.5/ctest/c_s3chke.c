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

void F77_s3chke(char *rout) {
   char *sf = ( rout ) ;
   float  A[2] = {0.0,0.0},
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

   if (strncmp( sf,"cblas_sgemm"   ,11)==0) {
      cblas_rout = "cblas_sgemm"   ;
      cblas_info = 1;
      cblas_sgemm( INVALID,  CblasNoTrans, CblasNoTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 1;
      cblas_sgemm( INVALID,  CblasNoTrans, CblasTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 1;
      cblas_sgemm( INVALID,  CblasTrans, CblasNoTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 1;
      cblas_sgemm( INVALID,  CblasTrans, CblasTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  INVALID, CblasNoTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  INVALID, CblasTrans, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, INVALID, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, INVALID, 0, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasNoTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = FALSE;
      cblas_sgemm( CblasColMajor,  CblasTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();

      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 9;  RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 9; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasTrans, 2, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 14; RowMajorStrg = TRUE;
      cblas_sgemm( CblasRowMajor,  CblasTrans, CblasTrans, 0, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_ssymm"   ,11)==0) {
      cblas_rout = "cblas_ssymm"   ;

      cblas_info = 1;
      cblas_ssymm( INVALID,  CblasRight, CblasLower, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  INVALID, CblasUpper, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssymm( CblasColMajor,  CblasRight, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();

      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasUpper, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasLower, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasUpper, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasLower, 0, INVALID,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasUpper, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasLower, 2, 0,
                   ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasUpper, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasLower, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasUpper, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasUpper, 0, 2,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasLeft, CblasLower, 0, 2,
                   ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssymm( CblasRowMajor,  CblasRight, CblasLower, 0, 2,
                   ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_strmm"   ,11)==0) {
      cblas_rout = "cblas_strmm"   ;

      cblas_info = 1;
      cblas_strmm( INVALID,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  INVALID, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, INVALID, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, INVALID,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   INVALID, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strmm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();

      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strmm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_strsm"   ,11)==0) {
      cblas_rout = "cblas_strsm"   ;

      cblas_info = 1;
      cblas_strsm( INVALID,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  INVALID, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, INVALID, CblasNoTrans,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, INVALID,
                   CblasNonUnit, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   INVALID, 0, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = FALSE;
      cblas_strsm( CblasColMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();

      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 6; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, INVALID, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 7; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, INVALID, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 2, 0, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 2 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasUpper, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasLeft, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 1, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasNoTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();
      cblas_info = 12; RowMajorStrg = TRUE;
      cblas_strsm( CblasRowMajor,  CblasRight, CblasLower, CblasTrans,
                   CblasNonUnit, 0, 2, ALPHA, A, 2, B, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_ssyrk"   ,11)==0) {
      cblas_rout = "cblas_ssyrk"   ;

      cblas_info = 1;
      cblas_ssyrk( INVALID,  CblasUpper, CblasNoTrans,
                   0, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  INVALID, CblasNoTrans,
                   0, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, INVALID,
                   0, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasTrans,
                   INVALID, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasTrans,
                   0, INVALID, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasUpper, CblasNoTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasUpper, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasLower, CblasNoTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasLower, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasTrans,
                   0, 2, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasUpper, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasUpper, CblasTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasLower, CblasNoTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = TRUE;
      cblas_ssyrk( CblasRowMajor,  CblasLower, CblasTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasNoTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasUpper, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasNoTrans,
                   2, 0, ALPHA, A, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 11; RowMajorStrg = FALSE;
      cblas_ssyrk( CblasColMajor,  CblasLower, CblasTrans,
                   2, 0, ALPHA, A, 1, BETA, C, 1 );
      chkxer();

   } else if (strncmp( sf,"cblas_ssyr2k"   ,12)==0) {
      cblas_rout = "cblas_ssyr2k"   ;

      cblas_info = 1;
      cblas_ssyr2k( INVALID,  CblasUpper, CblasNoTrans,
                    0, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 2; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  INVALID, CblasNoTrans,
                    0, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 3; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, INVALID,
                    0, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 4; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    INVALID, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 5; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    0, INVALID, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasUpper, CblasNoTrans,
                    0, 2, ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasLower, CblasNoTrans,
                    0, 2, ALPHA, A, 1, B, 2, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    0, 2, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 8; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    0, 2, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasUpper, CblasNoTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasLower, CblasNoTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 1, BETA, C, 2 );
      chkxer();
      cblas_info = 10; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    0, 2, ALPHA, A, 2, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = TRUE;
      cblas_ssyr2k( CblasRowMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasUpper, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasNoTrans,
                    2, 0, ALPHA, A, 2, B, 2, BETA, C, 1 );
      chkxer();
      cblas_info = 13; RowMajorStrg = FALSE;
      cblas_ssyr2k( CblasColMajor,  CblasLower, CblasTrans,
                    2, 0, ALPHA, A, 1, B, 1, BETA, C, 1 );
      chkxer();
   }
   if (cblas_ok == TRUE )
       printf(" %-12s PASSED THE TESTS OF ERROR-EXITS\n", cblas_rout);
   else
       printf("***** %s FAILED THE TESTS OF ERROR-EXITS *******\n",cblas_rout);
}
