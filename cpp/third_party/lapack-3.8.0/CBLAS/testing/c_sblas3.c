/*
 *     Written by D.P. Manley, Digital Equipment Corporation.
 *     Prefixed "C_" to BLAS routines and their declarations.
 *
 *     Modified by T. H. Do, 2/19/98, SGI/CRAY Research.
 */
#include <stdio.h>
#include <stdlib.h>
#include "cblas.h"
#include "cblas_test.h"

void F77_sgemm(int *layout, char *transpa, char *transpb, int *m, int *n,
              int *k, float *alpha, float *a, int *lda, float *b, int *ldb,
              float *beta, float *c, int *ldc ) {

  float *A, *B, *C;
  int i,j,LDA, LDB, LDC;
  CBLAS_TRANSPOSE transa, transb;

  get_transpose_type(transpa, &transa);
  get_transpose_type(transpb, &transb);

  if (*layout == TEST_ROW_MJR) {
     if (transa == CblasNoTrans) {
        LDA = *k+1;
        A = (float *)malloc( (*m)*LDA*sizeof( float ) );
        for( i=0; i<*m; i++ )
           for( j=0; j<*k; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     else {
        LDA = *m+1;
        A   = ( float* )malloc( LDA*(*k)*sizeof( float ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*m; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     if (transb == CblasNoTrans) {
        LDB = *n+1;
        B   = ( float* )malloc( (*k)*LDB*sizeof( float ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ )
              B[i*LDB+j]=b[j*(*ldb)+i];
     }
     else {
        LDB = *k+1;
        B   = ( float* )malloc( LDB*(*n)*sizeof( float ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ )
              B[i*LDB+j]=b[j*(*ldb)+i];
     }
     LDC = *n+1;
     C   = ( float* )malloc( (*m)*LDC*sizeof( float ) );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           C[i*LDC+j]=c[j*(*ldc)+i];
     cblas_sgemm( CblasRowMajor, transa, transb, *m, *n, *k, *alpha, A, LDA,
                  B, LDB, *beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           c[j*(*ldc)+i]=C[i*LDC+j];
     free(A);
     free(B);
     free(C);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_sgemm( CblasColMajor, transa, transb, *m, *n, *k, *alpha, a, *lda,
                  b, *ldb, *beta, c, *ldc );
  else
     cblas_sgemm( UNDEFINED, transa, transb, *m, *n, *k, *alpha, a, *lda,
                  b, *ldb, *beta, c, *ldc );
}
void F77_ssymm(int *layout, char *rtlf, char *uplow, int *m, int *n,
              float *alpha, float *a, int *lda, float *b, int *ldb,
              float *beta, float *c, int *ldc ) {

  float *A, *B, *C;
  int i,j,LDA, LDB, LDC;
  CBLAS_UPLO uplo;
  CBLAS_SIDE side;

  get_uplo_type(uplow,&uplo);
  get_side_type(rtlf,&side);

  if (*layout == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A   = ( float* )malloc( (*m)*LDA*sizeof( float ) );
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     else{
        LDA = *n+1;
        A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     LDB = *n+1;
     B   = ( float* )malloc( (*m)*LDB*sizeof( float ) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ )
           B[i*LDB+j]=b[j*(*ldb)+i];
     LDC = *n+1;
     C   = ( float* )malloc( (*m)*LDC*sizeof( float ) );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           C[i*LDC+j]=c[j*(*ldc)+i];
     cblas_ssymm( CblasRowMajor, side, uplo, *m, *n, *alpha, A, LDA, B, LDB,
                  *beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           c[j*(*ldc)+i]=C[i*LDC+j];
     free(A);
     free(B);
     free(C);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ssymm( CblasColMajor, side, uplo, *m, *n, *alpha, a, *lda, b, *ldb,
                  *beta, c, *ldc );
  else
     cblas_ssymm( UNDEFINED, side, uplo, *m, *n, *alpha, a, *lda, b, *ldb,
                  *beta, c, *ldc );
}

void F77_ssyrk(int *layout, char *uplow, char *transp, int *n, int *k,
              float *alpha, float *a, int *lda,
              float *beta, float *c, int *ldc ) {

  int i,j,LDA,LDC;
  float *A, *C;
  CBLAS_UPLO uplo;
  CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);

  if (*layout == TEST_ROW_MJR) {
     if (trans == CblasNoTrans) {
        LDA = *k+1;
        A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     else{
        LDA = *n+1;
        A   = ( float* )malloc( (*k)*LDA*sizeof( float ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     LDC = *n+1;
     C   = ( float* )malloc( (*n)*LDC*sizeof( float ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ )
           C[i*LDC+j]=c[j*(*ldc)+i];
     cblas_ssyrk(CblasRowMajor, uplo, trans, *n, *k, *alpha, A, LDA, *beta,
	         C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*n; i++ )
           c[j*(*ldc)+i]=C[i*LDC+j];
     free(A);
     free(C);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ssyrk(CblasColMajor, uplo, trans, *n, *k, *alpha, a, *lda, *beta,
	         c, *ldc );
  else
     cblas_ssyrk(UNDEFINED, uplo, trans, *n, *k, *alpha, a, *lda, *beta,
	         c, *ldc );
}

void F77_ssyr2k(int *layout, char *uplow, char *transp, int *n, int *k,
               float *alpha, float *a, int *lda, float *b, int *ldb,
               float *beta, float *c, int *ldc ) {
  int i,j,LDA,LDB,LDC;
  float *A, *B, *C;
  CBLAS_UPLO uplo;
  CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);

  if (*layout == TEST_ROW_MJR) {
     if (trans == CblasNoTrans) {
        LDA = *k+1;
        LDB = *k+1;
        A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
        B   = ( float* )malloc( (*n)*LDB*sizeof( float ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j]=a[j*(*lda)+i];
              B[i*LDB+j]=b[j*(*ldb)+i];
           }
     }
     else {
        LDA = *n+1;
        LDB = *n+1;
        A   = ( float* )malloc( LDA*(*k)*sizeof( float ) );
        B   = ( float* )malloc( LDB*(*k)*sizeof( float ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ){
              A[i*LDA+j]=a[j*(*lda)+i];
              B[i*LDB+j]=b[j*(*ldb)+i];
           }
     }
     LDC = *n+1;
     C   = ( float* )malloc( (*n)*LDC*sizeof( float ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ )
           C[i*LDC+j]=c[j*(*ldc)+i];
     cblas_ssyr2k(CblasRowMajor, uplo, trans, *n, *k, *alpha, A, LDA,
		  B, LDB, *beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*n; i++ )
           c[j*(*ldc)+i]=C[i*LDC+j];
     free(A);
     free(B);
     free(C);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ssyr2k(CblasColMajor, uplo, trans, *n, *k, *alpha, a, *lda,
		   b, *ldb, *beta, c, *ldc );
  else
     cblas_ssyr2k(UNDEFINED, uplo, trans, *n, *k, *alpha, a, *lda,
		   b, *ldb, *beta, c, *ldc );
}
void F77_strmm(int *layout, char *rtlf, char *uplow, char *transp, char *diagn,
              int *m, int *n, float *alpha, float *a, int *lda, float *b,
              int *ldb) {
  int i,j,LDA,LDB;
  float *A, *B;
  CBLAS_SIDE side;
  CBLAS_DIAG diag;
  CBLAS_UPLO uplo;
  CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);
  get_diag_type(diagn,&diag);
  get_side_type(rtlf,&side);

  if (*layout == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A   = ( float* )malloc( (*m)*LDA*sizeof( float ) );
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     else{
        LDA = *n+1;
        A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     LDB = *n+1;
     B   = ( float* )malloc( (*m)*LDB*sizeof( float ) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ )
           B[i*LDB+j]=b[j*(*ldb)+i];
     cblas_strmm(CblasRowMajor, side, uplo, trans, diag, *m, *n, *alpha,
		 A, LDA, B, LDB );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           b[j*(*ldb)+i]=B[i*LDB+j];
     free(A);
     free(B);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_strmm(CblasColMajor, side, uplo, trans, diag, *m, *n, *alpha,
		   a, *lda, b, *ldb);
  else
     cblas_strmm(UNDEFINED, side, uplo, trans, diag, *m, *n, *alpha,
		   a, *lda, b, *ldb);
}

void F77_strsm(int *layout, char *rtlf, char *uplow, char *transp, char *diagn,
              int *m, int *n, float *alpha, float *a, int *lda, float *b,
              int *ldb) {
  int i,j,LDA,LDB;
  float *A, *B;
  CBLAS_SIDE side;
  CBLAS_DIAG diag;
  CBLAS_UPLO uplo;
  CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);
  get_diag_type(diagn,&diag);
  get_side_type(rtlf,&side);

  if (*layout == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A   = ( float* )malloc( (*m)*LDA*sizeof( float ) );
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     else{
        LDA = *n+1;
        A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     LDB = *n+1;
     B   = ( float* )malloc( (*m)*LDB*sizeof( float ) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ )
           B[i*LDB+j]=b[j*(*ldb)+i];
     cblas_strsm(CblasRowMajor, side, uplo, trans, diag, *m, *n, *alpha,
		 A, LDA, B, LDB );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           b[j*(*ldb)+i]=B[i*LDB+j];
     free(A);
     free(B);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_strsm(CblasColMajor, side, uplo, trans, diag, *m, *n, *alpha,
		   a, *lda, b, *ldb);
  else
     cblas_strsm(UNDEFINED, side, uplo, trans, diag, *m, *n, *alpha,
		   a, *lda, b, *ldb);
}
