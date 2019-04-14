/*
 *     Written by D.P. Manley, Digital Equipment Corporation.
 *     Prefixed "C_" to BLAS routines and their declarations.
 *
 *     Modified by T. H. Do, 4/15/98, SGI/CRAY Research.
 */
#include <stdlib.h>
#include "common.h"
#include "cblas_test.h"

#define  TEST_COL_MJR	0
#define  TEST_ROW_MJR	1
#define  UNDEFINED     -1

void F77_cgemm(int *order, char *transpa, char *transpb, int *m, int *n,
     int *k, CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
     CBLAS_TEST_COMPLEX *b, int *ldb, CBLAS_TEST_COMPLEX *beta,
     CBLAS_TEST_COMPLEX *c, int *ldc ) {

  CBLAS_TEST_COMPLEX *A, *B, *C;
  int i,j,LDA, LDB, LDC;
  enum CBLAS_TRANSPOSE transa, transb;

  get_transpose_type(transpa, &transa);
  get_transpose_type(transpb, &transb);

  if (*order == TEST_ROW_MJR) {
     if (transa == CblasNoTrans) {
        LDA = *k+1;
        A=(CBLAS_TEST_COMPLEX*)malloc((*m)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*m; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else {
        LDA = *m+1;
        A=(CBLAS_TEST_COMPLEX* )malloc(LDA*(*k)*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*k; i++ )
           for( j=0; j<*m; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }

     if (transb == CblasNoTrans) {
        LDB = *n+1;
        B=(CBLAS_TEST_COMPLEX* )malloc((*k)*LDB*sizeof(CBLAS_TEST_COMPLEX) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ) {
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }
     else {
        LDB = *k+1;
        B=(CBLAS_TEST_COMPLEX* )malloc(LDB*(*n)*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }

     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDC*sizeof(CBLAS_TEST_COMPLEX));
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_cgemm( CblasRowMajor, transa, transb, *m, *n, *k, alpha, A, LDA,
                  B, LDB, beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(B);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_cgemm( CblasColMajor, transa, transb, *m, *n, *k, alpha, a, *lda,
                  b, *ldb, beta, c, *ldc );
  else
     cblas_cgemm( UNDEFINED, transa, transb, *m, *n, *k, alpha, a, *lda,
                  b, *ldb, beta, c, *ldc );
}

void F77_chemm(int *order, char *rtlf, char *uplow, int *m, int *n,
        CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
	CBLAS_TEST_COMPLEX *b, int *ldb, CBLAS_TEST_COMPLEX *beta,
        CBLAS_TEST_COMPLEX *c, int *ldc ) {

  CBLAS_TEST_COMPLEX *A, *B, *C;
  int i,j,LDA, LDB, LDC;
  enum CBLAS_UPLO uplo;
  enum CBLAS_SIDE side;

  get_uplo_type(uplow,&uplo);
  get_side_type(rtlf,&side);

  if (*order == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A= (CBLAS_TEST_COMPLEX* )malloc((*m)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else{
        LDA = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     LDB = *n+1;
     B=(CBLAS_TEST_COMPLEX* )malloc( (*m)*LDB*sizeof(CBLAS_TEST_COMPLEX ) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ) {
           B[i*LDB+j].real=b[j*(*ldb)+i].real;
           B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
        }
     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDC*sizeof(CBLAS_TEST_COMPLEX ) );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_chemm( CblasRowMajor, side, uplo, *m, *n, alpha, A, LDA, B, LDB,
                  beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(B);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_chemm( CblasColMajor, side, uplo, *m, *n, alpha, a, *lda, b, *ldb,
                  beta, c, *ldc );
  else
     cblas_chemm( UNDEFINED, side, uplo, *m, *n, alpha, a, *lda, b, *ldb,
                  beta, c, *ldc );
}
void F77_csymm(int *order, char *rtlf, char *uplow, int *m, int *n,
          CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
	  CBLAS_TEST_COMPLEX *b, int *ldb, CBLAS_TEST_COMPLEX *beta,
          CBLAS_TEST_COMPLEX *c, int *ldc ) {

  CBLAS_TEST_COMPLEX *A, *B, *C;
  int i,j,LDA, LDB, LDC;
  enum CBLAS_UPLO uplo;
  enum CBLAS_SIDE side;

  get_uplo_type(uplow,&uplo);
  get_side_type(rtlf,&side);

  if (*order == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     else{
        LDA = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ )
              A[i*LDA+j]=a[j*(*lda)+i];
     }
     LDB = *n+1;
     B=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDB*sizeof(CBLAS_TEST_COMPLEX ));
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ )
           B[i*LDB+j]=b[j*(*ldb)+i];
     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDC*sizeof(CBLAS_TEST_COMPLEX));
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           C[i*LDC+j]=c[j*(*ldc)+i];
     cblas_csymm( CblasRowMajor, side, uplo, *m, *n, alpha, A, LDA, B, LDB,
                  beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ )
           c[j*(*ldc)+i]=C[i*LDC+j];
     free(A);
     free(B);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_csymm( CblasColMajor, side, uplo, *m, *n, alpha, a, *lda, b, *ldb,
                  beta, c, *ldc );
  else
     cblas_csymm( UNDEFINED, side, uplo, *m, *n, alpha, a, *lda, b, *ldb,
                  beta, c, *ldc );
}

void F77_cherk(int *order, char *uplow, char *transp, int *n, int *k,
     float *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
     float *beta, CBLAS_TEST_COMPLEX *c, int *ldc ) {

  int i,j,LDA,LDC;
  CBLAS_TEST_COMPLEX *A, *C;
  enum CBLAS_UPLO uplo;
  enum CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);

  if (*order == TEST_ROW_MJR) {
     if (trans == CblasNoTrans) {
        LDA = *k+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else{
        LDA = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*k)*LDA*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDC*sizeof(CBLAS_TEST_COMPLEX ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_cherk(CblasRowMajor, uplo, trans, *n, *k, *alpha, A, LDA, *beta,
	         C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*n; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_cherk(CblasColMajor, uplo, trans, *n, *k, *alpha, a, *lda, *beta,
	         c, *ldc );
  else
     cblas_cherk(UNDEFINED, uplo, trans, *n, *k, *alpha, a, *lda, *beta,
	         c, *ldc );
}

void F77_csyrk(int *order, char *uplow, char *transp, int *n, int *k,
     CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
     CBLAS_TEST_COMPLEX *beta, CBLAS_TEST_COMPLEX *c, int *ldc ) {

  int i,j,LDA,LDC;
  CBLAS_TEST_COMPLEX *A, *C;
  enum CBLAS_UPLO uplo;
  enum CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);

  if (*order == TEST_ROW_MJR) {
     if (trans == CblasNoTrans) {
        LDA = *k+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else{
        LDA = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*k)*LDA*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDC*sizeof(CBLAS_TEST_COMPLEX ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_csyrk(CblasRowMajor, uplo, trans, *n, *k, alpha, A, LDA, beta,
	         C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*n; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_csyrk(CblasColMajor, uplo, trans, *n, *k, alpha, a, *lda, beta,
	         c, *ldc );
  else
     cblas_csyrk(UNDEFINED, uplo, trans, *n, *k, alpha, a, *lda, beta,
	         c, *ldc );
}
void F77_cher2k(int *order, char *uplow, char *transp, int *n, int *k,
        CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
	CBLAS_TEST_COMPLEX *b, int *ldb, float *beta,
        CBLAS_TEST_COMPLEX *c, int *ldc ) {
  int i,j,LDA,LDB,LDC;
  CBLAS_TEST_COMPLEX *A, *B, *C;
  enum CBLAS_UPLO uplo;
  enum CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);

  if (*order == TEST_ROW_MJR) {
     if (trans == CblasNoTrans) {
        LDA = *k+1;
        LDB = *k+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX ));
        B=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDB*sizeof(CBLAS_TEST_COMPLEX ));
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }
     else {
        LDA = *n+1;
        LDB = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc( LDA*(*k)*sizeof(CBLAS_TEST_COMPLEX ) );
        B=(CBLAS_TEST_COMPLEX* )malloc( LDB*(*k)*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ){
	      A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }
     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc( (*n)*LDC*sizeof(CBLAS_TEST_COMPLEX ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_cher2k(CblasRowMajor, uplo, trans, *n, *k, alpha, A, LDA,
		  B, LDB, *beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*n; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(B);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_cher2k(CblasColMajor, uplo, trans, *n, *k, alpha, a, *lda,
		   b, *ldb, *beta, c, *ldc );
  else
     cblas_cher2k(UNDEFINED, uplo, trans, *n, *k, alpha, a, *lda,
		   b, *ldb, *beta, c, *ldc );
}
void F77_csyr2k(int *order, char *uplow, char *transp, int *n, int *k,
         CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
	 CBLAS_TEST_COMPLEX *b, int *ldb, CBLAS_TEST_COMPLEX *beta,
         CBLAS_TEST_COMPLEX *c, int *ldc ) {
  int i,j,LDA,LDB,LDC;
  CBLAS_TEST_COMPLEX *A, *B, *C;
  enum CBLAS_UPLO uplo;
  enum CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);

  if (*order == TEST_ROW_MJR) {
     if (trans == CblasNoTrans) {
        LDA = *k+1;
        LDB = *k+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        B=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDB*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }
     else {
        LDA = *n+1;
        LDB = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc(LDA*(*k)*sizeof(CBLAS_TEST_COMPLEX));
        B=(CBLAS_TEST_COMPLEX* )malloc(LDB*(*k)*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ){
	      A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }
     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc( (*n)*LDC*sizeof(CBLAS_TEST_COMPLEX));
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_csyr2k(CblasRowMajor, uplo, trans, *n, *k, alpha, A, LDA,
		  B, LDB, beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*n; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(B);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_csyr2k(CblasColMajor, uplo, trans, *n, *k, alpha, a, *lda,
		   b, *ldb, beta, c, *ldc );
  else
     cblas_csyr2k(UNDEFINED, uplo, trans, *n, *k, alpha, a, *lda,
		   b, *ldb, beta, c, *ldc );
}
void F77_ctrmm(int *order, char *rtlf, char *uplow, char *transp, char *diagn,
       int *m, int *n, CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a,
       int *lda, CBLAS_TEST_COMPLEX *b, int *ldb) {
  int i,j,LDA,LDB;
  CBLAS_TEST_COMPLEX *A, *B;
  enum CBLAS_SIDE side;
  enum CBLAS_DIAG diag;
  enum CBLAS_UPLO uplo;
  enum CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);
  get_diag_type(diagn,&diag);
  get_side_type(rtlf,&side);

  if (*order == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else{
        LDA = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     LDB = *n+1;
     B=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDB*sizeof(CBLAS_TEST_COMPLEX));
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ) {
           B[i*LDB+j].real=b[j*(*ldb)+i].real;
           B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
        }
     cblas_ctrmm(CblasRowMajor, side, uplo, trans, diag, *m, *n, alpha,
		 A, LDA, B, LDB );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           b[j*(*ldb)+i].real=B[i*LDB+j].real;
           b[j*(*ldb)+i].imag=B[i*LDB+j].imag;
        }
     free(A);
     free(B);
  }
  else if (*order == TEST_COL_MJR)
     cblas_ctrmm(CblasColMajor, side, uplo, trans, diag, *m, *n, alpha,
		   a, *lda, b, *ldb);
  else
     cblas_ctrmm(UNDEFINED, side, uplo, trans, diag, *m, *n, alpha,
		   a, *lda, b, *ldb);
}

void F77_ctrsm(int *order, char *rtlf, char *uplow, char *transp, char *diagn,
         int *m, int *n, CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a,
         int *lda, CBLAS_TEST_COMPLEX *b, int *ldb) {
  int i,j,LDA,LDB;
  CBLAS_TEST_COMPLEX *A, *B;
  enum CBLAS_SIDE side;
  enum CBLAS_DIAG diag;
  enum CBLAS_UPLO uplo;
  enum CBLAS_TRANSPOSE trans;

  get_uplo_type(uplow,&uplo);
  get_transpose_type(transp,&trans);
  get_diag_type(diagn,&diag);
  get_side_type(rtlf,&side);

  if (*order == TEST_ROW_MJR) {
     if (side == CblasLeft) {
        LDA = *m+1;
        A=(CBLAS_TEST_COMPLEX* )malloc( (*m)*LDA*sizeof(CBLAS_TEST_COMPLEX ) );
        for( i=0; i<*m; i++ )
           for( j=0; j<*m; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else{
        LDA = *n+1;
        A=(CBLAS_TEST_COMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*n; i++ )
           for( j=0; j<*n; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     LDB = *n+1;
     B=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDB*sizeof(CBLAS_TEST_COMPLEX));
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ) {
           B[i*LDB+j].real=b[j*(*ldb)+i].real;
           B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
        }
     cblas_ctrsm(CblasRowMajor, side, uplo, trans, diag, *m, *n, alpha,
		 A, LDA, B, LDB );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           b[j*(*ldb)+i].real=B[i*LDB+j].real;
           b[j*(*ldb)+i].imag=B[i*LDB+j].imag;
        }
     free(A);
     free(B);
  }
  else if (*order == TEST_COL_MJR)
     cblas_ctrsm(CblasColMajor, side, uplo, trans, diag, *m, *n, alpha,
		   a, *lda, b, *ldb);
  else
     cblas_ctrsm(UNDEFINED, side, uplo, trans, diag, *m, *n, alpha,
		   a, *lda, b, *ldb);
}



void F77_cgemm3m(int *order, char *transpa, char *transpb, int *m, int *n,
     int *k, CBLAS_TEST_COMPLEX *alpha, CBLAS_TEST_COMPLEX *a, int *lda,
     CBLAS_TEST_COMPLEX *b, int *ldb, CBLAS_TEST_COMPLEX *beta,
     CBLAS_TEST_COMPLEX *c, int *ldc ) {

  CBLAS_TEST_COMPLEX *A, *B, *C;
  int i,j,LDA, LDB, LDC;
  enum CBLAS_TRANSPOSE transa, transb;

  get_transpose_type(transpa, &transa);
  get_transpose_type(transpb, &transb);

  if (*order == TEST_ROW_MJR) {
     if (transa == CblasNoTrans) {
        LDA = *k+1;
        A=(CBLAS_TEST_COMPLEX*)malloc((*m)*LDA*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*m; i++ )
           for( j=0; j<*k; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }
     else {
        LDA = *m+1;
        A=(CBLAS_TEST_COMPLEX* )malloc(LDA*(*k)*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*k; i++ )
           for( j=0; j<*m; j++ ) {
              A[i*LDA+j].real=a[j*(*lda)+i].real;
              A[i*LDA+j].imag=a[j*(*lda)+i].imag;
           }
     }

     if (transb == CblasNoTrans) {
        LDB = *n+1;
        B=(CBLAS_TEST_COMPLEX* )malloc((*k)*LDB*sizeof(CBLAS_TEST_COMPLEX) );
        for( i=0; i<*k; i++ )
           for( j=0; j<*n; j++ ) {
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }
     else {
        LDB = *k+1;
        B=(CBLAS_TEST_COMPLEX* )malloc(LDB*(*n)*sizeof(CBLAS_TEST_COMPLEX));
        for( i=0; i<*n; i++ )
           for( j=0; j<*k; j++ ) {
              B[i*LDB+j].real=b[j*(*ldb)+i].real;
              B[i*LDB+j].imag=b[j*(*ldb)+i].imag;
           }
     }

     LDC = *n+1;
     C=(CBLAS_TEST_COMPLEX* )malloc((*m)*LDC*sizeof(CBLAS_TEST_COMPLEX));
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           C[i*LDC+j].real=c[j*(*ldc)+i].real;
           C[i*LDC+j].imag=c[j*(*ldc)+i].imag;
        }
     cblas_cgemm3m( CblasRowMajor, transa, transb, *m, *n, *k, alpha, A, LDA,
                  B, LDB, beta, C, LDC );
     for( j=0; j<*n; j++ )
        for( i=0; i<*m; i++ ) {
           c[j*(*ldc)+i].real=C[i*LDC+j].real;
           c[j*(*ldc)+i].imag=C[i*LDC+j].imag;
        }
     free(A);
     free(B);
     free(C);
  }
  else if (*order == TEST_COL_MJR)
     cblas_cgemm3m( CblasColMajor, transa, transb, *m, *n, *k, alpha, a, *lda,
                  b, *ldb, beta, c, *ldc );
  else
     cblas_cgemm3m( UNDEFINED, transa, transb, *m, *n, *k, alpha, a, *lda,
                  b, *ldb, beta, c, *ldc );
}


