/*
 *     Written by D.P. Manley, Digital Equipment Corporation.
 *     Prefixed "C_" to BLAS routines and their declarations.
 *
 *     Modified by T. H. Do, 4/08/98, SGI/CRAY Research.
 */
#include <stdlib.h>
#include "cblas.h"
#include "cblas_test.h"

void F77_zgemv(int *layout, char *transp, int *m, int *n,
          const void *alpha,
          CBLAS_TEST_ZOMPLEX *a, int *lda, const void *x, int *incx,
          const void *beta, void *y, int *incy) {

  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;
  CBLAS_TRANSPOSE trans;

  get_transpose_type(transp, &trans);
  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A  = (CBLAS_TEST_ZOMPLEX *)malloc( (*m)*LDA*sizeof( CBLAS_TEST_ZOMPLEX) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ){
           A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
           A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
        }
     cblas_zgemv( CblasRowMajor, trans, *m, *n, alpha, A, LDA, x, *incx,
	    beta, y, *incy );
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zgemv( CblasColMajor, trans,
                  *m, *n, alpha, a, *lda, x, *incx, beta, y, *incy );
  else
     cblas_zgemv( UNDEFINED, trans,
                  *m, *n, alpha, a, *lda, x, *incx, beta, y, *incy );
}

void F77_zgbmv(int *layout, char *transp, int *m, int *n, int *kl, int *ku,
	      CBLAS_TEST_ZOMPLEX *alpha, CBLAS_TEST_ZOMPLEX *a, int *lda,
	      CBLAS_TEST_ZOMPLEX *x, int *incx,
	      CBLAS_TEST_ZOMPLEX *beta, CBLAS_TEST_ZOMPLEX *y, int *incy) {

  CBLAS_TEST_ZOMPLEX *A;
  int i,j,irow,jcol,LDA;
  CBLAS_TRANSPOSE trans;

  get_transpose_type(transp, &trans);
  if (*layout == TEST_ROW_MJR) {
     LDA = *ku+*kl+2;
     A=( CBLAS_TEST_ZOMPLEX* )malloc((*n+*kl)*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
     for( i=0; i<*ku; i++ ){
        irow=*ku+*kl-i;
        jcol=(*ku)-i;
        for( j=jcol; j<*n; j++ ){
           A[ LDA*(j-jcol)+irow ].real=a[ (*lda)*j+i ].real;
           A[ LDA*(j-jcol)+irow ].imag=a[ (*lda)*j+i ].imag;
        }
     }
     i=*ku;
     irow=*ku+*kl-i;
     for( j=0; j<*n; j++ ){
        A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
        A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
     }
     for( i=*ku+1; i<*ku+*kl+1; i++ ){
        irow=*ku+*kl-i;
        jcol=i-(*ku);
        for( j=jcol; j<(*n+*kl); j++ ){
           A[ LDA*j+irow ].real=a[ (*lda)*(j-jcol)+i ].real;
           A[ LDA*j+irow ].imag=a[ (*lda)*(j-jcol)+i ].imag;
        }
     }
     cblas_zgbmv( CblasRowMajor, trans, *m, *n, *kl, *ku, alpha, A, LDA, x,
		  *incx, beta, y, *incy );
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zgbmv( CblasColMajor, trans, *m, *n, *kl, *ku, alpha, a, *lda, x,
		  *incx, beta, y, *incy );
  else
     cblas_zgbmv( UNDEFINED, trans, *m, *n, *kl, *ku, alpha, a, *lda, x,
		  *incx, beta, y, *incy );
}

void F77_zgeru(int *layout, int *m, int *n, CBLAS_TEST_ZOMPLEX *alpha,
	 CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *y, int *incy,
         CBLAS_TEST_ZOMPLEX *a, int *lda){

  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;

  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A=(CBLAS_TEST_ZOMPLEX*)malloc((*m)*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ){
           A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
           A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
     }
     cblas_zgeru( CblasRowMajor, *m, *n, alpha, x, *incx, y, *incy, A, LDA );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ){
           a[ (*lda)*j+i ].real=A[ LDA*i+j ].real;
           a[ (*lda)*j+i ].imag=A[ LDA*i+j ].imag;
        }
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zgeru( CblasColMajor, *m, *n, alpha, x, *incx, y, *incy, a, *lda );
  else
     cblas_zgeru( UNDEFINED, *m, *n, alpha, x, *incx, y, *incy, a, *lda );
}

void F77_zgerc(int *layout, int *m, int *n, CBLAS_TEST_ZOMPLEX *alpha,
	 CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *y, int *incy,
         CBLAS_TEST_ZOMPLEX *a, int *lda) {
  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;

  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A=(CBLAS_TEST_ZOMPLEX* )malloc((*m)*LDA*sizeof(CBLAS_TEST_ZOMPLEX ) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ){
           A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
           A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
        }
     cblas_zgerc( CblasRowMajor, *m, *n, alpha, x, *incx, y, *incy, A, LDA );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ ){
           a[ (*lda)*j+i ].real=A[ LDA*i+j ].real;
           a[ (*lda)*j+i ].imag=A[ LDA*i+j ].imag;
        }
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zgerc( CblasColMajor, *m, *n, alpha, x, *incx, y, *incy, a, *lda );
  else
     cblas_zgerc( UNDEFINED, *m, *n, alpha, x, *incx, y, *incy, a, *lda );
}

void F77_zhemv(int *layout, char *uplow, int *n, CBLAS_TEST_ZOMPLEX *alpha,
      CBLAS_TEST_ZOMPLEX *a, int *lda, CBLAS_TEST_ZOMPLEX *x,
      int *incx, CBLAS_TEST_ZOMPLEX *beta, CBLAS_TEST_ZOMPLEX *y, int *incy){

  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;
  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A = (CBLAS_TEST_ZOMPLEX *)malloc((*n)*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ ){
           A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
           A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
     }
     cblas_zhemv( CblasRowMajor, uplo, *n, alpha, A, LDA, x, *incx,
	    beta, y, *incy );
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zhemv( CblasColMajor, uplo, *n, alpha, a, *lda, x, *incx,
	   beta, y, *incy );
  else
     cblas_zhemv( UNDEFINED, uplo, *n, alpha, a, *lda, x, *incx,
	   beta, y, *incy );
}

void F77_zhbmv(int *layout, char *uplow, int *n, int *k,
     CBLAS_TEST_ZOMPLEX *alpha, CBLAS_TEST_ZOMPLEX *a, int *lda,
     CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *beta,
     CBLAS_TEST_ZOMPLEX *y, int *incy){

CBLAS_TEST_ZOMPLEX *A;
int i,irow,j,jcol,LDA;

  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_zhbmv(CblasRowMajor, UNDEFINED, *n, *k, alpha, a, *lda, x,
		 *incx, beta, y, *incy );
     else {
        LDA = *k+2;
        A =(CBLAS_TEST_ZOMPLEX*)malloc((*n+*k)*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
        if (uplo == CblasUpper) {
           for( i=0; i<*k; i++ ){
              irow=*k-i;
              jcol=(*k)-i;
              for( j=jcol; j<*n; j++ ) {
                 A[ LDA*(j-jcol)+irow ].real=a[ (*lda)*j+i ].real;
                 A[ LDA*(j-jcol)+irow ].imag=a[ (*lda)*j+i ].imag;
              }
           }
           i=*k;
           irow=*k-i;
           for( j=0; j<*n; j++ ) {
              A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
              A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
           }
        }
        else {
           i=0;
           irow=*k-i;
           for( j=0; j<*n; j++ ) {
              A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
              A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
           }
           for( i=1; i<*k+1; i++ ){
              irow=*k-i;
              jcol=i;
              for( j=jcol; j<(*n+*k); j++ ) {
                 A[ LDA*j+irow ].real=a[ (*lda)*(j-jcol)+i ].real;
                 A[ LDA*j+irow ].imag=a[ (*lda)*(j-jcol)+i ].imag;
              }
           }
        }
        cblas_zhbmv( CblasRowMajor, uplo, *n, *k, alpha, A, LDA, x, *incx,
       		     beta, y, *incy );
        free(A);
      }
   }
   else if (*layout == TEST_COL_MJR)
     cblas_zhbmv(CblasColMajor, uplo, *n, *k, alpha, a, *lda, x, *incx,
                 beta, y, *incy );
   else
     cblas_zhbmv(UNDEFINED, uplo, *n, *k, alpha, a, *lda, x, *incx,
                 beta, y, *incy );
}

void F77_zhpmv(int *layout, char *uplow, int *n, CBLAS_TEST_ZOMPLEX *alpha,
     CBLAS_TEST_ZOMPLEX *ap, CBLAS_TEST_ZOMPLEX *x, int *incx,
     CBLAS_TEST_ZOMPLEX *beta, CBLAS_TEST_ZOMPLEX *y, int *incy){

  CBLAS_TEST_ZOMPLEX *A, *AP;
  int i,j,k,LDA;
  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);
  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_zhpmv(CblasRowMajor, UNDEFINED, *n, alpha, ap, x, *incx,
	         beta, y, *incy);
     else {
        LDA = *n;
        A = (CBLAS_TEST_ZOMPLEX* )malloc(LDA*LDA*sizeof(CBLAS_TEST_ZOMPLEX ));
        AP = (CBLAS_TEST_ZOMPLEX* )malloc( (((LDA+1)*LDA)/2)*
	        sizeof( CBLAS_TEST_ZOMPLEX ));
        if (uplo == CblasUpper) {
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
                 A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
                 AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        else {
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
                 A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ) {
	         AP[ k ].real=A[ LDA*i+j ].real;
	         AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        cblas_zhpmv( CblasRowMajor, uplo, *n, alpha, AP, x, *incx, beta, y,
                     *incy );
        free(A);
        free(AP);
     }
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zhpmv( CblasColMajor, uplo, *n, alpha, ap, x, *incx, beta, y,
                  *incy );
  else
     cblas_zhpmv( UNDEFINED, uplo, *n, alpha, ap, x, *incx, beta, y,
                  *incy );
}

void F77_ztbmv(int *layout, char *uplow, char *transp, char *diagn,
     int *n, int *k, CBLAS_TEST_ZOMPLEX *a, int *lda, CBLAS_TEST_ZOMPLEX *x,
     int *incx) {
  CBLAS_TEST_ZOMPLEX *A;
  int irow, jcol, i, j, LDA;
  CBLAS_TRANSPOSE trans;
  CBLAS_UPLO uplo;
  CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_ztbmv(CblasRowMajor, UNDEFINED, trans, diag, *n, *k, a, *lda,
	x, *incx);
     else {
        LDA = *k+2;
        A=(CBLAS_TEST_ZOMPLEX *)malloc((*n+*k)*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
        if (uplo == CblasUpper) {
           for( i=0; i<*k; i++ ){
              irow=*k-i;
              jcol=(*k)-i;
              for( j=jcol; j<*n; j++ ) {
                 A[ LDA*(j-jcol)+irow ].real=a[ (*lda)*j+i ].real;
                 A[ LDA*(j-jcol)+irow ].imag=a[ (*lda)*j+i ].imag;
              }
           }
           i=*k;
           irow=*k-i;
           for( j=0; j<*n; j++ ) {
              A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
              A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
           }
        }
        else {
          i=0;
          irow=*k-i;
          for( j=0; j<*n; j++ ) {
             A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
             A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
          }
          for( i=1; i<*k+1; i++ ){
             irow=*k-i;
             jcol=i;
             for( j=jcol; j<(*n+*k); j++ ) {
                A[ LDA*j+irow ].real=a[ (*lda)*(j-jcol)+i ].real;
                A[ LDA*j+irow ].imag=a[ (*lda)*(j-jcol)+i ].imag;
             }
          }
        }
        cblas_ztbmv(CblasRowMajor, uplo, trans, diag, *n, *k, A, LDA, x,
		    *incx);
        free(A);
     }
   }
   else if (*layout == TEST_COL_MJR)
     cblas_ztbmv(CblasColMajor, uplo, trans, diag, *n, *k, a, *lda, x, *incx);
   else
     cblas_ztbmv(UNDEFINED, uplo, trans, diag, *n, *k, a, *lda, x, *incx);
}

void F77_ztbsv(int *layout, char *uplow, char *transp, char *diagn,
      int *n, int *k, CBLAS_TEST_ZOMPLEX *a, int *lda, CBLAS_TEST_ZOMPLEX *x,
      int *incx) {

  CBLAS_TEST_ZOMPLEX *A;
  int irow, jcol, i, j, LDA;
  CBLAS_TRANSPOSE trans;
  CBLAS_UPLO uplo;
  CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_ztbsv(CblasRowMajor, UNDEFINED, trans, diag, *n, *k, a, *lda, x,
	         *incx);
     else {
        LDA = *k+2;
        A=(CBLAS_TEST_ZOMPLEX*)malloc((*n+*k)*LDA*sizeof(CBLAS_TEST_ZOMPLEX ));
        if (uplo == CblasUpper) {
           for( i=0; i<*k; i++ ){
              irow=*k-i;
              jcol=(*k)-i;
              for( j=jcol; j<*n; j++ ) {
                 A[ LDA*(j-jcol)+irow ].real=a[ (*lda)*j+i ].real;
                 A[ LDA*(j-jcol)+irow ].imag=a[ (*lda)*j+i ].imag;
              }
           }
           i=*k;
           irow=*k-i;
           for( j=0; j<*n; j++ ) {
              A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
              A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
           }
        }
        else {
           i=0;
           irow=*k-i;
           for( j=0; j<*n; j++ ) {
             A[ LDA*j+irow ].real=a[ (*lda)*j+i ].real;
             A[ LDA*j+irow ].imag=a[ (*lda)*j+i ].imag;
           }
           for( i=1; i<*k+1; i++ ){
              irow=*k-i;
              jcol=i;
              for( j=jcol; j<(*n+*k); j++ ) {
	         A[ LDA*j+irow ].real=a[ (*lda)*(j-jcol)+i ].real;
                 A[ LDA*j+irow ].imag=a[ (*lda)*(j-jcol)+i ].imag;
              }
           }
        }
        cblas_ztbsv(CblasRowMajor, uplo, trans, diag, *n, *k, A, LDA,
		    x, *incx);
        free(A);
     }
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ztbsv(CblasColMajor, uplo, trans, diag, *n, *k, a, *lda, x, *incx);
  else
     cblas_ztbsv(UNDEFINED, uplo, trans, diag, *n, *k, a, *lda, x, *incx);
}

void F77_ztpmv(int *layout, char *uplow, char *transp, char *diagn,
      int *n, CBLAS_TEST_ZOMPLEX *ap, CBLAS_TEST_ZOMPLEX *x, int *incx) {
  CBLAS_TEST_ZOMPLEX *A, *AP;
  int i, j, k, LDA;
  CBLAS_TRANSPOSE trans;
  CBLAS_UPLO uplo;
  CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_ztpmv( CblasRowMajor, UNDEFINED, trans, diag, *n, ap, x, *incx );
     else {
        LDA = *n;
        A=(CBLAS_TEST_ZOMPLEX*)malloc(LDA*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
        AP=(CBLAS_TEST_ZOMPLEX*)malloc((((LDA+1)*LDA)/2)*
	 	sizeof(CBLAS_TEST_ZOMPLEX));
        if (uplo == CblasUpper) {
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
                 A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
                 AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        else {
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
	         A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
	         AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        cblas_ztpmv( CblasRowMajor, uplo, trans, diag, *n, AP, x, *incx );
        free(A);
        free(AP);
     }
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ztpmv( CblasColMajor, uplo, trans, diag, *n, ap, x, *incx );
  else
     cblas_ztpmv( UNDEFINED, uplo, trans, diag, *n, ap, x, *incx );
}

void F77_ztpsv(int *layout, char *uplow, char *transp, char *diagn,
     int *n, CBLAS_TEST_ZOMPLEX *ap, CBLAS_TEST_ZOMPLEX *x, int *incx) {
  CBLAS_TEST_ZOMPLEX *A, *AP;
  int i, j, k, LDA;
  CBLAS_TRANSPOSE trans;
  CBLAS_UPLO uplo;
  CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_ztpsv( CblasRowMajor, UNDEFINED, trans, diag, *n, ap, x, *incx );
     else {
        LDA = *n;
        A=(CBLAS_TEST_ZOMPLEX*)malloc(LDA*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
        AP=(CBLAS_TEST_ZOMPLEX*)malloc((((LDA+1)*LDA)/2)*
		sizeof(CBLAS_TEST_ZOMPLEX));
     	if (uplo == CblasUpper) {
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
       	         A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
	         AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        else {
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
                 A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
	         AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        cblas_ztpsv( CblasRowMajor, uplo, trans, diag, *n, AP, x, *incx );
        free(A);
        free(AP);
     }
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ztpsv( CblasColMajor, uplo, trans, diag, *n, ap, x, *incx );
  else
     cblas_ztpsv( UNDEFINED, uplo, trans, diag, *n, ap, x, *incx );
}

void F77_ztrmv(int *layout, char *uplow, char *transp, char *diagn,
     int *n, CBLAS_TEST_ZOMPLEX *a, int *lda, CBLAS_TEST_ZOMPLEX *x,
      int *incx) {
  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;
  CBLAS_TRANSPOSE trans;
  CBLAS_UPLO uplo;
  CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*layout == TEST_ROW_MJR) {
     LDA=*n+1;
     A=(CBLAS_TEST_ZOMPLEX*)malloc((*n)*LDA*sizeof(CBLAS_TEST_ZOMPLEX));
     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ ) {
	  A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
          A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
       }
     cblas_ztrmv(CblasRowMajor, uplo, trans, diag, *n, A, LDA, x, *incx);
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_ztrmv(CblasColMajor, uplo, trans, diag, *n, a, *lda, x, *incx);
  else
     cblas_ztrmv(UNDEFINED, uplo, trans, diag, *n, a, *lda, x, *incx);
}
void F77_ztrsv(int *layout, char *uplow, char *transp, char *diagn,
       int *n, CBLAS_TEST_ZOMPLEX *a, int *lda, CBLAS_TEST_ZOMPLEX *x,
              int *incx) {
  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;
  CBLAS_TRANSPOSE trans;
  CBLAS_UPLO uplo;
  CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A =(CBLAS_TEST_ZOMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_ZOMPLEX ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ ) {
           A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
	   A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
	}
     cblas_ztrsv(CblasRowMajor, uplo, trans, diag, *n, A, LDA, x, *incx );
     free(A);
   }
   else if (*layout == TEST_COL_MJR)
     cblas_ztrsv(CblasColMajor, uplo, trans, diag, *n, a, *lda, x, *incx );
   else
     cblas_ztrsv(UNDEFINED, uplo, trans, diag, *n, a, *lda, x, *incx );
}

void F77_zhpr(int *layout, char *uplow, int *n, double *alpha,
	     CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *ap) {
  CBLAS_TEST_ZOMPLEX *A, *AP;
  int i,j,k,LDA;
  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_zhpr(CblasRowMajor, UNDEFINED, *n, *alpha, x, *incx, ap );
     else {
        LDA = *n;
        A = (CBLAS_TEST_ZOMPLEX* )malloc(LDA*LDA*sizeof(CBLAS_TEST_ZOMPLEX ) );
        AP = ( CBLAS_TEST_ZOMPLEX* )malloc( (((LDA+1)*LDA)/2)*
		sizeof( CBLAS_TEST_ZOMPLEX ));
        if (uplo == CblasUpper) {
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ){
                 A[ LDA*i+j ].real=ap[ k ].real;
                 A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ){
                 AP[ k ].real=A[ LDA*i+j ].real;
                 AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        else {
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ){
                 A[ LDA*i+j ].real=ap[ k ].real;
       	         A[ LDA*i+j ].imag=ap[ k ].imag;
              }
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ){
                 AP[ k ].real=A[ LDA*i+j ].real;
                 AP[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        cblas_zhpr(CblasRowMajor, uplo, *n, *alpha, x, *incx, AP );
        if (uplo == CblasUpper) {
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ){
                 A[ LDA*i+j ].real=AP[ k ].real;
                 A[ LDA*i+j ].imag=AP[ k ].imag;
              }
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ){
                 ap[ k ].real=A[ LDA*i+j ].real;
                 ap[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        else {
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ){
                 A[ LDA*i+j ].real=AP[ k ].real;
                 A[ LDA*i+j ].imag=AP[ k ].imag;
              }
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ){
                 ap[ k ].real=A[ LDA*i+j ].real;
                 ap[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        free(A);
        free(AP);
     }
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zhpr(CblasColMajor, uplo, *n, *alpha, x, *incx, ap );
  else
     cblas_zhpr(UNDEFINED, uplo, *n, *alpha, x, *incx, ap );
}

void F77_zhpr2(int *layout, char *uplow, int *n, CBLAS_TEST_ZOMPLEX *alpha,
       CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *y, int *incy,
       CBLAS_TEST_ZOMPLEX *ap) {
  CBLAS_TEST_ZOMPLEX *A, *AP;
  int i,j,k,LDA;
  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*layout == TEST_ROW_MJR) {
     if (uplo != CblasUpper && uplo != CblasLower )
        cblas_zhpr2( CblasRowMajor, UNDEFINED, *n, alpha, x, *incx, y,
		     *incy, ap );
     else {
        LDA = *n;
        A=(CBLAS_TEST_ZOMPLEX*)malloc( LDA*LDA*sizeof(CBLAS_TEST_ZOMPLEX ) );
        AP=(CBLAS_TEST_ZOMPLEX*)malloc( (((LDA+1)*LDA)/2)*
	sizeof( CBLAS_TEST_ZOMPLEX ));
        if (uplo == CblasUpper) {
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ) {
                 A[ LDA*i+j ].real=ap[ k ].real;
	         A[ LDA*i+j ].imag=ap[ k ].imag;
	      }
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
	         AP[ k ].imag=A[ LDA*i+j ].imag;
	      }
        }
        else {
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ) {
	         A[ LDA*i+j ].real=ap[ k ].real;
	         A[ LDA*i+j ].imag=ap[ k ].imag;
	      }
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ) {
                 AP[ k ].real=A[ LDA*i+j ].real;
	         AP[ k ].imag=A[ LDA*i+j ].imag;
	      }
        }
        cblas_zhpr2( CblasRowMajor, uplo, *n, alpha, x, *incx, y, *incy, AP );
        if (uplo == CblasUpper) {
           for( i=0, k=0; i<*n; i++ )
              for( j=i; j<*n; j++, k++ ) {
                 A[ LDA*i+j ].real=AP[ k ].real;
                 A[ LDA*i+j ].imag=AP[ k ].imag;
              }
           for( j=0, k=0; j<*n; j++ )
              for( i=0; i<j+1; i++, k++ ) {
                 ap[ k ].real=A[ LDA*i+j ].real;
	         ap[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        else {
           for( i=0, k=0; i<*n; i++ )
              for( j=0; j<i+1; j++, k++ ) {
                 A[ LDA*i+j ].real=AP[ k ].real;
	         A[ LDA*i+j ].imag=AP[ k ].imag;
              }
           for( j=0, k=0; j<*n; j++ )
              for( i=j; i<*n; i++, k++ ) {
                 ap[ k ].real=A[ LDA*i+j ].real;
	         ap[ k ].imag=A[ LDA*i+j ].imag;
              }
        }
        free(A);
        free(AP);
     }
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zhpr2( CblasColMajor, uplo, *n, alpha, x, *incx, y, *incy, ap );
  else
     cblas_zhpr2( UNDEFINED, uplo, *n, alpha, x, *incx, y, *incy, ap );
}

void F77_zher(int *layout, char *uplow, int *n, double *alpha,
  CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *a, int *lda) {
  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;
  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A=(CBLAS_TEST_ZOMPLEX*)malloc((*n)*LDA*sizeof( CBLAS_TEST_ZOMPLEX ));

     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ ) {
	  A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
          A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
       }

     cblas_zher(CblasRowMajor, uplo, *n, *alpha, x, *incx, A, LDA );
     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ ) {
	  a[ (*lda)*j+i ].real=A[ LDA*i+j ].real;
          a[ (*lda)*j+i ].imag=A[ LDA*i+j ].imag;
       }
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zher( CblasColMajor, uplo, *n, *alpha, x, *incx, a, *lda );
  else
     cblas_zher( UNDEFINED, uplo, *n, *alpha, x, *incx, a, *lda );
}

void F77_zher2(int *layout, char *uplow, int *n, CBLAS_TEST_ZOMPLEX *alpha,
          CBLAS_TEST_ZOMPLEX *x, int *incx, CBLAS_TEST_ZOMPLEX *y, int *incy,
	  CBLAS_TEST_ZOMPLEX *a, int *lda) {

  CBLAS_TEST_ZOMPLEX *A;
  int i,j,LDA;
  CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*layout == TEST_ROW_MJR) {
     LDA = *n+1;
     A= ( CBLAS_TEST_ZOMPLEX* )malloc((*n)*LDA*sizeof(CBLAS_TEST_ZOMPLEX ) );

     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ ) {
	  A[ LDA*i+j ].real=a[ (*lda)*j+i ].real;
          A[ LDA*i+j ].imag=a[ (*lda)*j+i ].imag;
       }

     cblas_zher2(CblasRowMajor, uplo, *n, alpha, x, *incx, y, *incy, A, LDA );
     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ ) {
	  a[ (*lda)*j+i ].real=A[ LDA*i+j ].real;
          a[ (*lda)*j+i ].imag=A[ LDA*i+j ].imag;
       }
     free(A);
  }
  else if (*layout == TEST_COL_MJR)
     cblas_zher2( CblasColMajor, uplo, *n, alpha, x, *incx, y, *incy, a, *lda);
  else
     cblas_zher2( UNDEFINED, uplo, *n, alpha, x, *incx, y, *incy, a, *lda);
}
