/*
 *     Written by D.P. Manley, Digital Equipment Corporation.
 *     Prefixed "C_" to BLAS routines and their declarations.
 *
 *     Modified by T. H. Do, 1/23/98, SGI/CRAY Research.
 */
#include <stdlib.h>
#include "common.h"
#include "cblas_test.h"

void F77_sgemv(int *order, char *transp, int *m, int *n, float *alpha,
	       float *a, int *lda, float *x, int *incx, float *beta,
	       float *y, int *incy ) {

  float *A;
  int i,j,LDA;
  enum CBLAS_TRANSPOSE trans;

  get_transpose_type(transp, &trans);
  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*m)*LDA*sizeof( float ) );
     for( i=0; i<*m; i++ )
        for( j=0; j<*n; j++ )
           A[ LDA*i+j ]=a[ (*lda)*j+i ];
     cblas_sgemv( CblasRowMajor, trans,
		  *m, *n, *alpha, A, LDA, x, *incx, *beta, y, *incy );
     free(A);
  }
  else if (*order == TEST_COL_MJR)
     cblas_sgemv( CblasColMajor, trans,
		  *m, *n, *alpha, a, *lda, x, *incx, *beta, y, *incy );
  else
     cblas_sgemv( UNDEFINED, trans,
		  *m, *n, *alpha, a, *lda, x, *incx, *beta, y, *incy );
}

void F77_sger(int *order, int *m, int *n, float *alpha, float *x, int *incx,
	     float *y, int *incy, float *a, int *lda ) {

  float *A;
  int i,j,LDA;

  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*m)*LDA*sizeof( float ) );

     for( i=0; i<*m; i++ ) {
       for( j=0; j<*n; j++ )
         A[ LDA*i+j ]=a[ (*lda)*j+i ];
     }

     cblas_sger(CblasRowMajor, *m, *n, *alpha, x, *incx, y, *incy, A, LDA );
     for( i=0; i<*m; i++ )
       for( j=0; j<*n; j++ )
         a[ (*lda)*j+i ]=A[ LDA*i+j ];
     free(A);
  }
  else
     cblas_sger( CblasColMajor, *m, *n, *alpha, x, *incx, y, *incy, a, *lda );
}

void F77_strmv(int *order, char *uplow, char *transp, char *diagn,
	      int *n, float *a, int *lda, float *x, int *incx) {
  float *A;
  int i,j,LDA;
  enum CBLAS_TRANSPOSE trans;
  enum CBLAS_UPLO uplo;
  enum CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ )
         A[ LDA*i+j ]=a[ (*lda)*j+i ];
     cblas_strmv(CblasRowMajor, uplo, trans, diag, *n, A, LDA, x, *incx);
     free(A);
  }
  else if (*order == TEST_COL_MJR)
     cblas_strmv(CblasColMajor, uplo, trans, diag, *n, a, *lda, x, *incx);
  else {
     cblas_strmv(UNDEFINED, uplo, trans, diag, *n, a, *lda, x, *incx);
  }
}

void F77_strsv(int *order, char *uplow, char *transp, char *diagn,
	       int *n, float *a, int *lda, float *x, int *incx ) {
  float *A;
  int i,j,LDA;
  enum CBLAS_TRANSPOSE trans;
  enum CBLAS_UPLO uplo;
  enum CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ )
           A[ LDA*i+j ]=a[ (*lda)*j+i ];
     cblas_strsv(CblasRowMajor, uplo, trans, diag, *n, A, LDA, x, *incx );
     free(A);
   }
   else
     cblas_strsv(CblasColMajor, uplo, trans, diag, *n, a, *lda, x, *incx );
}
void F77_ssymv(int *order, char *uplow, int *n, float *alpha, float *a,
	      int *lda, float *x, int *incx, float *beta, float *y,
	      int *incy) {
  float *A;
  int i,j,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ )
           A[ LDA*i+j ]=a[ (*lda)*j+i ];
     cblas_ssymv(CblasRowMajor, uplo, *n, *alpha, A, LDA, x, *incx,
		 *beta, y, *incy );
     free(A);
   }
   else
     cblas_ssymv(CblasColMajor, uplo, *n, *alpha, a, *lda, x, *incx,
		 *beta, y, *incy );
}

void F77_ssyr(int *order, char *uplow, int *n, float *alpha, float *x,
	     int *incx, float *a, int *lda) {
  float *A;
  int i,j,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ )
           A[ LDA*i+j ]=a[ (*lda)*j+i ];
     cblas_ssyr(CblasRowMajor, uplo, *n, *alpha, x, *incx, A, LDA);
     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ )
         a[ (*lda)*j+i ]=A[ LDA*i+j ];
     free(A);
   }
   else
     cblas_ssyr(CblasColMajor, uplo, *n, *alpha, x, *incx, a, *lda);
}

void F77_ssyr2(int *order, char *uplow, int *n, float *alpha, float *x,
	     int *incx, float *y, int *incy, float *a, int *lda) {
  float *A;
  int i,j,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *n+1;
     A   = ( float* )malloc( (*n)*LDA*sizeof( float ) );
     for( i=0; i<*n; i++ )
        for( j=0; j<*n; j++ )
           A[ LDA*i+j ]=a[ (*lda)*j+i ];
     cblas_ssyr2(CblasRowMajor, uplo, *n, *alpha, x, *incx, y, *incy, A, LDA);
     for( i=0; i<*n; i++ )
       for( j=0; j<*n; j++ )
         a[ (*lda)*j+i ]=A[ LDA*i+j ];
     free(A);
   }
   else
     cblas_ssyr2(CblasColMajor, uplo, *n, *alpha, x, *incx, y, *incy, a, *lda);
}

void F77_sgbmv(int *order, char *transp, int *m, int *n, int *kl, int *ku,
	       float *alpha, float *a, int *lda, float *x, int *incx,
	       float *beta, float *y, int *incy ) {

  float *A;
  int i,irow,j,jcol,LDA;
  enum CBLAS_TRANSPOSE trans;

  get_transpose_type(transp, &trans);

  if (*order == TEST_ROW_MJR) {
     LDA = *ku+*kl+2;
     A   = ( float* )malloc( (*n+*kl)*LDA*sizeof( float ) );
     for( i=0; i<*ku; i++ ){
        irow=*ku+*kl-i;
        jcol=(*ku)-i;
        for( j=jcol; j<*n; j++ )
           A[ LDA*(j-jcol)+irow ]=a[ (*lda)*j+i ];
     }
     i=*ku;
     irow=*ku+*kl-i;
     for( j=0; j<*n; j++ )
        A[ LDA*j+irow ]=a[ (*lda)*j+i ];
     for( i=*ku+1; i<*ku+*kl+1; i++ ){
        irow=*ku+*kl-i;
        jcol=i-(*ku);
        for( j=jcol; j<(*n+*kl); j++ )
           A[ LDA*j+irow ]=a[ (*lda)*(j-jcol)+i ];
     }
     cblas_sgbmv( CblasRowMajor, trans, *m, *n, *kl, *ku, *alpha,
		  A, LDA, x, *incx, *beta, y, *incy );
     free(A);
  }
  else
     cblas_sgbmv( CblasColMajor, trans, *m, *n, *kl, *ku, *alpha,
		  a, *lda, x, *incx, *beta, y, *incy );
}

void F77_stbmv(int *order, char *uplow, char *transp, char *diagn,
	      int *n, int *k, float *a, int *lda, float *x, int *incx) {
  float *A;
  int irow, jcol, i, j, LDA;
  enum CBLAS_TRANSPOSE trans;
  enum CBLAS_UPLO uplo;
  enum CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*order == TEST_ROW_MJR) {
     LDA = *k+1;
     A = ( float* )malloc( (*n+*k)*LDA*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( i=0; i<*k; i++ ){
           irow=*k-i;
           jcol=(*k)-i;
           for( j=jcol; j<*n; j++ )
              A[ LDA*(j-jcol)+irow ]=a[ (*lda)*j+i ];
        }
        i=*k;
        irow=*k-i;
        for( j=0; j<*n; j++ )
           A[ LDA*j+irow ]=a[ (*lda)*j+i ];
     }
     else {
       i=0;
       irow=*k-i;
       for( j=0; j<*n; j++ )
          A[ LDA*j+irow ]=a[ (*lda)*j+i ];
       for( i=1; i<*k+1; i++ ){
          irow=*k-i;
          jcol=i;
          for( j=jcol; j<(*n+*k); j++ )
             A[ LDA*j+irow ]=a[ (*lda)*(j-jcol)+i ];
       }
     }
     cblas_stbmv(CblasRowMajor, uplo, trans, diag, *n, *k, A, LDA, x, *incx);
     free(A);
   }
   else
     cblas_stbmv(CblasColMajor, uplo, trans, diag, *n, *k, a, *lda, x, *incx);
}

void F77_stbsv(int *order, char *uplow, char *transp, char *diagn,
	      int *n, int *k, float *a, int *lda, float *x, int *incx) {
  float *A;
  int irow, jcol, i, j, LDA;
  enum CBLAS_TRANSPOSE trans;
  enum CBLAS_UPLO uplo;
  enum CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*order == TEST_ROW_MJR) {
     LDA = *k+1;
     A = ( float* )malloc( (*n+*k)*LDA*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( i=0; i<*k; i++ ){
        irow=*k-i;
        jcol=(*k)-i;
        for( j=jcol; j<*n; j++ )
           A[ LDA*(j-jcol)+irow ]=a[ (*lda)*j+i ];
        }
        i=*k;
        irow=*k-i;
        for( j=0; j<*n; j++ )
           A[ LDA*j+irow ]=a[ (*lda)*j+i ];
     }
     else {
        i=0;
        irow=*k-i;
        for( j=0; j<*n; j++ )
           A[ LDA*j+irow ]=a[ (*lda)*j+i ];
        for( i=1; i<*k+1; i++ ){
           irow=*k-i;
           jcol=i;
           for( j=jcol; j<(*n+*k); j++ )
              A[ LDA*j+irow ]=a[ (*lda)*(j-jcol)+i ];
        }
     }
     cblas_stbsv(CblasRowMajor, uplo, trans, diag, *n, *k, A, LDA, x, *incx);
     free(A);
  }
  else
     cblas_stbsv(CblasColMajor, uplo, trans, diag, *n, *k, a, *lda, x, *incx);
}

void F77_ssbmv(int *order, char *uplow, int *n, int *k, float *alpha,
	      float *a, int *lda, float *x, int *incx, float *beta,
	      float *y, int *incy) {
  float *A;
  int i,j,irow,jcol,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *k+1;
     A   = ( float* )malloc( (*n+*k)*LDA*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( i=0; i<*k; i++ ){
           irow=*k-i;
           jcol=(*k)-i;
           for( j=jcol; j<*n; j++ )
        A[ LDA*(j-jcol)+irow ]=a[ (*lda)*j+i ];
        }
        i=*k;
        irow=*k-i;
        for( j=0; j<*n; j++ )
           A[ LDA*j+irow ]=a[ (*lda)*j+i ];
     }
     else {
        i=0;
        irow=*k-i;
        for( j=0; j<*n; j++ )
           A[ LDA*j+irow ]=a[ (*lda)*j+i ];
        for( i=1; i<*k+1; i++ ){
           irow=*k-i;
           jcol=i;
           for( j=jcol; j<(*n+*k); j++ )
              A[ LDA*j+irow ]=a[ (*lda)*(j-jcol)+i ];
        }
     }
     cblas_ssbmv(CblasRowMajor, uplo, *n, *k, *alpha, A, LDA, x, *incx,
		 *beta, y, *incy );
     free(A);
   }
   else
     cblas_ssbmv(CblasColMajor, uplo, *n, *k, *alpha, a, *lda, x, *incx,
		 *beta, y, *incy );
}

void F77_sspmv(int *order, char *uplow, int *n, float *alpha, float *ap,
	      float *x, int *incx, float *beta, float *y, int *incy) {
  float *A,*AP;
  int i,j,k,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *n;
     A   = ( float* )malloc( LDA*LDA*sizeof( float ) );
     AP  = ( float* )malloc( (((LDA+1)*LDA)/2)*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     else {
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     cblas_sspmv( CblasRowMajor, uplo, *n, *alpha, AP, x, *incx, *beta, y,
		  *incy );
     free(A); free(AP);
  }
  else
     cblas_sspmv( CblasColMajor, uplo, *n, *alpha, ap, x, *incx, *beta, y,
		  *incy );
}

void F77_stpmv(int *order, char *uplow, char *transp, char *diagn,
	      int *n, float *ap, float *x, int *incx) {
  float *A, *AP;
  int i, j, k, LDA;
  enum CBLAS_TRANSPOSE trans;
  enum CBLAS_UPLO uplo;
  enum CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*order == TEST_ROW_MJR) {
     LDA = *n;
     A   = ( float* )malloc( LDA*LDA*sizeof( float ) );
     AP  = ( float* )malloc( (((LDA+1)*LDA)/2)*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     else {
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     cblas_stpmv( CblasRowMajor, uplo, trans, diag, *n, AP, x, *incx );
     free(A); free(AP);
  }
  else
     cblas_stpmv( CblasColMajor, uplo, trans, diag, *n, ap, x, *incx );
}

void F77_stpsv(int *order, char *uplow, char *transp, char *diagn,
	      int *n, float *ap, float *x, int *incx) {
  float *A, *AP;
  int i, j, k, LDA;
  enum CBLAS_TRANSPOSE trans;
  enum CBLAS_UPLO uplo;
  enum CBLAS_DIAG diag;

  get_transpose_type(transp,&trans);
  get_uplo_type(uplow,&uplo);
  get_diag_type(diagn,&diag);

  if (*order == TEST_ROW_MJR) {
     LDA = *n;
     A   = ( float* )malloc( LDA*LDA*sizeof( float ) );
     AP  = ( float* )malloc( (((LDA+1)*LDA)/2)*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];

     }
     else {
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     cblas_stpsv( CblasRowMajor, uplo, trans, diag, *n, AP, x, *incx );
     free(A); free(AP);
  }
  else
     cblas_stpsv( CblasColMajor, uplo, trans, diag, *n, ap, x, *incx );
}

void F77_sspr(int *order, char *uplow, int *n, float *alpha, float *x,
	     int *incx, float *ap ){
  float *A, *AP;
  int i,j,k,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *n;
     A   = ( float* )malloc( LDA*LDA*sizeof( float ) );
     AP  = ( float* )malloc( (((LDA+1)*LDA)/2)*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     else {
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     cblas_sspr( CblasRowMajor, uplo, *n, *alpha, x, *incx, AP );
     if (uplo == CblasUpper) {
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              A[ LDA*i+j ]=AP[ k ];
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              ap[ k ]=A[ LDA*i+j ];
     }
     else {
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              A[ LDA*i+j ]=AP[ k ];
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              ap[ k ]=A[ LDA*i+j ];
     }
     free(A); free(AP);
  }
  else
     cblas_sspr( CblasColMajor, uplo, *n, *alpha, x, *incx, ap );
}

void F77_sspr2(int *order, char *uplow, int *n, float *alpha, float *x,
	     int *incx, float *y, int *incy, float *ap ){
  float *A, *AP;
  int i,j,k,LDA;
  enum CBLAS_UPLO uplo;

  get_uplo_type(uplow,&uplo);

  if (*order == TEST_ROW_MJR) {
     LDA = *n;
     A   = ( float* )malloc( LDA*LDA*sizeof( float ) );
     AP  = ( float* )malloc( (((LDA+1)*LDA)/2)*sizeof( float ) );
     if (uplo == CblasUpper) {
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     else {
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              A[ LDA*i+j ]=ap[ k ];
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              AP[ k ]=A[ LDA*i+j ];
     }
     cblas_sspr2( CblasRowMajor, uplo, *n, *alpha, x, *incx, y, *incy, AP );
     if (uplo == CblasUpper) {
        for( i=0, k=0; i<*n; i++ )
           for( j=i; j<*n; j++, k++ )
              A[ LDA*i+j ]=AP[ k ];
        for( j=0, k=0; j<*n; j++ )
           for( i=0; i<j+1; i++, k++ )
              ap[ k ]=A[ LDA*i+j ];
     }
     else {
        for( i=0, k=0; i<*n; i++ )
           for( j=0; j<i+1; j++, k++ )
              A[ LDA*i+j ]=AP[ k ];
        for( j=0, k=0; j<*n; j++ )
           for( i=j; i<*n; i++, k++ )
              ap[ k ]=A[ LDA*i+j ];
     }
     free(A);
     free(AP);
  }
  else
     cblas_sspr2( CblasColMajor, uplo, *n, *alpha, x, *incx, y, *incy, ap );
}
