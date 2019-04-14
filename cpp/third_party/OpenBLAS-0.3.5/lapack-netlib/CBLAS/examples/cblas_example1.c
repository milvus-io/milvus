/* cblas_example.c */

#include <stdio.h>
#include <stdlib.h>
#include "cblas.h"

int main ( )
{
   CBLAS_LAYOUT Layout;
   CBLAS_TRANSPOSE transa;

   double *a, *x, *y;
   double alpha, beta;
   int m, n, lda, incx, incy, i;

   Layout = CblasColMajor;
   transa = CblasNoTrans;

   m = 4; /* Size of Column ( the number of rows ) */
   n = 4; /* Size of Row ( the number of columns ) */
   lda = 4; /* Leading dimension of 5 * 4 matrix is 5 */
   incx = 1;
   incy = 1;
   alpha = 1;
   beta = 0;

   a = (double *)malloc(sizeof(double)*m*n);
   x = (double *)malloc(sizeof(double)*n);
   y = (double *)malloc(sizeof(double)*n);
   /* The elements of the first column */
   a[0] = 1;
   a[1] = 2;
   a[2] = 3;
   a[3] = 4;
   /* The elements of the second column */
   a[m] = 1;
   a[m+1] = 1;
   a[m+2] = 1;
   a[m+3] = 1;
   /* The elements of the third column */
   a[m*2] = 3;
   a[m*2+1] = 4;
   a[m*2+2] = 5;
   a[m*2+3] = 6;
   /* The elements of the fourth column */
   a[m*3] = 5;
   a[m*3+1] = 6;
   a[m*3+2] = 7;
   a[m*3+3] = 8;
   /* The elemetns of x and y */
   x[0] = 1;
   x[1] = 2;
   x[2] = 1;
   x[3] = 1;
   y[0] = 0;
   y[1] = 0;
   y[2] = 0;
   y[3] = 0;

   cblas_dgemv( Layout, transa, m, n, alpha, a, lda, x, incx, beta,
                y, incy );
   /* Print y */
   for( i = 0; i < n; i++ )
      printf(" y%d = %f\n", i, y[i]);
   free(a);
   free(x);
   free(y);
   return 0;
}
