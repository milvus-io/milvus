#include <stdio.h>
#include "culapack.h"

static int initialized = 0;

int sgetrf_(int *m, int *n, float  *a, int *lda, int *ipiv, int *info) {

  if (!initialized)  {
    culaInitialize();
    initialized = 1;
  }

  *info = culaSgetrf(*m, *m, a, *lda, ipiv);

  return 0;
}

int cgetrf_(int *m, int *n, float  *a, int *lda, int *ipiv, int *info) {

  if (!initialized)  {
    culaInitialize();
    initialized = 1;
  }

  *info = culaCgetrf(*m, *m, (culaFloatComplex *)a, *lda, ipiv);

  return 0;
}
