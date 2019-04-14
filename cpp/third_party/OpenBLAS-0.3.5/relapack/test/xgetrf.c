#include "test.h"

datatype *A[2];
int *ipiv[2], info;

void pre() {
    x2matgen(n, n, A[0], A[1]);
    memset(ipiv[0], 0, n * sizeof(int));
    memset(ipiv[1], 0, n * sizeof(int));
}

void post() {
    error = x2vecerr(n * n, A[0], A[1]) + i2vecerr(n, ipiv[0], ipiv[1]);
}

void tests() {
    A[0] = xmalloc(n * n);
    A[1] = xmalloc(n * n);
    ipiv[0] = imalloc(n);
    ipiv[1] = imalloc(n);

    #define ROUTINE XPREF(getrf)

    TEST(&n, &n, A[i], &n, ipiv[i], &info);
    TEST(&n, &n2, A[i], &n, ipiv[i], &info);
    TEST(&n2, &n, A[i], &n, ipiv[i], &info);

    free(A[0]);
    free(A[1]);
    free(ipiv[0]);
    free(ipiv[1]);
}
