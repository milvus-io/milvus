#include "test.h"

datatype *A[2], *Work;
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
    const int lWork = n * n;
    A[0] = xmalloc(n * n);
    A[1] = xmalloc(n * n);
    ipiv[0] = imalloc(n);
    ipiv[1] = imalloc(n);
    Work = xmalloc(lWork);

    #define ROUTINE XPREF(hetrf)

    TEST("L", &n, A[i], &n, ipiv[i], Work, &lWork, &info);
    TEST("U", &n, A[i], &n, ipiv[i], Work, &lWork, &info);

    #undef ROUTINE
    #define ROUTINE XPREF(hetrf_rook)

    TEST("L", &n, A[i], &n, ipiv[i], Work, &lWork, &info);
    TEST("U", &n, A[i], &n, ipiv[i], Work, &lWork, &info);

    free(A[0]);
    free(A[1]);
    free(ipiv[0]);
    free(ipiv[1]);
    free(Work);
}
