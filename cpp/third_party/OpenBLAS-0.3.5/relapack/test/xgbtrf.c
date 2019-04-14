#include "test.h"

datatype *A[2];
int *ipiv[2], info;
int kl, ku, ld;

void pre() {
    int i;
    x2matgen(ld, n, A[0], A[1]);
    for (i = 0; i < n; i++) {
        // set diagonal
        A[0][x1 * (i + ld * i)] =
        A[1][x1 * (i + ld * i)] = (datatype) rand() / RAND_MAX;
    }
    memset(ipiv[0], 0, n * sizeof(int));
    memset(ipiv[1], 0, n * sizeof(int));
}

void post() {
    error = x2vecerr(ld * n, A[0], A[1]) + i2vecerr(n, ipiv[0], ipiv[1]);
}

void tests() {
    kl = n - 10;
    ku = n;
    ld = 2 * kl + ku + 1;

    A[0] = xmalloc(ld * n);
    A[1] = xmalloc(ld * n);
    ipiv[0] = imalloc(n);
    ipiv[1] = imalloc(n);

    #define ROUTINE XPREF(gbtrf)

    TEST(&n, &n, &kl, &ku, A[i], &ld, ipiv[i], &info);
    TEST(&n, &n2, &kl, &ku, A[i], &ld, ipiv[i], &info);
    TEST(&n2, &n, &kl, &ku, A[i], &ld, ipiv[i], &info);

    free(A[0]);
    free(A[1]);
    free(ipiv[0]);
    free(ipiv[1]);
}
