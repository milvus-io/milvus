#include "test.h"

datatype *A[2], *B[2];
int info;

void pre() {
    x2matgen(n, n, A[0], A[1]);
    x2matgen(n, n, B[0], B[1]);
}

void post() {
    error = x2vecerr(n * n, A[0], A[1]);
}

void tests() {
    A[0] = xmalloc(n * n);
    A[1] = xmalloc(n * n);
    B[0] = xmalloc(n * n);
    B[1] = xmalloc(n * n);

    #define ROUTINE XPREF(hegst)

    TEST(iONE, "L", &n, A[i], &n, B[i], &n, &info);
    TEST(iONE, "U", &n, A[i], &n, B[i], &n, &info);
    TEST(iTWO, "L", &n, A[i], &n, B[i], &n, &info);
    TEST(iTWO, "U", &n, A[i], &n, B[i], &n, &info);

    free(A[0]);
    free(A[1]);
    free(B[0]);
    free(B[1]);
}
