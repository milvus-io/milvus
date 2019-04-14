#include "test.h"

datatype *A[2], *B[2], *C[2], *Work, scale[2];
int info;

#define xlascl XPREF(LAPACK(lascl))
void xlascl(const char *, const int *, const int *, const datatype *, const
        datatype *, const int *, const int *, datatype *, const int *, int *);

void pre() {
    int i;

    x2matgen(n, n, A[0], A[1]);
    x2matgen(n, n, B[0], B[1]);
    x2matgen(n, n, C[0], C[1]);

    for (i = 0; i < n; i++) {
        // set diagonal
        A[0][x1 * (i + n * i)] =
        A[1][x1 * (i + n * i)] = (datatype) rand() / RAND_MAX;
        // clear first subdiagonal
        A[0][x1 * (i + 1 + n * i)] =
        A[1][x1 * (i + 1 + n * i)] =
        B[0][x1 * (i + 1 + n * i)] =
        B[1][x1 * (i + 1 + n * i)] =
        A[0][x1 * (i + 1 + n * i) + x1 - 1] =
        A[1][x1 * (i + 1 + n * i) + x1 - 1] =
        B[0][x1 * (i + 1 + n * i) + x1 - 1] =
        B[1][x1 * (i + 1 + n * i) + x1 - 1] = 0;
    }
}

void post() {
    if (scale[0] != 1 || scale[0] != 1)
        printf("scale[RELAPACK] = %12g\tscale[LAPACK] = %12g\n", scale[0], scale[1]);
    if (scale[0])
        xlascl("G", iZERO, iZERO, &scale[0], &scale[1], &n, &n, C[0], &n, &info);
    error = x2vecerr(n * n, C[0], C[1]);
}

void tests() {
    A[0] = xmalloc(n * n);
    A[1] = xmalloc(n * n);
    B[0] = xmalloc(n * n);
    B[1] = xmalloc(n * n);
    C[0] = xmalloc(n * n);
    C[1] = xmalloc(n * n);

    #define ROUTINE XPREF(trsyl)

    TEST("N", "N", iONE, &n, &n, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);
    TEST("N", "N", iONE, &n2, &n, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);
    TEST("N", "N", iONE, &n, &n2, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);
    TEST("C", "N", iONE, &n, &n, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);
    TEST("N", "C", iONE, &n, &n, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);
    TEST("C", "C", iONE, &n, &n, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);
    TEST("N", "N", iMONE, &n, &n, A[i], &n, B[i], &n, C[i], &n, &scale[i], &info);

    free(A[0]);
    free(A[1]);
    free(B[0]);
    free(B[1]);
    free(C[0]);
    free(C[1]);
}
