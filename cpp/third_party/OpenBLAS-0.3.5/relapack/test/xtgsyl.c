#include "test.h"

datatype *A[2], *B[2], *C[2], *D[2], *E[2], *F[2], *Work, scale[2], dif[2];
int *iWork, lWork, info;

#define xlascl XPREF(LAPACK(lascl))
void xlascl(const char *, const int *, const int *, const datatype *, const
        datatype *, const int *, const int *, datatype *, const int *, int *);

#define xscal XPREF(LAPACK(scal))
void xscal(const int *, const datatype *, datatype *, const int *);

void pre() {
    int i;

    x2matgen(n, n, A[0], A[1]);
    x2matgen(n, n, B[0], B[1]);
    x2matgen(n, n, C[0], C[1]);
    x2matgen(n, n, D[0], D[1]);
    x2matgen(n, n, E[0], E[1]);
    x2matgen(n, n, F[0], F[1]);

    for (i = 0; i < n; i++) {
        // set diagonal
        A[0][x1 * (i + n * i)] =
        A[1][x1 * (i + n * i)] = (datatype) rand() / RAND_MAX;
        E[0][x1 * (i + n * i)] =
        E[1][x1 * (i + n * i)] = (datatype) rand() / RAND_MAX;
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
    if (scale[0]) {
        xlascl("G", iZERO, iZERO, &scale[0], &scale[1], &n, &n, C[0], &n, &info);
        xlascl("G", iZERO, iZERO, &scale[0], &scale[1], &n, &n, F[0], &n, &info);
    }
    error = x2vecerr(n * n, C[0], C[1]) + x2vecerr(n * n, F[0], F[1]);
}

void tests() {
    lWork = 2 * n * n;
    A[0] = xmalloc(n * n);
    A[1] = xmalloc(n * n);
    B[0] = xmalloc(n * n);
    B[1] = xmalloc(n * n);
    C[0] = xmalloc(n * n);
    C[1] = xmalloc(n * n);
    D[0] = xmalloc(n * n);
    D[1] = xmalloc(n * n);
    E[0] = xmalloc(n * n);
    E[1] = xmalloc(n * n);
    F[0] = xmalloc(n * n);
    F[1] = xmalloc(n * n);
    Work = xmalloc(lWork);
    iWork = imalloc(n + n + 2);

    #define ROUTINE XPREF(tgsyl)

    TEST("N", iZERO, &n, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST("N", iZERO, &n2, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST("N", iZERO, &n, &n2, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST("N", iONE, &n, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST("N", iTWO, &n, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST("N", iTHREE, &n, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST("N", iFOUR, &n, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);
    TEST(xCTRANS, iZERO, &n, &n, A[i], &n, B[i], &n, C[i], &n, D[i], &n, E[i], &n, F[i], &n, &scale[i], &dif[i], Work, &lWork, iWork, &info);

    free(A[0]);
    free(A[1]);
    free(B[0]);
    free(B[1]);
    free(C[0]);
    free(C[1]);
    free(D[0]);
    free(D[1]);
    free(E[0]);
    free(E[1]);
    free(F[0]);
    free(F[1]);
    free(Work);
    free(iWork);
}
