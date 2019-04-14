#include "test.h"

datatype *A[2];
int info[2];
int n;

void pre() {
    int i;
    x2matgen(n, n, A[0], A[1]);
    for (i = 0; i < n; i++) {
        // set diagonal
        A[0][x1 * (i + n * i)] =
        A[1][x1 * (i + n * i)] = (datatype) rand() / RAND_MAX;
        // set first row
        A[0][x1 * (n * i)] =
        A[1][x1 * (n * i)] = (datatype) rand() / RAND_MAX + n;
    }
}

void post() {
    error = x2vecerr(n * n, A[0], A[1]);
}

void tests() {
    A[0] = xmalloc(n * n);
    A[1] = xmalloc(n * n);

    #define ROUTINE XPREF(pbtrf)

    const int
        kd1 = n / 4,
        kd2 = n * 3 / 4;
    TEST("L", &n, &kd1, A[i], &n, &info[i]);
    TEST("L", &n, &kd2, A[i], &n, &info[i]);
    TEST("U", &n, &kd1, A[i] - x1 * kd1, &n, &info[i]);
    TEST("U", &n, &kd2, A[i] - x1 * kd2, &n, &info[i]);

    free(A[0]);
    free(A[1]);
}
