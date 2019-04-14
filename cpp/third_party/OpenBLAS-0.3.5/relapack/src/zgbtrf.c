#include "relapack.h"
#include "stdlib.h"

static void RELAPACK_zgbtrf_rec(const int *, const int *, const int *,
    const int *, double *, const int *, int *, double *, const int *, double *,
    const int *, int *);


/** ZGBTRF computes an LU factorization of a complex m-by-n band matrix A using partial pivoting with row interchanges.
 *
 * This routine is functionally equivalent to LAPACK's zgbtrf.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/dc/dcb/zgbtrf_8f.html
 * */
void RELAPACK_zgbtrf(
    const int *m, const int *n, const int *kl, const int *ku,
    double *Ab, const int *ldAb, int *ipiv,
    int *info
) {

    // Check arguments
    *info = 0;
    if (*m < 0)
        *info = -1;
    else if (*n < 0)
        *info = -2;
    else if (*kl < 0)
        *info = -3;
    else if (*ku < 0)
        *info = -4;
    else if (*ldAb < 2 * *kl + *ku + 1)
        *info = -6;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("ZGBTRF", &minfo);
        return;
    }

    // Constant
    const double ZERO[] = { 0., 0. };

    // Result upper band width
    const int kv = *ku + *kl;

    // Unskew A
    const int ldA[] = { *ldAb - 1 };
    double *const A = Ab + 2 * kv;

    // Zero upper diagonal fill-in elements
    int i, j;
    for (j = 0; j < *n; j++) {
        double *const A_j = A + 2 * *ldA * j;
        for (i = MAX(0, j - kv); i < j - *ku; i++)
            A_j[2 * i] = A_j[2 * i + 1] = 0.;
    }

    // Allocate work space
    const int n1 = ZREC_SPLIT(*n);
    const int mWorkl = (kv > n1) ? MAX(1, *m - *kl) : kv;
    const int nWorkl = (kv > n1) ? n1 : kv;
    const int mWorku = (*kl > n1) ? n1 : *kl;
    const int nWorku = (*kl > n1) ? MAX(0, *n - *kl) : *kl;
    double *Workl = malloc(mWorkl * nWorkl * 2 * sizeof(double));
    double *Worku = malloc(mWorku * nWorku * 2 * sizeof(double));
    LAPACK(zlaset)("L", &mWorkl, &nWorkl, ZERO, ZERO, Workl, &mWorkl);
    LAPACK(zlaset)("U", &mWorku, &nWorku, ZERO, ZERO, Worku, &mWorku);

    // Recursive kernel
    RELAPACK_zgbtrf_rec(m, n, kl, ku, Ab, ldAb, ipiv, Workl, &mWorkl, Worku, &mWorku, info);

    // Free work space
    free(Workl);
    free(Worku);
}


/** zgbtrf's recursive compute kernel */
static void RELAPACK_zgbtrf_rec(
    const int *m, const int *n, const int *kl, const int *ku,
    double *Ab, const int *ldAb, int *ipiv,
    double *Workl, const int *ldWorkl, double *Worku, const int *ldWorku,
    int *info
) {

    if (*n <= MAX(CROSSOVER_ZGBTRF, 1)) {
        // Unblocked
        LAPACK(zgbtf2)(m, n, kl, ku, Ab, ldAb, ipiv, info);
        return;
    }

    // Constants
    const double ONE[]  = { 1., 0. };
    const double MONE[] = { -1., 0. };
    const int    iONE[] = { 1 };

    // Loop iterators
    int i, j;

    // Output upper band width
    const int kv = *ku + *kl;

    // Unskew A
    const int ldA[] = { *ldAb - 1 };
    double *const A = Ab + 2 * kv;

    // Splitting
    const int n1  = MIN(ZREC_SPLIT(*n), *kl);
    const int n2  = *n - n1;
    const int m1  = MIN(n1, *m);
    const int m2  = *m - m1;
    const int mn1 = MIN(m1, n1);
    const int mn2 = MIN(m2, n2);

    // Ab_L *
    //      Ab_BR
    double *const Ab_L  = Ab;
    double *const Ab_BR = Ab + 2 * *ldAb * n1;

    // A_L A_R
    double *const A_L = A;
    double *const A_R = A + 2 * *ldA * n1;

    // A_TL A_TR
    // A_BL A_BR
    double *const A_TL = A;
    double *const A_TR = A + 2 * *ldA * n1;
    double *const A_BL = A                 + 2 * m1;
    double *const A_BR = A + 2 * *ldA * n1 + 2 * m1;

    // ipiv_T
    // ipiv_B
    int *const ipiv_T = ipiv;
    int *const ipiv_B = ipiv + n1;

    // Banded splitting
    const int n21 = MIN(n2, kv - n1);
    const int n22 = MIN(n2 - n21, n1);
    const int m21 = MIN(m2, *kl - m1);
    const int m22 = MIN(m2 - m21, m1);

    //   n1 n21  n22
    // m *  A_Rl ARr
    double *const A_Rl = A_R;
    double *const A_Rr = A_R + 2 * *ldA * n21;

    //     n1    n21    n22
    // m1  *     A_TRl  A_TRr
    // m21 A_BLt A_BRtl A_BRtr
    // m22 A_BLb A_BRbl A_BRbr
    double *const A_TRl  = A_TR;
    double *const A_TRr  = A_TR + 2 * *ldA * n21;
    double *const A_BLt  = A_BL;
    double *const A_BLb  = A_BL                  + 2 * m21;
    double *const A_BRtl = A_BR;
    double *const A_BRtr = A_BR + 2 * *ldA * n21;
    double *const A_BRbl = A_BR                  + 2 * m21;
    double *const A_BRbr = A_BR + 2 * *ldA * n21 + 2 * m21;

    // recursion(Ab_L, ipiv_T)
    RELAPACK_zgbtrf_rec(m, &n1, kl, ku, Ab_L, ldAb, ipiv_T, Workl, ldWorkl, Worku, ldWorku, info);

    // Workl = A_BLb
    LAPACK(zlacpy)("U", &m22, &n1, A_BLb, ldA, Workl, ldWorkl);

    // partially redo swaps in A_L
    for (i = 0; i < mn1; i++) {
        const int ip = ipiv_T[i] - 1;
        if (ip != i) {
            if (ip < *kl)
                BLAS(zswap)(&i, A_L + 2 * i, ldA, A_L + 2 * ip, ldA);
            else
                BLAS(zswap)(&i, A_L + 2 * i, ldA, Workl + 2 * (ip - *kl), ldWorkl);
        }
    }

    // apply pivots to A_Rl
    LAPACK(zlaswp)(&n21, A_Rl, ldA, iONE, &mn1, ipiv_T, iONE);

    // apply pivots to A_Rr columnwise
    for (j = 0; j < n22; j++) {
        double *const A_Rrj = A_Rr + 2 * *ldA * j;
        for (i = j; i < mn1; i++) {
            const int ip = ipiv_T[i] - 1;
            if (ip != i) {
                const double tmpr = A_Rrj[2 * i];
                const double tmpc = A_Rrj[2 * i + 1];
                A_Rrj[2 * i]     = A_Rrj[2 * ip];
                A_Rrj[2 * i + 1] = A_Rrj[2 * ip + 1];
                A_Rrj[2 * ip]     = tmpr;
                A_Rrj[2 * ip + 1] = tmpc;
            }
        }
    }

    // A_TRl = A_TL \ A_TRl
    BLAS(ztrsm)("L", "L", "N", "U", &m1, &n21, ONE, A_TL, ldA, A_TRl, ldA);
    // Worku = A_TRr
    LAPACK(zlacpy)("L", &m1, &n22, A_TRr, ldA, Worku, ldWorku);
    // Worku = A_TL \ Worku
    BLAS(ztrsm)("L", "L", "N", "U", &m1, &n22, ONE, A_TL, ldA, Worku, ldWorku);
    // A_TRr = Worku
    LAPACK(zlacpy)("L", &m1, &n22, Worku, ldWorku, A_TRr, ldA);
    // A_BRtl = A_BRtl - A_BLt * A_TRl
    BLAS(zgemm)("N", "N", &m21, &n21, &n1, MONE, A_BLt, ldA, A_TRl, ldA, ONE, A_BRtl, ldA);
    // A_BRbl = A_BRbl - Workl * A_TRl
    BLAS(zgemm)("N", "N", &m22, &n21, &n1, MONE, Workl, ldWorkl, A_TRl, ldA, ONE, A_BRbl, ldA);
    // A_BRtr = A_BRtr - A_BLt * Worku
    BLAS(zgemm)("N", "N", &m21, &n22, &n1, MONE, A_BLt, ldA, Worku, ldWorku, ONE, A_BRtr, ldA);
    // A_BRbr = A_BRbr - Workl * Worku
    BLAS(zgemm)("N", "N", &m22, &n22, &n1, MONE, Workl, ldWorkl, Worku, ldWorku, ONE, A_BRbr, ldA);

    // partially undo swaps in A_L
    for (i = mn1 - 1; i >= 0; i--) {
        const int ip = ipiv_T[i] - 1;
        if (ip != i) {
            if (ip < *kl)
                BLAS(zswap)(&i, A_L + 2 * i, ldA, A_L + 2 * ip, ldA);
            else
                BLAS(zswap)(&i, A_L + 2 * i, ldA, Workl + 2 * (ip - *kl), ldWorkl);
        }
    }

    // recursion(Ab_BR, ipiv_B)
    RELAPACK_zgbtrf_rec(&m2, &n2, kl, ku, Ab_BR, ldAb, ipiv_B, Workl, ldWorkl, Worku, ldWorku, info);
    if (*info)
        *info += n1;
    // shift pivots
    for (i = 0; i < mn2; i++)
        ipiv_B[i] += n1;
}
