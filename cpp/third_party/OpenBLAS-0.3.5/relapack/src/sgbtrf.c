#include "relapack.h"
#include "stdlib.h"

static void RELAPACK_sgbtrf_rec(const int *, const int *, const int *,
    const int *, float *, const int *, int *, float *, const int *, float *,
    const int *, int *);


/** SGBTRF computes an LU factorization of a real m-by-n band matrix A using partial pivoting with row interchanges.
 *
 * This routine is functionally equivalent to LAPACK's sgbtrf.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d5/d72/sgbtrf_8f.html
 * */
void RELAPACK_sgbtrf(
    const int *m, const int *n, const int *kl, const int *ku,
    float *Ab, const int *ldAb, int *ipiv,
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
        LAPACK(xerbla)("SGBTRF", &minfo);
        return;
    }

    // Constant
    const float ZERO[] = { 0. };

    // Result upper band width
    const int kv = *ku + *kl;

    // Unskewg A
    const int ldA[] = { *ldAb - 1 };
    float *const A = Ab + kv;

    // Zero upper diagonal fill-in elements
    int i, j;
    for (j = 0; j < *n; j++) {
        float *const A_j = A + *ldA * j;
        for (i = MAX(0, j - kv); i < j - *ku; i++)
            A_j[i] = 0.;
    }

    // Allocate work space
    const int n1 = SREC_SPLIT(*n);
    const int mWorkl = (kv > n1) ? MAX(1, *m - *kl) : kv;
    const int nWorkl = (kv > n1) ? n1 : kv;
    const int mWorku = (*kl > n1) ? n1 : *kl;
    const int nWorku = (*kl > n1) ? MAX(0, *n - *kl) : *kl;
    float *Workl = malloc(mWorkl * nWorkl * sizeof(float));
    float *Worku = malloc(mWorku * nWorku * sizeof(float));
    LAPACK(slaset)("L", &mWorkl, &nWorkl, ZERO, ZERO, Workl, &mWorkl);
    LAPACK(slaset)("U", &mWorku, &nWorku, ZERO, ZERO, Worku, &mWorku);

    // Recursive kernel
    RELAPACK_sgbtrf_rec(m, n, kl, ku, Ab, ldAb, ipiv, Workl, &mWorkl, Worku, &mWorku, info);

    // Free work space
    free(Workl);
    free(Worku);
}


/** sgbtrf's recursive compute kernel */
static void RELAPACK_sgbtrf_rec(
    const int *m, const int *n, const int *kl, const int *ku,
    float *Ab, const int *ldAb, int *ipiv,
    float *Workl, const int *ldWorkl, float *Worku, const int *ldWorku,
    int *info
) {

    if (*n <= MAX(CROSSOVER_SGBTRF, 1)) {
        // Unblocked
        LAPACK(sgbtf2)(m, n, kl, ku, Ab, ldAb, ipiv, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1. };
    const float MONE[] = { -1. };
    const int    iONE[] = { 1 };

    // Loop iterators
    int i, j;

    // Output upper band width
    const int kv = *ku + *kl;

    // Unskew A
    const int ldA[] = { *ldAb - 1 };
    float *const A = Ab + kv;

    // Splitting
    const int n1  = MIN(SREC_SPLIT(*n), *kl);
    const int n2  = *n - n1;
    const int m1  = MIN(n1, *m);
    const int m2  = *m - m1;
    const int mn1 = MIN(m1, n1);
    const int mn2 = MIN(m2, n2);

    // Ab_L *
    //      Ab_BR
    float *const Ab_L  = Ab;
    float *const Ab_BR = Ab + *ldAb * n1;

    // A_L A_R
    float *const A_L = A;
    float *const A_R = A + *ldA * n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + *ldA * n1;
    float *const A_BL = A             + m1;
    float *const A_BR = A + *ldA * n1 + m1;

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
    float *const A_Rl = A_R;
    float *const A_Rr = A_R + *ldA * n21;

    //     n1    n21    n22
    // m1  *     A_TRl  A_TRr
    // m21 A_BLt A_BRtl A_BRtr
    // m22 A_BLb A_BRbl A_BRbr
    float *const A_TRl  = A_TR;
    float *const A_TRr  = A_TR + *ldA * n21;
    float *const A_BLt  = A_BL;
    float *const A_BLb  = A_BL              + m21;
    float *const A_BRtl = A_BR;
    float *const A_BRtr = A_BR + *ldA * n21;
    float *const A_BRbl = A_BR              + m21;
    float *const A_BRbr = A_BR + *ldA * n21 + m21;

    // recursion(Ab_L, ipiv_T)
    RELAPACK_sgbtrf_rec(m, &n1, kl, ku, Ab_L, ldAb, ipiv_T, Workl, ldWorkl, Worku, ldWorku, info);

    // Workl = A_BLb
    LAPACK(slacpy)("U", &m22, &n1, A_BLb, ldA, Workl, ldWorkl);

    // partially redo swaps in A_L
    for (i = 0; i < mn1; i++) {
        const int ip = ipiv_T[i] - 1;
        if (ip != i) {
            if (ip < *kl)
                BLAS(sswap)(&i, A_L + i, ldA, A_L + ip, ldA);
            else
                BLAS(sswap)(&i, A_L + i, ldA, Workl + ip - *kl, ldWorkl);
        }
    }

    // apply pivots to A_Rl
    LAPACK(slaswp)(&n21, A_Rl, ldA, iONE, &mn1, ipiv_T, iONE);

    // apply pivots to A_Rr columnwise
    for (j = 0; j < n22; j++) {
        float *const A_Rrj = A_Rr + *ldA * j;
        for (i = j; i < mn1; i++) {
            const int ip = ipiv_T[i] - 1;
            if (ip != i) {
                const float tmp = A_Rrj[i];
                A_Rrj[i] = A_Rr[ip];
                A_Rrj[ip] = tmp;
            }
        }
    }

    // A_TRl = A_TL \ A_TRl
    BLAS(strsm)("L", "L", "N", "U", &m1, &n21, ONE, A_TL, ldA, A_TRl, ldA);
    // Worku = A_TRr
    LAPACK(slacpy)("L", &m1, &n22, A_TRr, ldA, Worku, ldWorku);
    // Worku = A_TL \ Worku
    BLAS(strsm)("L", "L", "N", "U", &m1, &n22, ONE, A_TL, ldA, Worku, ldWorku);
    // A_TRr = Worku
    LAPACK(slacpy)("L", &m1, &n22, Worku, ldWorku, A_TRr, ldA);
    // A_BRtl = A_BRtl - A_BLt * A_TRl
    BLAS(sgemm)("N", "N", &m21, &n21, &n1, MONE, A_BLt, ldA, A_TRl, ldA, ONE, A_BRtl, ldA);
    // A_BRbl = A_BRbl - Workl * A_TRl
    BLAS(sgemm)("N", "N", &m22, &n21, &n1, MONE, Workl, ldWorkl, A_TRl, ldA, ONE, A_BRbl, ldA);
    // A_BRtr = A_BRtr - A_BLt * Worku
    BLAS(sgemm)("N", "N", &m21, &n22, &n1, MONE, A_BLt, ldA, Worku, ldWorku, ONE, A_BRtr, ldA);
    // A_BRbr = A_BRbr - Workl * Worku
    BLAS(sgemm)("N", "N", &m22, &n22, &n1, MONE, Workl, ldWorkl, Worku, ldWorku, ONE, A_BRbr, ldA);

    // partially undo swaps in A_L
    for (i = mn1 - 1; i >= 0; i--) {
        const int ip = ipiv_T[i] - 1;
        if (ip != i) {
            if (ip < *kl)
                BLAS(sswap)(&i, A_L + i, ldA, A_L + ip, ldA);
            else
                BLAS(sswap)(&i, A_L + i, ldA, Workl + ip - *kl, ldWorkl);
        }
    }

    // recursion(Ab_BR, ipiv_B)
    RELAPACK_sgbtrf_rec(&m2, &n2, kl, ku, Ab_BR, ldAb, ipiv_B, Workl, ldWorkl, Worku, ldWorku, info);
    if (*info)
        *info += n1;
    // shift pivots
    for (i = 0; i < mn2; i++)
        ipiv_B[i] += n1;
}
