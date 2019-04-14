#include "relapack.h"

static void RELAPACK_sgetrf_rec(const int *, const int *, float *, const int *,
    int *, int *);


/** SGETRF computes an LU factorization of a general M-by-N matrix A using partial pivoting with row interchanges.
 *
 * This routine is functionally equivalent to LAPACK's sgetrf.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/de/de2/sgetrf_8f.html
 * */
void RELAPACK_sgetrf(
    const int *m, const int *n,
    float *A, const int *ldA, int *ipiv,
    int *info
) {

    // Check arguments
    *info = 0;
    if (*m < 0)
        *info = -1;
    else if (*n < 0)
        *info = -2;
    else if (*ldA < MAX(1, *n))
        *info = -4;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("SGETRF", &minfo);
        return;
    }

    const int sn = MIN(*m, *n);

    RELAPACK_sgetrf_rec(m, &sn, A, ldA, ipiv, info);

    // Right remainder
    if (*m < *n) {
        // Constants
        const float ONE[] = { 1. };
        const int  iONE[] = { 1. };

        // Splitting
        const int rn = *n - *m;

        // A_L A_R
        const float *const A_L = A;
        float *const       A_R = A + *ldA * *m;

        // A_R = apply(ipiv, A_R)
        LAPACK(slaswp)(&rn, A_R, ldA, iONE, m, ipiv, iONE);
        // A_R = A_L \ A_R
        BLAS(strsm)("L", "L", "N", "U", m, &rn, ONE, A_L, ldA, A_R, ldA);
    }
}


/** sgetrf's recursive compute kernel */
static void RELAPACK_sgetrf_rec(
    const int *m, const int *n,
    float *A, const int *ldA, int *ipiv,
    int *info
) {

    if (*n <= MAX(CROSSOVER_SGETRF, 1)) {
        // Unblocked
        LAPACK(sgetf2)(m, n, A, ldA, ipiv, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1. };
    const float MONE[] = { -1. };
    const int   iONE[] = { 1 };

    // Splitting
    const int n1 = SREC_SPLIT(*n);
    const int n2 = *n - n1;
    const int m2 = *m - n1;

    // A_L A_R
    float *const A_L = A;
    float *const A_R = A + *ldA * n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + *ldA * n1;
    float *const A_BL = A             + n1;
    float *const A_BR = A + *ldA * n1 + n1;

    // ipiv_T
    // ipiv_B
    int *const ipiv_T = ipiv;
    int *const ipiv_B = ipiv + n1;

    // recursion(A_L, ipiv_T)
    RELAPACK_sgetrf_rec(m, &n1, A_L, ldA, ipiv_T, info);
    // apply pivots to A_R
    LAPACK(slaswp)(&n2, A_R, ldA, iONE, &n1, ipiv_T, iONE);

    // A_TR = A_TL \ A_TR
    BLAS(strsm)("L", "L", "N", "U", &n1, &n2, ONE, A_TL, ldA, A_TR, ldA);
    // A_BR = A_BR - A_BL * A_TR
    BLAS(sgemm)("N", "N", &m2, &n2, &n1, MONE, A_BL, ldA, A_TR, ldA, ONE, A_BR, ldA);

    // recursion(A_BR, ipiv_B)
    RELAPACK_sgetrf_rec(&m2, &n2, A_BR, ldA, ipiv_B, info);
    if (*info)
        *info += n1;
    // apply pivots to A_BL
    LAPACK(slaswp)(&n1, A_BL, ldA, iONE, &n2, ipiv_B, iONE);
    // shift pivots
    int i;
    for (i = 0; i < n2; i++)
        ipiv_B[i] += n1;
}
