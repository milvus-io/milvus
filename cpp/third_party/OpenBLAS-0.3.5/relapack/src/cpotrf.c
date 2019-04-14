#include "relapack.h"

static void RELAPACK_cpotrf_rec(const char *, const int *, float *,
        const int *, int *);


/** CPOTRF computes the Cholesky factorization of a complex Hermitian positive definite matrix A.
 *
 * This routine is functionally equivalent to LAPACK's cpotrf.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/dd/dce/cpotrf_8f.html
 * */
void RELAPACK_cpotrf(
    const char *uplo, const int *n,
    float *A, const int *ldA,
    int *info
) {

    // Check arguments
    const int lower = LAPACK(lsame)(uplo, "L");
    const int upper = LAPACK(lsame)(uplo, "U");
    *info = 0;
    if (!lower && !upper)
        *info = -1;
    else if (*n < 0)
        *info = -2;
    else if (*ldA < MAX(1, *n))
        *info = -4;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("CPOTRF", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Recursive kernel
    RELAPACK_cpotrf_rec(&cleanuplo, n, A, ldA, info);
}


/** cpotrf's recursive compute kernel */
static void RELAPACK_cpotrf_rec(
    const char *uplo, const int *n,
    float *A, const int *ldA,
    int *info
){

    if (*n <= MAX(CROSSOVER_CPOTRF, 1)) {
        // Unblocked
        LAPACK(cpotf2)(uplo, n, A, ldA, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1., 0. };
    const float MONE[] = { -1., 0. };

    // Splitting
    const int n1 = CREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + 2 * *ldA * n1;
    float *const A_BL = A                 + 2 * n1;
    float *const A_BR = A + 2 * *ldA * n1 + 2 * n1;

    // recursion(A_TL)
    RELAPACK_cpotrf_rec(uplo, &n1, A_TL, ldA, info);
    if (*info)
        return;

    if (*uplo == 'L') {
        // A_BL = A_BL / A_TL'
        BLAS(ctrsm)("R", "L", "C", "N", &n2, &n1, ONE, A_TL, ldA, A_BL, ldA);
        // A_BR = A_BR - A_BL * A_BL'
        BLAS(cherk)("L", "N", &n2, &n1, MONE, A_BL, ldA, ONE, A_BR, ldA);
    } else {
        // A_TR = A_TL' \ A_TR
        BLAS(ctrsm)("L", "U", "C", "N", &n1, &n2, ONE, A_TL, ldA, A_TR, ldA);
        // A_BR = A_BR - A_TR' * A_TR
        BLAS(cherk)("U", "C", &n2, &n1, MONE, A_TR, ldA, ONE, A_BR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_cpotrf_rec(uplo, &n2, A_BR, ldA, info);
    if (*info)
        *info += n1;
}
