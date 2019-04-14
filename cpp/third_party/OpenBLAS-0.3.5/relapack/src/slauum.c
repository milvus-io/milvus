#include "relapack.h"

static void RELAPACK_slauum_rec(const char *, const int *, float *,
    const int *, int *);


/** SLAUUM computes the product U * U**T or L**T * L, where the triangular factor U or L is stored in the upper or lower triangular part of the array A.
 *
 * This routine is functionally equivalent to LAPACK's slauum.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/dd/d5a/slauum_8f.html
 * */
void RELAPACK_slauum(
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
        LAPACK(xerbla)("SLAUUM", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Recursive kernel
    RELAPACK_slauum_rec(&cleanuplo, n, A, ldA, info);
}


/** slauum's recursive compute kernel */
static void RELAPACK_slauum_rec(
    const char *uplo, const int *n,
    float *A, const int *ldA,
    int *info
) {

    if (*n <= MAX(CROSSOVER_SLAUUM, 1)) {
        // Unblocked
        LAPACK(slauu2)(uplo, n, A, ldA, info);
        return;
    }

    // Constants
    const float ONE[] = { 1. };

    // Splitting
    const int n1 = SREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + *ldA * n1;
    float *const A_BL = A             + n1;
    float *const A_BR = A + *ldA * n1 + n1;

    // recursion(A_TL)
    RELAPACK_slauum_rec(uplo, &n1, A_TL, ldA, info);

    if (*uplo == 'L') {
        // A_TL = A_TL + A_BL' * A_BL
        BLAS(ssyrk)("L", "T", &n1, &n2, ONE, A_BL, ldA, ONE, A_TL, ldA);
        // A_BL = A_BR' * A_BL
        BLAS(strmm)("L", "L", "T", "N", &n2, &n1, ONE, A_BR, ldA, A_BL, ldA);
    } else {
        // A_TL = A_TL + A_TR * A_TR'
        BLAS(ssyrk)("U", "N", &n1, &n2, ONE, A_TR, ldA, ONE, A_TL, ldA);
        // A_TR = A_TR * A_BR'
        BLAS(strmm)("R", "U", "T", "N", &n1, &n2, ONE, A_BR, ldA, A_TR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_slauum_rec(uplo, &n2, A_BR, ldA, info);
}
