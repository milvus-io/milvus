#include "relapack.h"

static void RELAPACK_zlauum_rec(const char *, const int *, double *,
    const int *, int *);


/** ZLAUUM computes the product U * U**H or L**H * L, where the triangular factor U or L is stored in the upper or lower triangular part of the array A.
 *
 * This routine is functionally equivalent to LAPACK's zlauum.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d8/d45/zlauum_8f.html
 * */
void RELAPACK_zlauum(
    const char *uplo, const int *n,
    double *A, const int *ldA,
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
        LAPACK(xerbla)("ZLAUUM", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Recursive kernel
    RELAPACK_zlauum_rec(&cleanuplo, n, A, ldA, info);
}


/** zlauum's recursive compute kernel */
static void RELAPACK_zlauum_rec(
    const char *uplo, const int *n,
    double *A, const int *ldA,
    int *info
) {

    if (*n <= MAX(CROSSOVER_ZLAUUM, 1)) {
        // Unblocked
        LAPACK(zlauu2)(uplo, n, A, ldA, info);
        return;
    }

    // Constants
    const double ONE[] = { 1., 0. };

    // Splitting
    const int n1 = ZREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    double *const A_TL = A;
    double *const A_TR = A + 2 * *ldA * n1;
    double *const A_BL = A                 + 2 * n1;
    double *const A_BR = A + 2 * *ldA * n1 + 2 * n1;

    // recursion(A_TL)
    RELAPACK_zlauum_rec(uplo, &n1, A_TL, ldA, info);

    if (*uplo == 'L') {
        // A_TL = A_TL + A_BL' * A_BL
        BLAS(zherk)("L", "C", &n1, &n2, ONE, A_BL, ldA, ONE, A_TL, ldA);
        // A_BL = A_BR' * A_BL
        BLAS(ztrmm)("L", "L", "C", "N", &n2, &n1, ONE, A_BR, ldA, A_BL, ldA);
    } else {
        // A_TL = A_TL + A_TR * A_TR'
        BLAS(zherk)("U", "N", &n1, &n2, ONE, A_TR, ldA, ONE, A_TL, ldA);
        // A_TR = A_TR * A_BR'
        BLAS(ztrmm)("R", "U", "C", "N", &n1, &n2, ONE, A_BR, ldA, A_TR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_zlauum_rec(uplo, &n2, A_BR, ldA, info);
}
