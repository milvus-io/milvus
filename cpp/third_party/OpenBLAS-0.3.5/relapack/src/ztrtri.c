#include "relapack.h"

static void RELAPACK_ztrtri_rec(const char *, const char *, const int *,
    double *, const int *, int *);


/** CTRTRI computes the inverse of a complex upper or lower triangular matrix A.
 *
 * This routine is functionally equivalent to LAPACK's ztrtri.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d1/d0e/ztrtri_8f.html
 * */
void RELAPACK_ztrtri(
    const char *uplo, const char *diag, const int *n,
    double *A, const int *ldA,
    int *info
) {

    // Check arguments
    const int lower = LAPACK(lsame)(uplo, "L");
    const int upper = LAPACK(lsame)(uplo, "U");
    const int nounit = LAPACK(lsame)(diag, "N");
    const int unit = LAPACK(lsame)(diag, "U");
    *info = 0;
    if (!lower && !upper)
        *info = -1;
    else if (!nounit && !unit)
        *info = -2;
    else if (*n < 0)
        *info = -3;
    else if (*ldA < MAX(1, *n))
        *info = -5;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("ZTRTRI", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower  ? 'L' : 'U';
    const char cleandiag = nounit ? 'N' : 'U';

    // check for singularity
    if (nounit) {
        int i;
        for (i = 0; i < *n; i++)
            if (A[2 * (i + *ldA * i)] == 0 && A[2 * (i + *ldA * i) + 1] == 0) {
                *info = i;
                return;
            }
    }

    // Recursive kernel
    RELAPACK_ztrtri_rec(&cleanuplo, &cleandiag, n, A, ldA, info);
}


/** ztrtri's recursive compute kernel */
static void RELAPACK_ztrtri_rec(
    const char *uplo, const char *diag, const int *n,
    double *A, const int *ldA,
    int *info
){

    if (*n <= MAX(CROSSOVER_ZTRTRI, 1)) {
        // Unblocked
        LAPACK(ztrti2)(uplo, diag, n, A, ldA, info);
        return;
    }

    // Constants
    const double ONE[]  = { 1. };
    const double MONE[] = { -1. };

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
    RELAPACK_ztrtri_rec(uplo, diag, &n1, A_TL, ldA, info);
    if (*info)
        return;

    if (*uplo == 'L') {
        // A_BL = - A_BL * A_TL
        BLAS(ztrmm)("R", "L", "N", diag, &n2, &n1, MONE, A_TL, ldA, A_BL, ldA);
        // A_BL = A_BR \ A_BL
        BLAS(ztrsm)("L", "L", "N", diag, &n2, &n1, ONE, A_BR, ldA, A_BL, ldA);
    } else {
        // A_TR = - A_TL * A_TR
        BLAS(ztrmm)("L", "U", "N", diag, &n1, &n2, MONE, A_TL, ldA, A_TR, ldA);
        // A_TR = A_TR / A_BR
        BLAS(ztrsm)("R", "U", "N", diag, &n1, &n2, ONE, A_BR, ldA, A_TR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_ztrtri_rec(uplo, diag, &n2, A_BR, ldA, info);
    if (*info)
        *info += n1;
}
