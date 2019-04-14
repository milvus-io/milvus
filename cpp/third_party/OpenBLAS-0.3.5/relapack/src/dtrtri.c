#include "relapack.h"

static void RELAPACK_dtrtri_rec(const char *, const char *, const int *,
    double *, const int *, int *);


/** DTRTRI computes the inverse of a real upper or lower triangular matrix A.
 *
 * This routine is functionally equivalent to LAPACK's dtrtri.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d5/dba/dtrtri_8f.html
 * */
void RELAPACK_dtrtri(
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
        LAPACK(xerbla)("DTRTRI", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower  ? 'L' : 'U';
    const char cleandiag = nounit ? 'N' : 'U';

    // check for singularity
    if (nounit) {
        int i;
        for (i = 0; i < *n; i++)
            if (A[i + *ldA * i] == 0) {
                *info = i;
                return;
            }
    }

    // Recursive kernel
    RELAPACK_dtrtri_rec(&cleanuplo, &cleandiag, n, A, ldA, info);
}


/** dtrtri's recursive compute kernel */
static void RELAPACK_dtrtri_rec(
    const char *uplo, const char *diag, const int *n,
    double *A, const int *ldA,
    int *info
){

    if (*n <= MAX(CROSSOVER_DTRTRI, 1)) {
        // Unblocked
        LAPACK(dtrti2)(uplo, diag, n, A, ldA, info);
        return;
    }

    // Constants
    const double ONE[]  = { 1. };
    const double MONE[] = { -1. };

    // Splitting
    const int n1 = DREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    double *const A_TL = A;
    double *const A_TR = A + *ldA * n1;
    double *const A_BL = A             + n1;
    double *const A_BR = A + *ldA * n1 + n1;

    // recursion(A_TL)
    RELAPACK_dtrtri_rec(uplo, diag, &n1, A_TL, ldA, info);
    if (*info)
        return;

    if (*uplo == 'L') {
        // A_BL = - A_BL * A_TL
        BLAS(dtrmm)("R", "L", "N", diag, &n2, &n1, MONE, A_TL, ldA, A_BL, ldA);
        // A_BL = A_BR \ A_BL
        BLAS(dtrsm)("L", "L", "N", diag, &n2, &n1, ONE, A_BR, ldA, A_BL, ldA);
    } else {
        // A_TR = - A_TL * A_TR
        BLAS(dtrmm)("L", "U", "N", diag, &n1, &n2, MONE, A_TL, ldA, A_TR, ldA);
        // A_TR = A_TR / A_BR
        BLAS(dtrsm)("R", "U", "N", diag, &n1, &n2, ONE, A_BR, ldA, A_TR, ldA);
    }

    // recursion(A_BR)
    RELAPACK_dtrtri_rec(uplo, diag, &n2, A_BR, ldA, info);
    if (*info)
        *info += n1;
}
