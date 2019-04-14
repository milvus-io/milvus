#include "relapack.h"
#include "stdlib.h"

static void RELAPACK_spbtrf_rec(const char *, const int *, const int *,
    float *, const int *, float *, const int *, int *);


/** SPBTRF computes the Cholesky factorization of a real symmetric positive definite band matrix A.
 *
 * This routine is functionally equivalent to LAPACK's spbtrf.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d1/d22/spbtrf_8f.html
 * */
void RELAPACK_spbtrf(
    const char *uplo, const int *n, const int *kd,
    float *Ab, const int *ldAb,
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
    else if (*kd < 0)
        *info = -3;
    else if (*ldAb < *kd + 1)
        *info = -5;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("SPBTRF", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Constant
    const float ZERO[] = { 0. };

    // Allocate work space
    const int n1 = SREC_SPLIT(*n);
    const int mWork = (*kd > n1) ? (lower ? *n - *kd : n1) : *kd;
    const int nWork = (*kd > n1) ? (lower ? n1 : *n - *kd) : *kd;
    float *Work = malloc(mWork * nWork * sizeof(float));
    LAPACK(slaset)(uplo, &mWork, &nWork, ZERO, ZERO, Work, &mWork);

    // Recursive kernel
    RELAPACK_spbtrf_rec(&cleanuplo, n, kd, Ab, ldAb, Work, &mWork, info);

    // Free work space
    free(Work);
}


/** spbtrf's recursive compute kernel */
static void RELAPACK_spbtrf_rec(
    const char *uplo, const int *n, const int *kd,
    float *Ab, const int *ldAb,
    float *Work, const int *ldWork,
    int *info
){

    if (*n <= MAX(CROSSOVER_SPBTRF, 1)) {
        // Unblocked
        LAPACK(spbtf2)(uplo, n, kd, Ab, ldAb, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1. };
    const float MONE[] = { -1. };

    // Unskew A
    const int ldA[] = { *ldAb - 1 };
    float *const A = Ab + ((*uplo == 'L') ? 0 : *kd);

    // Splitting
    const int n1 = MIN(SREC_SPLIT(*n), *kd);
    const int n2 = *n - n1;

    // * *
    // * Ab_BR
    float *const Ab_BR = Ab + *ldAb * n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + *ldA * n1;
    float *const A_BL = A             + n1;
    float *const A_BR = A + *ldA * n1 + n1;

    // recursion(A_TL)
    RELAPACK_spotrf(uplo, &n1, A_TL, ldA, info);
    if (*info)
        return;

    // Banded splitting
    const int n21 = MIN(n2, *kd - n1);
    const int n22 = MIN(n2 - n21, *kd);

    //     n1    n21    n22
    // n1  *     A_TRl  A_TRr
    // n21 A_BLt A_BRtl A_BRtr
    // n22 A_BLb A_BRbl A_BRbr
    float *const A_TRl  = A_TR;
    float *const A_TRr  = A_TR + *ldA * n21;
    float *const A_BLt  = A_BL;
    float *const A_BLb  = A_BL               + n21;
    float *const A_BRtl = A_BR;
    float *const A_BRtr = A_BR + *ldA * n21;
    float *const A_BRbl = A_BR               + n21;
    float *const A_BRbr = A_BR + *ldA * n21  + n21;

    if (*uplo == 'L') {
        // A_BLt = ABLt / A_TL'
        BLAS(strsm)("R", "L", "T", "N", &n21, &n1, ONE, A_TL, ldA, A_BLt, ldA);
        // A_BRtl = A_BRtl - A_BLt * A_BLt'
        BLAS(ssyrk)("L", "N", &n21, &n1, MONE, A_BLt, ldA, ONE, A_BRtl, ldA);
        // Work = A_BLb
        LAPACK(slacpy)("U", &n22, &n1, A_BLb, ldA, Work, ldWork);
        // Work = Work / A_TL'
        BLAS(strsm)("R", "L", "T", "N", &n22, &n1, ONE, A_TL, ldA, Work, ldWork);
        // A_BRbl = A_BRbl - Work * A_BLt'
        BLAS(sgemm)("N", "T", &n22, &n21, &n1, MONE, Work, ldWork, A_BLt, ldA, ONE, A_BRbl, ldA);
        // A_BRbr = A_BRbr - Work * Work'
        BLAS(ssyrk)("L", "N", &n22, &n1, MONE, Work, ldWork, ONE, A_BRbr, ldA);
        // A_BLb = Work
        LAPACK(slacpy)("U", &n22, &n1, Work, ldWork, A_BLb, ldA);
    } else {
        // A_TRl = A_TL' \ A_TRl
        BLAS(strsm)("L", "U", "T", "N", &n1, &n21, ONE, A_TL, ldA, A_TRl, ldA);
        // A_BRtl = A_BRtl - A_TRl' * A_TRl
        BLAS(ssyrk)("U", "T", &n21, &n1, MONE, A_TRl, ldA, ONE, A_BRtl, ldA);
        // Work = A_TRr
        LAPACK(slacpy)("L", &n1, &n22, A_TRr, ldA, Work, ldWork);
        // Work = A_TL' \ Work
        BLAS(strsm)("L", "U", "T", "N", &n1, &n22, ONE, A_TL, ldA, Work, ldWork);
        // A_BRtr = A_BRtr - A_TRl' * Work
        BLAS(sgemm)("T", "N", &n21, &n22, &n1, MONE, A_TRl, ldA, Work, ldWork, ONE, A_BRtr, ldA);
        // A_BRbr = A_BRbr - Work' * Work
        BLAS(ssyrk)("U", "T", &n22, &n1, MONE, Work, ldWork, ONE, A_BRbr, ldA);
        // A_TRr = Work
        LAPACK(slacpy)("L", &n1, &n22, Work, ldWork, A_TRr, ldA);
    }

    // recursion(A_BR)
    if (*kd > n1)
        RELAPACK_spotrf(uplo, &n2, A_BR, ldA, info);
    else
        RELAPACK_spbtrf_rec(uplo, &n2, kd, Ab_BR, ldAb, Work, ldWork, info);
    if (*info)
        *info += n1;
}
