#include "relapack.h"
#if XSYGST_ALLOW_MALLOC
#include "stdlib.h"
#endif

static void RELAPACK_ssygst_rec(const int *, const char *, const int *,
    float *, const int *, const float *, const int *,
    float *, const int *, int *);


/** SSYGST reduces a real symmetric-definite generalized eigenproblem to standard form.
 *
 * This routine is functionally equivalent to LAPACK's ssygst.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d8/d78/ssygst_8f.html
 * */
void RELAPACK_ssygst(
    const int *itype, const char *uplo, const int *n,
    float *A, const int *ldA, const float *B, const int *ldB,
    int *info
) {

    // Check arguments
    const int lower = LAPACK(lsame)(uplo, "L");
    const int upper = LAPACK(lsame)(uplo, "U");
    *info = 0;
    if (*itype < 1 || *itype > 3)
        *info = -1;
    else if (!lower && !upper)
        *info = -2;
    else if (*n < 0)
        *info = -3;
    else if (*ldA < MAX(1, *n))
        *info = -5;
    else if (*ldB < MAX(1, *n))
        *info = -7;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("SSYGST", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Allocate work space
    float *Work = NULL;
    int   lWork = 0;
#if XSYGST_ALLOW_MALLOC
    const int n1 = SREC_SPLIT(*n);
    lWork = n1 * (*n - n1);
    Work  = malloc(lWork * sizeof(float));
    if (!Work)
        lWork = 0;
#endif

    // Recursive kernel
    RELAPACK_ssygst_rec(itype, &cleanuplo, n, A, ldA, B, ldB, Work, &lWork, info);

    // Free work space
#if XSYGST_ALLOW_MALLOC
    if (Work)
        free(Work);
#endif
}


/** ssygst's recursive compute kernel */
static void RELAPACK_ssygst_rec(
    const int *itype, const char *uplo, const int *n,
    float *A, const int *ldA, const float *B, const int *ldB,
    float *Work, const int *lWork, int *info
) {

    if (*n <= MAX(CROSSOVER_SSYGST, 1)) {
        // Unblocked
        LAPACK(ssygs2)(itype, uplo, n, A, ldA, B, ldB, info);
        return;
    }

    // Constants
    const float ZERO[]  = { 0. };
    const float ONE[]   = { 1. };
    const float MONE[]  = { -1. };
    const float HALF[]  = { .5 };
    const float MHALF[] = { -.5 };
    const int   iONE[]  = { 1 };

    // Loop iterator
    int i;

    // Splitting
    const int n1 = SREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    float *const A_TL = A;
    float *const A_TR = A + *ldA * n1;
    float *const A_BL = A             + n1;
    float *const A_BR = A + *ldA * n1 + n1;

    // B_TL B_TR
    // B_BL B_BR
    const float *const B_TL = B;
    const float *const B_TR = B + *ldB * n1;
    const float *const B_BL = B             + n1;
    const float *const B_BR = B + *ldB * n1 + n1;

    // recursion(A_TL, B_TL)
    RELAPACK_ssygst_rec(itype, uplo, &n1, A_TL, ldA, B_TL, ldB, Work, lWork, info);

    if (*itype == 1)
        if (*uplo == 'L') {
            // A_BL = A_BL / B_TL'
            BLAS(strsm)("R", "L", "T", "N", &n2, &n1, ONE, B_TL, ldB, A_BL, ldA);
            if (*lWork > n2 * n1) {
                // T = -1/2 * B_BL * A_TL
                BLAS(ssymm)("R", "L", &n2, &n1, MHALF, A_TL, ldA, B_BL, ldB, ZERO, Work, &n2);
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(saxpy)(&n2, ONE, Work + n2 * i, iONE, A_BL + *ldA * i, iONE);
            } else
                // A_BL = A_BL - 1/2 B_BL * A_TL
                BLAS(ssymm)("R", "L", &n2, &n1, MHALF, A_TL, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_BR = A_BR - A_BL * B_BL' - B_BL * A_BL'
            BLAS(ssyr2k)("L", "N", &n2, &n1, MONE, A_BL, ldA, B_BL, ldB, ONE, A_BR, ldA);
            if (*lWork > n2 * n1)
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(saxpy)(&n2, ONE, Work + n2 * i, iONE, A_BL + *ldA * i, iONE);
            else
                // A_BL = A_BL - 1/2 B_BL * A_TL
                BLAS(ssymm)("R", "L", &n2, &n1, MHALF, A_TL, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_BL = B_BR \ A_BL
            BLAS(strsm)("L", "L", "N", "N", &n2, &n1, ONE, B_BR, ldB, A_BL, ldA);
        } else {
            // A_TR = B_TL' \ A_TR
            BLAS(strsm)("L", "U", "T", "N", &n1, &n2, ONE, B_TL, ldB, A_TR, ldA);
            if (*lWork > n2 * n1) {
                // T = -1/2 * A_TL * B_TR
                BLAS(ssymm)("L", "U", &n1, &n2, MHALF, A_TL, ldA, B_TR, ldB, ZERO, Work, &n1);
                // A_TR = A_BL + T
                for (i = 0; i < n2; i++)
                    BLAS(saxpy)(&n1, ONE, Work + n1 * i, iONE, A_TR + *ldA * i, iONE);
            } else
                // A_TR = A_TR - 1/2 A_TL * B_TR
                BLAS(ssymm)("L", "U", &n1, &n2, MHALF, A_TL, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_BR = A_BR - A_TR' * B_TR - B_TR' * A_TR
            BLAS(ssyr2k)("U", "T", &n2, &n1, MONE, A_TR, ldA, B_TR, ldB, ONE, A_BR, ldA);
            if (*lWork > n2 * n1)
                // A_TR = A_BL + T
                for (i = 0; i < n2; i++)
                    BLAS(saxpy)(&n1, ONE, Work + n1 * i, iONE, A_TR + *ldA * i, iONE);
            else
                // A_TR = A_TR - 1/2 A_TL * B_TR
                BLAS(ssymm)("L", "U", &n1, &n2, MHALF, A_TL, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_TR = A_TR / B_BR
            BLAS(strsm)("R", "U", "N", "N", &n1, &n2, ONE, B_BR, ldB, A_TR, ldA);
        }
    else
        if (*uplo == 'L') {
            // A_BL = A_BL * B_TL
            BLAS(strmm)("R", "L", "N", "N", &n2, &n1, ONE, B_TL, ldB, A_BL, ldA);
            if (*lWork > n2 * n1) {
                // T = 1/2 * A_BR * B_BL
                BLAS(ssymm)("L", "L", &n2, &n1, HALF, A_BR, ldA, B_BL, ldB, ZERO, Work, &n2);
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(saxpy)(&n2, ONE, Work + n2 * i, iONE, A_BL + *ldA * i, iONE);
            } else
                // A_BL = A_BL + 1/2 A_BR * B_BL
                BLAS(ssymm)("L", "L", &n2, &n1, HALF, A_BR, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_TL = A_TL + A_BL' * B_BL + B_BL' * A_BL
            BLAS(ssyr2k)("L", "T", &n1, &n2, ONE, A_BL, ldA, B_BL, ldB, ONE, A_TL, ldA);
            if (*lWork > n2 * n1)
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(saxpy)(&n2, ONE, Work + n2 * i, iONE, A_BL + *ldA * i, iONE);
            else
                // A_BL = A_BL + 1/2 A_BR * B_BL
                BLAS(ssymm)("L", "L", &n2, &n1, HALF, A_BR, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_BL = B_BR * A_BL
            BLAS(strmm)("L", "L", "T", "N", &n2, &n1, ONE, B_BR, ldB, A_BL, ldA);
        } else {
            // A_TR = B_TL * A_TR
            BLAS(strmm)("L", "U", "N", "N", &n1, &n2, ONE, B_TL, ldB, A_TR, ldA);
            if (*lWork > n2 * n1) {
                // T = 1/2 * B_TR * A_BR
                BLAS(ssymm)("R", "U", &n1, &n2, HALF, A_BR, ldA, B_TR, ldB, ZERO, Work, &n1);
                // A_TR = A_TR + T
                for (i = 0; i < n2; i++)
                    BLAS(saxpy)(&n1, ONE, Work + n1 * i, iONE, A_TR + *ldA * i, iONE);
            } else
                // A_TR = A_TR + 1/2 B_TR A_BR
                BLAS(ssymm)("R", "U", &n1, &n2, HALF, A_BR, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_TL = A_TL + A_TR * B_TR' + B_TR * A_TR'
            BLAS(ssyr2k)("U", "N", &n1, &n2, ONE, A_TR, ldA, B_TR, ldB, ONE, A_TL, ldA);
            if (*lWork > n2 * n1)
                // A_TR = A_TR + T
                for (i = 0; i < n2; i++)
                    BLAS(saxpy)(&n1, ONE, Work + n1 * i, iONE, A_TR + *ldA * i, iONE);
            else
                // A_TR = A_TR + 1/2 B_TR * A_BR
                BLAS(ssymm)("R", "U", &n1, &n2, HALF, A_BR, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_TR = A_TR * B_BR
            BLAS(strmm)("R", "U", "T", "N", &n1, &n2, ONE, B_BR, ldB, A_TR, ldA);
        }

    // recursion(A_BR, B_BR)
    RELAPACK_ssygst_rec(itype, uplo, &n2, A_BR, ldA, B_BR, ldB, Work, lWork, info);
}
