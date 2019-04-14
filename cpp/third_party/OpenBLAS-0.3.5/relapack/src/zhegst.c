#include "relapack.h"
#if XSYGST_ALLOW_MALLOC
#include "stdlib.h"
#endif

static void RELAPACK_zhegst_rec(const int *, const char *, const int *,
    double *, const int *, const double *, const int *,
    double *, const int *, int *);


/** ZHEGST reduces a complex Hermitian-definite generalized eigenproblem to standard form.
 *
 * This routine is functionally equivalent to LAPACK's zhegst.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/dc/d68/zhegst_8f.html
 * */
void RELAPACK_zhegst(
    const int *itype, const char *uplo, const int *n,
    double *A, const int *ldA, const double *B, const int *ldB,
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
        LAPACK(xerbla)("ZHEGST", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Allocate work space
    double *Work = NULL;
    int    lWork = 0;
#if XSYGST_ALLOW_MALLOC
    const int n1 = ZREC_SPLIT(*n);
    lWork = n1 * (*n - n1);
    Work  = malloc(lWork * 2 * sizeof(double));
    if (!Work)
        lWork = 0;
#endif

    // recursive kernel
    RELAPACK_zhegst_rec(itype, &cleanuplo, n, A, ldA, B, ldB, Work, &lWork, info);

    // Free work space
#if XSYGST_ALLOW_MALLOC
    if (Work)
        free(Work);
#endif
}


/** zhegst's recursive compute kernel */
static void RELAPACK_zhegst_rec(
    const int *itype, const char *uplo, const int *n,
    double *A, const int *ldA, const double *B, const int *ldB,
    double *Work, const int *lWork, int *info
) {

    if (*n <= MAX(CROSSOVER_ZHEGST, 1)) {
        // Unblocked
        LAPACK(zhegs2)(itype, uplo, n, A, ldA, B, ldB, info);
        return;
    }

    // Constants
    const double ZERO[]  = { 0., 0. };
    const double ONE[]   = { 1., 0. };
    const double MONE[]  = { -1., 0. };
    const double HALF[]  = { .5, 0. };
    const double MHALF[] = { -.5, 0. };
    const int    iONE[]  = { 1 };

    // Loop iterator
    int i;

    // Splitting
    const int n1 = ZREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_TL A_TR
    // A_BL A_BR
    double *const A_TL = A;
    double *const A_TR = A + 2 * *ldA * n1;
    double *const A_BL = A                 + 2 * n1;
    double *const A_BR = A + 2 * *ldA * n1 + 2 * n1;

    // B_TL B_TR
    // B_BL B_BR
    const double *const B_TL = B;
    const double *const B_TR = B + 2 * *ldB * n1;
    const double *const B_BL = B                 + 2 * n1;
    const double *const B_BR = B + 2 * *ldB * n1 + 2 * n1;

    // recursion(A_TL, B_TL)
    RELAPACK_zhegst_rec(itype, uplo, &n1, A_TL, ldA, B_TL, ldB, Work, lWork, info);

    if (*itype == 1)
        if (*uplo == 'L') {
            // A_BL = A_BL / B_TL'
            BLAS(ztrsm)("R", "L", "C", "N", &n2, &n1, ONE, B_TL, ldB, A_BL, ldA);
            if (*lWork >= n2 * n1) {
                // T = -1/2 * B_BL * A_TL
                BLAS(zhemm)("R", "L", &n2, &n1, MHALF, A_TL, ldA, B_BL, ldB, ZERO, Work, &n2);
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(zaxpy)(&n2, ONE, Work + 2 * n2 * i, iONE, A_BL + 2 * *ldA * i, iONE);
            } else
                // A_BL = A_BL - 1/2 B_BL * A_TL
                BLAS(zhemm)("R", "L", &n2, &n1, MHALF, A_TL, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_BR = A_BR - A_BL * B_BL' - B_BL * A_BL'
            BLAS(zher2k)("L", "N", &n2, &n1, MONE, A_BL, ldA, B_BL, ldB, ONE, A_BR, ldA);
            if (*lWork >= n2 * n1)
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(zaxpy)(&n2, ONE, Work + 2 * n2 * i, iONE, A_BL + 2 * *ldA * i, iONE);
            else
                // A_BL = A_BL - 1/2 B_BL * A_TL
                BLAS(zhemm)("R", "L", &n2, &n1, MHALF, A_TL, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_BL = B_BR \ A_BL
            BLAS(ztrsm)("L", "L", "N", "N", &n2, &n1, ONE, B_BR, ldB, A_BL, ldA);
        } else {
            // A_TR = B_TL' \ A_TR
            BLAS(ztrsm)("L", "U", "C", "N", &n1, &n2, ONE, B_TL, ldB, A_TR, ldA);
            if (*lWork >= n2 * n1) {
                // T = -1/2 * A_TL * B_TR
                BLAS(zhemm)("L", "U", &n1, &n2, MHALF, A_TL, ldA, B_TR, ldB, ZERO, Work, &n1);
                // A_TR = A_BL + T
                for (i = 0; i < n2; i++)
                    BLAS(zaxpy)(&n1, ONE, Work + 2 * n1 * i, iONE, A_TR + 2 * *ldA * i, iONE);
            } else
                // A_TR = A_TR - 1/2 A_TL * B_TR
                BLAS(zhemm)("L", "U", &n1, &n2, MHALF, A_TL, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_BR = A_BR - A_TR' * B_TR - B_TR' * A_TR
            BLAS(zher2k)("U", "C", &n2, &n1, MONE, A_TR, ldA, B_TR, ldB, ONE, A_BR, ldA);
            if (*lWork >= n2 * n1)
                // A_TR = A_BL + T
                for (i = 0; i < n2; i++)
                    BLAS(zaxpy)(&n1, ONE, Work + 2 * n1 * i, iONE, A_TR + 2 * *ldA * i, iONE);
            else
                // A_TR = A_TR - 1/2 A_TL * B_TR
                BLAS(zhemm)("L", "U", &n1, &n2, MHALF, A_TL, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_TR = A_TR / B_BR
            BLAS(ztrsm)("R", "U", "N", "N", &n1, &n2, ONE, B_BR, ldB, A_TR, ldA);
        }
    else
        if (*uplo == 'L') {
            // A_BL = A_BL * B_TL
            BLAS(ztrmm)("R", "L", "N", "N", &n2, &n1, ONE, B_TL, ldB, A_BL, ldA);
            if (*lWork >= n2 * n1) {
                // T = 1/2 * A_BR * B_BL
                BLAS(zhemm)("L", "L", &n2, &n1, HALF, A_BR, ldA, B_BL, ldB, ZERO, Work, &n2);
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(zaxpy)(&n2, ONE, Work + 2 * n2 * i, iONE, A_BL + 2 * *ldA * i, iONE);
            } else
                // A_BL = A_BL + 1/2 A_BR * B_BL
                BLAS(zhemm)("L", "L", &n2, &n1, HALF, A_BR, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_TL = A_TL + A_BL' * B_BL + B_BL' * A_BL
            BLAS(zher2k)("L", "C", &n1, &n2, ONE, A_BL, ldA, B_BL, ldB, ONE, A_TL, ldA);
            if (*lWork >= n2 * n1)
                // A_BL = A_BL + T
                for (i = 0; i < n1; i++)
                    BLAS(zaxpy)(&n2, ONE, Work + 2 * n2 * i, iONE, A_BL + 2 * *ldA * i, iONE);
            else
                // A_BL = A_BL + 1/2 A_BR * B_BL
                BLAS(zhemm)("L", "L", &n2, &n1, HALF, A_BR, ldA, B_BL, ldB, ONE, A_BL, ldA);
            // A_BL = B_BR * A_BL
            BLAS(ztrmm)("L", "L", "C", "N", &n2, &n1, ONE, B_BR, ldB, A_BL, ldA);
        } else {
            // A_TR = B_TL * A_TR
            BLAS(ztrmm)("L", "U", "N", "N", &n1, &n2, ONE, B_TL, ldB, A_TR, ldA);
            if (*lWork >= n2 * n1) {
                // T = 1/2 * B_TR * A_BR
                BLAS(zhemm)("R", "U", &n1, &n2, HALF, A_BR, ldA, B_TR, ldB, ZERO, Work, &n1);
                // A_TR = A_TR + T
                for (i = 0; i < n2; i++)
                    BLAS(zaxpy)(&n1, ONE, Work + 2 * n1 * i, iONE, A_TR + 2 * *ldA * i, iONE);
            } else
                // A_TR = A_TR + 1/2 B_TR A_BR
                BLAS(zhemm)("R", "U", &n1, &n2, HALF, A_BR, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_TL = A_TL + A_TR * B_TR' + B_TR * A_TR'
            BLAS(zher2k)("U", "N", &n1, &n2, ONE, A_TR, ldA, B_TR, ldB, ONE, A_TL, ldA);
            if (*lWork >= n2 * n1)
                // A_TR = A_TR + T
                for (i = 0; i < n2; i++)
                    BLAS(zaxpy)(&n1, ONE, Work + 2 * n1 * i, iONE, A_TR + 2 * *ldA * i, iONE);
            else
                // A_TR = A_TR + 1/2 B_TR * A_BR
                BLAS(zhemm)("R", "U", &n1, &n2, HALF, A_BR, ldA, B_TR, ldB, ONE, A_TR, ldA);
            // A_TR = A_TR * B_BR
            BLAS(ztrmm)("R", "U", "C", "N", &n1, &n2, ONE, B_BR, ldB, A_TR, ldA);
        }

    // recursion(A_BR, B_BR)
    RELAPACK_zhegst_rec(itype, uplo, &n2, A_BR, ldA, B_BR, ldB, Work, lWork, info);
}
