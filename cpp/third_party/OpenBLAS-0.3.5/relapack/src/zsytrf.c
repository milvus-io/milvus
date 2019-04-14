#include "relapack.h"
#if XSYTRF_ALLOW_MALLOC
#include <stdlib.h>
#endif

static void RELAPACK_zsytrf_rec(const char *, const int *, const int *, int *,
    double *, const int *, int *, double *, const int *, int *);


/** ZSYTRF computes the factorization of a complex symmetric matrix A using the Bunch-Kaufman diagonal pivoting method.
 *
 * This routine is functionally equivalent to LAPACK's zsytrf.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/da/d94/zsytrf_8f.html
 * */
void RELAPACK_zsytrf(
    const char *uplo, const int *n,
    double *A, const int *ldA, int *ipiv,
    double *Work, const int *lWork, int *info
) {

    // Required work size
    const int cleanlWork = *n * (*n / 2);
    int minlWork = cleanlWork;
#if XSYTRF_ALLOW_MALLOC
    minlWork = 1;
#endif

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
    else if (*lWork < minlWork && *lWork != -1)
        *info = -7;
    else if (*lWork == -1) {
        // Work size query
        *Work = cleanlWork;
        return;
    }

    // Ensure Work size
    double *cleanWork = Work;
#if XSYTRF_ALLOW_MALLOC
    if (!*info && *lWork < cleanlWork) {
        cleanWork = malloc(cleanlWork * 2 * sizeof(double));
        if (!cleanWork)
            *info = -7;
    }
#endif

    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("ZSYTRF", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';

    // Dummy arguments
    int nout;

    // Recursive kernel
    RELAPACK_zsytrf_rec(&cleanuplo, n, n, &nout, A, ldA, ipiv, cleanWork, n, info);

#if XSYTRF_ALLOW_MALLOC
    if (cleanWork != Work)
        free(cleanWork);
#endif
}


/** zsytrf's recursive compute kernel */
static void RELAPACK_zsytrf_rec(
    const char *uplo, const int *n_full, const int *n, int *n_out,
    double *A, const int *ldA, int *ipiv,
    double *Work, const int *ldWork, int *info
) {

    // top recursion level?
    const int top = *n_full == *n;

    if (*n <= MAX(CROSSOVER_ZSYTRF, 3)) {
        // Unblocked
        if (top) {
            LAPACK(zsytf2)(uplo, n, A, ldA, ipiv, info);
            *n_out = *n;
        } else
            RELAPACK_zsytrf_rec2(uplo, n_full, n, n_out, A, ldA, ipiv, Work, ldWork, info);
        return;
    }

    int info1, info2;

    // Constants
    const double ONE[]  = { 1., 0. };
    const double MONE[] = { -1., 0. };
    const int    iONE[] = { 1 };

    // Loop iterator
    int i;

    const int n_rest = *n_full - *n;

    if (*uplo == 'L') {
        // Splitting (setup)
        int n1 = ZREC_SPLIT(*n);
        int n2 = *n - n1;

        // Work_L *
        double *const Work_L = Work;

        // recursion(A_L)
        int n1_out;
        RELAPACK_zsytrf_rec(uplo, n_full, &n1, &n1_out, A, ldA, ipiv, Work_L, ldWork, &info1);
        n1 = n1_out;

        // Splitting (continued)
        n2 = *n - n1;
        const int n_full2 = *n_full - n1;

        // *      *
        // A_BL   A_BR
        // A_BL_B A_BR_B
        double *const A_BL   = A                 + 2 * n1;
        double *const A_BR   = A + 2 * *ldA * n1 + 2 * n1;
        double *const A_BL_B = A                 + 2 * *n;
        double *const A_BR_B = A + 2 * *ldA * n1 + 2 * *n;

        // *        *
        // Work_BL Work_BR
        // *       *
        // (top recursion level: use Work as Work_BR)
        double *const Work_BL =              Work                    + 2 * n1;
        double *const Work_BR = top ? Work : Work + 2 * *ldWork * n1 + 2 * n1;
        const int ldWork_BR = top ? n2 : *ldWork;

        // ipiv_T
        // ipiv_B
        int *const ipiv_B = ipiv + n1;

        // A_BR = A_BR - A_BL Work_BL'
        RELAPACK_zgemmt(uplo, "N", "T", &n2, &n1, MONE, A_BL, ldA, Work_BL, ldWork, ONE, A_BR, ldA);
        BLAS(zgemm)("N", "T", &n_rest, &n2, &n1, MONE, A_BL_B, ldA, Work_BL, ldWork, ONE, A_BR_B, ldA);

        // recursion(A_BR)
        int n2_out;
        RELAPACK_zsytrf_rec(uplo, &n_full2, &n2, &n2_out, A_BR, ldA, ipiv_B, Work_BR, &ldWork_BR, &info2);

        if (n2_out != n2) {
            // undo 1 column of updates
            const int n_restp1 = n_rest + 1;

            // last column of A_BR
            double *const A_BR_r = A_BR + 2 * *ldA * n2_out + 2 * n2_out;

            // last row of A_BL
            double *const A_BL_b = A_BL + 2 * n2_out;

            // last row of Work_BL
            double *const Work_BL_b = Work_BL + 2 * n2_out;

            // A_BR_r = A_BR_r + A_BL_b Work_BL_b'
            BLAS(zgemv)("N", &n_restp1, &n1, ONE, A_BL_b, ldA, Work_BL_b, ldWork, ONE, A_BR_r, iONE);
        }
        n2 = n2_out;

        // shift pivots
        for (i = 0; i < n2; i++)
            if (ipiv_B[i] > 0)
                ipiv_B[i] += n1;
            else
                ipiv_B[i] -= n1;

        *info  = info1 || info2;
        *n_out = n1 + n2;
    } else {
        // Splitting (setup)
        int n2 = ZREC_SPLIT(*n);
        int n1 = *n - n2;

        // * Work_R
        // (top recursion level: use Work as Work_R)
        double *const Work_R = top ? Work : Work + 2 * *ldWork * n1;

        // recursion(A_R)
        int n2_out;
        RELAPACK_zsytrf_rec(uplo, n_full, &n2, &n2_out, A, ldA, ipiv, Work_R, ldWork, &info2);
        const int n2_diff = n2 - n2_out;
        n2 = n2_out;

        // Splitting (continued)
        n1 = *n - n2;
        const int n_full1  = *n_full - n2;

        // * A_TL_T A_TR_T
        // * A_TL   A_TR
        // * *      *
        double *const A_TL_T = A + 2 * *ldA * n_rest;
        double *const A_TR_T = A + 2 * *ldA * (n_rest + n1);
        double *const A_TL   = A + 2 * *ldA * n_rest        + 2 * n_rest;
        double *const A_TR   = A + 2 * *ldA * (n_rest + n1) + 2 * n_rest;

        // Work_L *
        // *      Work_TR
        // *      *
        // (top recursion level: Work_R was Work)
        double *const Work_L  = Work;
        double *const Work_TR = Work + 2 * *ldWork * (top ? n2_diff : n1) + 2 * n_rest;
        const int ldWork_L = top ? n1 : *ldWork;

        // A_TL = A_TL - A_TR Work_TR'
        RELAPACK_zgemmt(uplo, "N", "T", &n1, &n2, MONE, A_TR, ldA, Work_TR, ldWork, ONE, A_TL, ldA);
        BLAS(zgemm)("N", "T", &n_rest, &n1, &n2, MONE, A_TR_T, ldA, Work_TR, ldWork, ONE, A_TL_T, ldA);

        // recursion(A_TL)
        int n1_out;
        RELAPACK_zsytrf_rec(uplo, &n_full1, &n1, &n1_out, A, ldA, ipiv, Work_L, &ldWork_L, &info1);

        if (n1_out != n1) {
            // undo 1 column of updates
            const int n_restp1 = n_rest + 1;

            // A_TL_T_l = A_TL_T_l + A_TR_T Work_TR_t'
            BLAS(zgemv)("N", &n_restp1, &n2, ONE, A_TR_T, ldA, Work_TR, ldWork, ONE, A_TL_T, iONE);
        }
        n1 = n1_out;

        *info  = info2 || info1;
        *n_out = n1 + n2;
    }
}
