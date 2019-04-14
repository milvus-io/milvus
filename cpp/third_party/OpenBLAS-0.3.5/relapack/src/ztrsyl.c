#include "relapack.h"

static void RELAPACK_ztrsyl_rec(const char *, const char *, const int *,
    const int *, const int *, const double *, const int *, const double *,
    const int *, double *, const int *, double *, int *);


/** ZTRSYL solves the complex Sylvester matrix equation.
 *
 * This routine is functionally equivalent to LAPACK's ztrsyl.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d1/d36/ztrsyl_8f.html
 * */
void RELAPACK_ztrsyl(
    const char *tranA, const char *tranB, const int *isgn,
    const int *m, const int *n,
    const double *A, const int *ldA, const double *B, const int *ldB,
    double *C, const int *ldC, double *scale,
    int *info
) {

    // Check arguments
    const int notransA = LAPACK(lsame)(tranA, "N");
    const int ctransA = LAPACK(lsame)(tranA, "C");
    const int notransB = LAPACK(lsame)(tranB, "N");
    const int ctransB = LAPACK(lsame)(tranB, "C");
    *info = 0;
    if (!ctransA && !notransA)
        *info = -1;
    else if (!ctransB && !notransB)
        *info = -2;
    else if (*isgn != 1 && *isgn != -1)
        *info = -3;
    else if (*m < 0)
        *info = -4;
    else if (*n < 0)
        *info = -5;
    else if (*ldA < MAX(1, *m))
        *info = -7;
    else if (*ldB < MAX(1, *n))
        *info = -9;
    else if (*ldC < MAX(1, *m))
        *info = -11;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("ZTRSYL", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleantranA = notransA ? 'N' : 'C';
    const char cleantranB = notransB ? 'N' : 'C';

    // Recursive kernel
    RELAPACK_ztrsyl_rec(&cleantranA, &cleantranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
}


/** ztrsyl's recursive compute kernel */
static void RELAPACK_ztrsyl_rec(
    const char *tranA, const char *tranB, const int *isgn,
    const int *m, const int *n,
    const double *A, const int *ldA, const double *B, const int *ldB,
    double *C, const int *ldC, double *scale,
    int *info
) {

    if (*m <= MAX(CROSSOVER_ZTRSYL, 1) && *n <= MAX(CROSSOVER_ZTRSYL, 1)) {
        // Unblocked
        RELAPACK_ztrsyl_rec2(tranA, tranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
        return;
    }

    // Constants
    const double ONE[]  = { 1., 0. };
    const double MONE[] = { -1., 0. };
    const double MSGN[] = { -*isgn, 0. };
    const int    iONE[] = { 1 };

    // Outputs
    double scale1[] = { 1., 0. };
    double scale2[] = { 1., 0. };
    int    info1[]  = { 0 };
    int    info2[]  = { 0 };

    if (*m > *n) {
        // Splitting
        const int m1 = ZREC_SPLIT(*m);
        const int m2 = *m - m1;

        // A_TL A_TR
        // 0    A_BR
        const double *const A_TL = A;
        const double *const A_TR = A + 2 * *ldA * m1;
        const double *const A_BR = A + 2 * *ldA * m1 + 2 * m1;

        // C_T
        // C_B
        double *const C_T = C;
        double *const C_B = C + 2 * m1;

        if (*tranA == 'N') {
            // recusion(A_BR, B, C_B)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, &m2, n, A_BR, ldA, B, ldB, C_B, ldC, scale1, info1);
            // C_T = C_T - A_TR * C_B
            BLAS(zgemm)("N", "N", &m1, n, &m2, MONE, A_TR, ldA, C_B, ldC, scale1, C_T, ldC);
            // recusion(A_TL, B, C_T)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, &m1, n, A_TL, ldA, B, ldB, C_T, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, &m2, n, C_B, ldC, info);
        } else {
            // recusion(A_TL, B, C_T)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, &m1, n, A_TL, ldA, B, ldB, C_T, ldC, scale1, info1);
            // C_B = C_B - A_TR' * C_T
            BLAS(zgemm)("C", "N", &m2, n, &m1, MONE, A_TR, ldA, C_T, ldC, scale1, C_B, ldC);
            // recusion(A_BR, B, C_B)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, &m2, n, A_BR, ldA, B, ldB, C_B, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, &m1, n, C_B, ldC, info);
        }
    } else {
        // Splitting
        const int n1 = ZREC_SPLIT(*n);
        const int n2 = *n - n1;

        // B_TL B_TR
        // 0    B_BR
        const double *const B_TL = B;
        const double *const B_TR = B + 2 * *ldB * n1;
        const double *const B_BR = B + 2 * *ldB * n1 + 2 * n1;

        // C_L C_R
        double *const C_L = C;
        double *const C_R = C + 2 * *ldC * n1;

        if (*tranB == 'N') {
            // recusion(A, B_TL, C_L)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, m, &n1, A, ldA, B_TL, ldB, C_L, ldC, scale1, info1);
            // C_R = C_R -/+ C_L * B_TR
            BLAS(zgemm)("N", "N", m, &n2, &n1, MSGN, C_L, ldC, B_TR, ldB, scale1, C_R, ldC);
            // recusion(A, B_BR, C_R)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, m, &n2, A, ldA, B_BR, ldB, C_R, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, m, &n1, C_L, ldC, info);
        } else {
            // recusion(A, B_BR, C_R)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, m, &n2, A, ldA, B_BR, ldB, C_R, ldC, scale1, info1);
            // C_L = C_L -/+ C_R * B_TR'
            BLAS(zgemm)("N", "C", m, &n1, &n2, MSGN, C_R, ldC, B_TR, ldB, scale1, C_L, ldC);
            // recusion(A, B_TL, C_L)
            RELAPACK_ztrsyl_rec(tranA, tranB, isgn, m, &n1, A, ldA, B_TL, ldB, C_L, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, m, &n2, C_R, ldC, info);
        }
    }

    *scale = scale1[0] * scale2[0];
    *info  = info1[0] || info2[0];
}
